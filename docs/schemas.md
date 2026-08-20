# Contratos de datos — Bronze / Silver / Gold

> Fase 1. Documento de diseño, cerrado antes de escribir código de
> transformación (principio rector #1 de `docs/decisions/plan-fases.md`).
> Decisiones de arquitectura no triviales referenciadas están en
> `docs/decisions/` (ADRs 0001-0004).

## Convenciones generales

- **Tipos**: se listan en términos conceptuales (`string`, `int`, `float`,
  `date`, `datetime`, `bool`); el tipo exacto de Polars/Pandera se fija al
  implementar (Fase 2/3).
- **Columnas de linaje** (todas las tablas Bronze): `dia_simulado` (date —
  partición), `ingested_at` (datetime — cuándo corrió la ingesta real),
  `source_file` (string), `batch_hash` (string — hash del contenido del
  batch, para detectar reprocesos idénticos).
- **Nomenclatura**: `bronze_<tabla>`, `silver_<tabla>`, `dim_*`/`fct_*` en
  Gold (según el plan).

---

## 1. Estrategia de replay cronológico (resumen)

Diseño completo en el ADR 0004. Resumen operativo:

| Tabla fuente | Ancla temporal | Re-emisión en Bronze |
| --- | --- | --- |
| `orders` | `order_purchase_timestamp`, `order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date` | Una fila cruda por cada timestamp no nulo, en el día simulado correspondiente |
| `order_reviews` | `review_creation_date` | Una fila, en su propio día |
| `order_items`, `order_payments` | (ninguno propio) | Una sola vez, junto al día de `order_purchase_timestamp` del pedido padre |
| `customers`, `products`, `sellers`, `geolocation`, `category_translation` | (sin dimensión temporal en la fuente) | Snapshot completo, no particionado por día |

`order_purchase_timestamp` es el ancla porque es el único timestamp de
`orders` sin nulos en el 100% de los pedidos (confirmado en la exploración
de Fase 0) — todo pedido tiene garantizado un "día de llegada" inicial.

---

## 2. Capa Bronze

### `bronze_orders`

Copia cruda de `orders.csv`, re-emitida por evento (ADR 0004).

| Columna | Tipo | Nullable | Notas |
| --- | --- | --- | --- |
| `order_id` | string | no | |
| `customer_id` | string | no | FK per-pedido, no es la persona real (ver `dim_cliente`) |
| `order_status` | string | no | valor final conocido del pedido, tal cual la fuente |
| `order_purchase_timestamp` | datetime | no | |
| `order_approved_at` | datetime | sí | nulo válido si el pedido no llegó a esa etapa |
| `order_delivered_carrier_date` | datetime | sí | ídem |
| `order_delivered_customer_date` | datetime | sí | ídem |
| `order_estimated_delivery_date` | datetime | no | |

**Grano:** `(order_id, dia_simulado)` — no `order_id` solo, porque un
mismo pedido puede re-emitirse en varios días.

### `bronze_order_items`

Copia cruda de `order_items.csv`. Grano: `(order_id, order_item_id)`.
Columnas: `order_id`, `order_item_id` (int), `product_id`, `seller_id`,
`shipping_limit_date` (datetime), `price` (float), `freight_value` (float)
— ninguna nullable.

### `bronze_order_payments`

Copia cruda de `order_payments.csv`. Grano: `(order_id, payment_sequential)`.
Columnas: `order_id`, `payment_sequential` (int), `payment_type` (string),
`payment_installments` (int), `payment_value` (float) — ninguna nullable.

### `bronze_order_reviews`

Copia cruda de `order_reviews.csv`. Grano nominal `review_id` (con
duplicados conocidos, sin deduplicar en Bronze — eso es trabajo de
Silver). Columnas: `review_id`, `order_id`, `review_score` (int),
`review_comment_title` (string, nullable), `review_comment_message`
(string, nullable), `review_creation_date` (datetime),
`review_answer_timestamp` (datetime).

### Tablas de referencia (`bronze_customers`, `bronze_products`, `bronze_sellers`, `bronze_geolocation`, `bronze_category_translation`)

Copia cruda 1:1 de sus CSVs fuente, snapshot completo (sin partición por
día). Columnas = columnas originales del CSV (ver
`notebooks/01_exploracion_olist.ipynb`, sección 1, para el schema exacto
inferido de cada una).

---

## 3. Capa Silver

Silver aplica: deduplicación, normalización de catálogo (ADR 0003),
resolución de tipos, construcción del historial SCD2, y validación
fail-fast (sección 4).

### `silver_orders`

Grano: `order_id` (deduplicado de las re-emisiones de Bronze — el
contenido es idéntico entre re-emisiones, se conserva una sola fila).
Mismas columnas que `bronze_orders` menos las de linaje.
**PK:** `order_id`. **FK:** `customer_id → silver_customers.customer_id`.

### `silver_order_status_history` (SCD2)

Ver diseño completo en sección 5. Grano: `(order_id, status_event)`.

### `silver_order_items`

Igual a `bronze_order_items`, tipado/validado.
**PK:** `(order_id, order_item_id)`. **FK:** `order_id → silver_orders`,
`product_id → silver_products`, `seller_id → silver_sellers`.

### `silver_order_payments`

Igual a `bronze_order_payments`, tipado/validado.
**PK:** `(order_id, payment_sequential)`. **FK:** `order_id → silver_orders`.

### `silver_order_reviews`

Deduplicado por `review_id`, quedándose con la fila de
`review_answer_timestamp` más reciente cuando hay duplicados.
**PK:** `review_id` (ya limpia tras el dedup). **FK:** `order_id → silver_orders`.

### `silver_customers`

Igual a `bronze_customers`, tipado/validado.
**PK:** `customer_id`. Se conserva `customer_unique_id` como columna (no
como PK — la resolución a persona real ocurre en Gold, `dim_cliente`).

### `silver_products`

Igual a `bronze_products` + normalización de catálogo (ADR 0003):
`product_category_name` nulo → `"sem_categoria"`;
`product_category_name_english` resuelto vía join contra
`silver_category_translation`, con fallback al nombre en portugués si no
hay traducción. **PK:** `product_id`.

### `silver_sellers`

Igual a `bronze_sellers`, tipado/validado. **PK:** `seller_id`.

### `silver_geolocation_agg`

Agregación de `bronze_geolocation` por `geolocation_zip_code_prefix`
(promedio de `lat`/`lng`) — la fuente no es 1:1 por zip prefix (Fase 0:
19,015 prefijos únicos en 1M de filas), así que no es utilizable como
dimensión sin agregar antes. **PK:** `geolocation_zip_code_prefix`.

### `silver_category_translation`

Igual a `bronze_category_translation`, sin cambios. **PK:**
`product_category_name`.

---

## 4. Reglas de calidad no negociables (fail-fast) por transición

Semántica: hard-stop total del batch (ADR 0001), alcance definido en
ADR 0003. Estas son las reglas concretas a implementar como schemas
Pandera en la Fase 3 — no hay ambigüedad pendiente.

### Bronze → Silver (dispara fail-fast)

- Nulo en cualquier columna marcada "no" en la tabla de nullability de la
  sección 2/3 (ej. `order_id`, `customer_id`, `product_id`, `order_status`,
  `price`, `freight_value`, `payment_value`).
- Violación de PK después de aplicar la deduplicación documentada (ej.
  `review_id` repetido tras quedarse con el más reciente).
- Huérfano de FK no documentado como esperado — hoy 0 casos en:
  `order_items→orders`, `order_items→products`, `order_items→sellers`,
  `order_payments→orders`, `order_reviews→orders`, `orders→customers`. Si
  aparece uno, es una regresión de la fuente y debe frenar el pipeline.
- Tipo no parseable (ej. un valor de `price` que no convierte a `float`,
  un timestamp fuera del formato esperado en una columna no documentada
  como "nulo válido").
- Medida imposible: `price < 0`, `freight_value < 0`, `payment_value < 0`,
  `payment_installments < 1`, `review_score` fuera de `[1, 5]`.

### Bronze → Silver (NO dispara fail-fast — se normaliza)

- `product_category_name` nulo → normalizado a `"sem_categoria"`.
- Categoría sin fila en `category_translation` → fallback al nombre en
  portugués.
- Timestamps de `orders` nulos cuando el `order_status` explica el nulo
  (ej. pedido no entregado sin `order_delivered_customer_date`).

### Silver → Gold (dispara fail-fast)

- Cualquier fila de un fact (`fct_pedidos`, `fct_pagos`) que no resuelva
  **todas** sus FKs contra las dimensiones correspondientes — debería ser
  imposible dado que Silver ya garantiza integridad referencial, pero se
  revalida como red de seguridad en el borde Silver→Gold.
- Grano duplicado en un fact (ej. dos filas para el mismo
  `(order_id, order_item_id)` en `fct_pedidos`).
- Medida imposible que haya sobrevivido agregaciones (mismos rangos que
  en Bronze→Silver).

---

## 5. Diseño SCD2 — `silver_order_status_history`

**Objetivo:** reconstruir la trayectoria de estados de un pedido a partir
de las re-emisiones de `bronze_orders` (ADR 0004), sin que Olist provea un
log de cambios explícito.

**Construcción:** para cada `order_id`, se toma su fila de `bronze_orders`
(el contenido es idéntico en todas sus re-emisiones) y se "despliega"
(unpivot) cada uno de sus 4 timestamps no nulos en una fila de evento,
ordenadas cronológicamente:

| `order_id` | `status_event` | Timestamp fuente |
| --- | --- | --- |
| — | `creado` | `order_purchase_timestamp` |
| — | `aprobado` | `order_approved_at` |
| — | `despachado` | `order_delivered_carrier_date` |
| — | `entregado` | `order_delivered_customer_date` |

Un pedido cancelado antes de aprobarse, por ejemplo, genera solo el evento
`creado` (los demás timestamps son nulos → no generan fila).

**Schema de `silver_order_status_history`:**

| Columna | Tipo | Notas |
| --- | --- | --- |
| `order_id` | string | FK a `silver_orders` |
| `status_event` | string | uno de `creado / aprobado / despachado / entregado` |
| `order_status_raw` | string | el `order_status` final tal cual la fuente (contexto) |
| `valid_from` | datetime | timestamp del evento |
| `valid_to` | datetime, nullable | timestamp del siguiente evento del mismo pedido; nulo si es el más reciente |
| `is_current` | bool | `true` solo en la fila con `valid_to` nulo |

**PK:** `(order_id, status_event)`. Máximo 4 filas por pedido.

Esta tabla es la que responde "¿cuál era el estado de este pedido en la
fecha X?" — no se modela como fact en Gold (el plan solo pide `fct_pedidos`
y `fct_pagos`); Gold consume el estado **actual** (`is_current = true`) a
través de `dim_estado_pedido`. El historial completo queda disponible en
Silver para quien lo necesite consultar directamente.

---

## 6. Capa Gold — modelo dimensional (star schema)

### Dimensiones

**`dim_cliente`** — grano: `customer_unique_id` (la persona real, no
`customer_id` que es por-pedido — ver Fase 0).
Columnas: `customer_unique_id` (PK), `customer_zip_code_prefix`,
`customer_city`, `customer_state`.

**`dim_producto`** — grano: `product_id`.
Columnas: `product_id` (PK), `product_category_name`,
`product_category_name_english`, `product_weight_g`, `product_length_cm`,
`product_height_cm`, `product_width_cm`, `product_photos_qty`.

**`dim_vendedor`** — grano: `seller_id`.
Columnas: `seller_id` (PK), `seller_zip_code_prefix`, `seller_city`,
`seller_state`.

**`dim_tiempo`** — grano: 1 fila por día calendario, cubriendo el rango de
fechas del dataset (2016-09 a 2018-10, confirmado en Fase 0).
Columnas: `fecha` (PK), `anio`, `mes`, `dia`, `dia_semana`, `nombre_mes`,
`es_fin_de_semana`.

**`dim_estado_pedido`** — grano: `status_event`.
Columnas: `status_event` (PK, uno de `creado/aprobado/despachado/entregado`),
`descripcion`.

### Hechos

**`fct_pedidos`** — grano: `(order_id, order_item_id)` (línea de pedido,
no el pedido completo — así se conserva el detalle de precio/flete por
producto).
Columnas: `order_id`, `order_item_id`, `customer_unique_id` (FK),
`product_id` (FK), `seller_id` (FK), `fecha_compra` (FK a `dim_tiempo`,
vía `order_purchase_timestamp`), `status_event` (FK a
`dim_estado_pedido`, estado actual del pedido). Medidas: `price`,
`freight_value`.

**`fct_pagos`** — grano: `(order_id, payment_sequential)`.
Columnas: `order_id`, `payment_sequential`, `customer_unique_id` (FK, vía
el pedido), `fecha_compra` (FK a `dim_tiempo`), `payment_type` (atributo
degenerado — baja cardinalidad, no amerita dimensión propia). Medidas:
`payment_value`, `payment_installments`.

### Diagrama ER

```mermaid
erDiagram
    dim_cliente ||--o{ fct_pedidos : "hace"
    dim_cliente ||--o{ fct_pagos : "paga"
    dim_producto ||--o{ fct_pedidos : "vendido en"
    dim_vendedor ||--o{ fct_pedidos : "vende"
    dim_tiempo ||--o{ fct_pedidos : "ocurre en"
    dim_tiempo ||--o{ fct_pagos : "ocurre en"
    dim_estado_pedido ||--o{ fct_pedidos : "estado actual"

    dim_cliente {
        string customer_unique_id PK
        int customer_zip_code_prefix
        string customer_city
        string customer_state
    }
    dim_producto {
        string product_id PK
        string product_category_name
        string product_category_name_english
        int product_weight_g
    }
    dim_vendedor {
        string seller_id PK
        int seller_zip_code_prefix
        string seller_city
        string seller_state
    }
    dim_tiempo {
        date fecha PK
        int anio
        int mes
        int dia
        string dia_semana
        bool es_fin_de_semana
    }
    dim_estado_pedido {
        string status_event PK
        string descripcion
    }
    fct_pedidos {
        string order_id
        int order_item_id
        string customer_unique_id FK
        string product_id FK
        string seller_id FK
        date fecha_compra FK
        string status_event FK
        float price
        float freight_value
    }
    fct_pagos {
        string order_id
        int payment_sequential
        string customer_unique_id FK
        date fecha_compra FK
        string payment_type
        float payment_value
        int payment_installments
    }
```
