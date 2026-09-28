# 0028 — Escritura atómica de Gold: construir sobre una copia y reemplazar el archivo

**Estado:** Aceptada
**Fecha:** 2026-09-28

## Contexto

`dbt build` recorre el grafo en orden: construye un modelo, corre sus tests
y sigue con el siguiente. Cada modelo se escribe directo en
`warehouse.duckdb`, y dbt no tiene una transacción que abarque la corrida
completa. Si un test de `fct_reviews` falla, las dimensiones ya se
reconstruyeron con Silver(D), `fct_pedidos` ya hizo el merge de D, y
`fct_reviews` y lo que depende de ella quedó en D-1. Gold queda
**mezclado**, y quien lo consulte no tiene cómo saberlo.

En Silver el problema se resolvió con un manifiesto al final (ADR 0024),
que detecta la inconsistencia pero no la evita. Gold tiene dos diferencias:
es un solo archivo con estado (los hechos incrementales dependen de lo que
ya había, ADR 0029), y sus lectores naturales (un BI, un notebook con
DuckDB) no van a leer un JSON antes de consultar.

## Decisión

`gold-build --dia D` (ADR 0027) publica Gold de forma atómica:

1. **Guardas**, antes de tocar nada: manifiesto de Silver válido para D
   (ADR 0027), ruta de Gold local, y la guarda contra ir hacia atrás (más
   abajo).
2. **Copia:** `warehouse.duckdb` se copia a un temporal en
   `gold/_staging/`. Si no existe todavía, o si se pasó `--full-refresh`,
   el temporal parte vacío (así un full refresh no arrastra tablas de
   modelos que ya no existen).
3. **Construcción:** `MEDALLION_GOLD_DB` apunta al temporal y se corre
   `dbt build` contra él (con `--full-refresh` si corresponde).
4. **Metadatos:** con una conexión de DuckDB, se escribe la tabla
   `_gold_build` (una fila, la de la última corrida): `as_of` (D),
   `built_at`, el `built_at` del Silver usado, si fue full refresh y las
   filas por mart. Después `CHECKPOINT` y se cierra la conexión, para que
   todo quede en el archivo y no en un `.wal` aparte.
5. **Publicación:** `os.replace` del temporal sobre `warehouse.duckdb`.

Si cualquier paso falla, se borra el temporal y `warehouse.duckdb` queda
exactamente como estaba. Se reusa `atomic_write` de `medallion.common`
(el mismo mecanismo de staging + `os.replace` de las ADRs 0015 y 0024).

**Guarda contra ir hacia atrás:** si Gold ya está en un `as_of` posterior
a D, `gold-build --dia D` falla, salvo con `--full-refresh`. Correr el
mismo D otra vez está permitido (idempotencia).

## Alternativas consideradas

- **Escribir en sitio y confiar en el reintento:** es lo que hace dbt por
  defecto. El merge es idempotente, así que volver a correr D repara Gold,
  pero mientras tanto queda inconsistente sin ninguna señal.
- **Una marca al estilo del manifiesto de Silver:** detecta el problema,
  pero no lo evita, y un BI no la lee.
- **Esquema de staging dentro del mismo archivo, intercambiado en una
  transacción:** atómico, pero los modelos incrementales necesitan el
  estado anterior dentro del esquema de staging, lo que obliga a copiar
  tablas o a administrar el esquema desde dbt. Más complejo que la copia,
  sin ninguna ventaja.

## Por qué

Da a Gold la misma garantía que ya tienen Bronze y Silver ("no escribe nada
si algo falla"), y más fuerte que la de Silver: los lectores nunca ven un
Gold a medias. Encaja con lo ya decidido: la ruta de Gold es una variable
de entorno (ADR 0027), así que apuntarla al temporal no cuesta nada, y el
mecanismo de escritura ya existe y está probado. La guarda hacia atrás
convierte en regla del comando lo que la ADR 0029 exige (reprocesar hacia
atrás requiere full refresh).

## Consecuencias

- Costo: una copia del archivo por corrida. Silver completo pesa 31 MB, así
  que se espera un Gold de decenas de MB; se mide en la corrida real.
- `_gold_build` es consultable con SQL: cualquiera puede preguntarle a Gold
  a qué día corresponde sin leer ningún archivo aparte.
- En Windows, `os.replace` falla si otro proceso tiene abierto
  `warehouse.duckdb`. Es un error claro y coherente con la regla de un solo
  escritor (ADR 0005). En Linux (la imagen de Airflow) el reemplazo
  funciona y quien ya estaba leyendo sigue viendo la versión anterior
  completa.
- Mientras corre `gold-build`, los lectores siguen viendo el Gold anterior.
  Dos `gold-build` simultáneos se pisarían al reemplazar: serializar las
  corridas sigue pendiente para la Fase 5.
- Una corrida interrumpida puede dejar huérfanos en `gold/_staging/`, sin
  efecto sobre los lectores (igual que en Bronze y Silver).
