# 0035 — Coordenadas como atributos de `dim_cliente` y `dim_vendedor`, sin `dim_ubicacion`

**Estado:** Aceptada
**Fecha:** 2026-09-28

## Contexto

`silver_geolocation_agg` tiene una latitud y longitud promedio por código
postal (ADR 0025). No es FK de clientes ni vendedores: algunos códigos no
tienen geolocalización, y eso no es un error. Medido en el diseño: 279
`customer_id` y 7 vendedores quedan sin coordenadas. Hay que decidir cómo llegan
las coordenadas a Gold.

## Decisión

- `geolocation_lat` y `geolocation_lng` son **atributos**:
  - de `dim_cliente`, por versión (ADR 0030): cada dirección tiene sus
    coordenadas;
  - de `dim_vendedor`.
- Se unen por código postal con un `LEFT JOIN`: la coordenada puede ser
  nula, sin fail-fast.
- **Sin `dim_ubicacion`.**
- La distancia cliente–vendedor (haversine) queda anotada como extensión
  para la Fase 8; no se calcula en esta fase.

## Alternativas consideradas

- **`dim_ubicacion` por código postal** (compartida, o como *outrigger* de
  cliente y vendedor): agrega un join sin aportar atributos nuevos. Ciudad
  y estado ya están en cada dimensión, y el código postal por sí solo no es
  una entidad de análisis.
- **Fail-fast ante coordenadas nulas:** sería declarar error algo que
  Silver ya decidió que no lo es (sección 3 de `docs/schemas.md`).
- **Calcular ya la distancia en `fct_pedidos`:** interesante (distancia
  frente a días de entrega), pero es alcance nuevo; se anota para la
  narrativa de la Fase 8.

## Por qué

Las coordenadas describen una dirección, así que viven donde vive la
dirección. Menos tablas y joins, y el caso nulo queda explícito en la
documentación en lugar de esconderse en un join interno que perdería
filas.

## Consecuencias

- Los análisis geográficos filtran las coordenadas nulas de forma
  explícita. Se documenta en la descripción de las columnas.
- Test: `not_null` **no** se aplica a las coordenadas; sí se prueba que,
  cuando existen, estén dentro de los extremos de Brasil (ADR 0025).
