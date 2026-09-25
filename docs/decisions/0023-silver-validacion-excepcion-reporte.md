# 0023 — Validación de Silver: todas las fallas en una `SilverValidationError`

**Estado:** Aceptada
**Fecha:** 2026-09-25

## Contexto

La ADR 0001 fija que cualquier falla de validación detiene todo (hard
stop), y la ADR 0003 fija qué cuenta como falla. Falta decidir cómo se
detectan las fallas, qué se lanza y qué información deja una corrida
fallida.

## Decisión

- Silver se construye y valida **entero en memoria** antes de escribir
  nada. Pandera valida cada tabla en modo *lazy* (junta todas las fallas
  de la tabla en lugar de cortar en la primera). Las reglas entre tablas
  (FK, reviews vencidas de la ADR 0020) y las de conversión de tipos
  aportan sus fallas al mismo reporte.
- Si hay al menos una falla, se lanza **una sola**
  `SilverValidationError`, con un reporte estructurado. Por cada falla:
  tabla, regla, columna(s), cantidad de filas afectadas y hasta 5
  ejemplos. El mensaje de la excepción es legible, y el reporte queda
  disponible como atributo para los tests.
- Si la validación falla, **no se escribe nada** en Silver (ADR 0024).
- Mismo patrón que `RoutingError` en Bronze: excepción propia y ejemplos
  acotados a 5.

## Alternativas consideradas

- **Lanzar el primer error de Pandera tal cual:** muestra un error por
  corrida, en el formato interno de Pandera, y Airflow ve una excepción
  genérica.
- **Además, persistir el reporte en JSON** (`silver/_reportes/`): deja un
  registro auditable, pero los logs de Airflow cubren buena parte de esa
  necesidad. **Se difiere a la Fase 5**, para decidirlo al ver qué dan
  esos logs.

## Por qué

Una corrida fallida muestra todo lo que está mal de una vez, con un
mensaje que se entiende sin conocer Pandera, y con un tipo de excepción
propio que Airflow va a mostrar en el log de la tarea.

## Consecuencias

- Los tests de fail-fast verifican el contenido del reporte (regla y
  cantidad), no solo que se lance la excepción.
- En la Fase 5 hay que decidir si el reporte también se persiste.
