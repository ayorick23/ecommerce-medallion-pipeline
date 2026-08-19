# 0001 — Semántica de fail-fast: hard stop total

**Estado:** Aceptada
**Fecha:** 2026-08-18

## Contexto

El pipeline debe decidir qué hacer cuando un registro falla la validación de
datos en una transición entre capas (Bronze→Silver, Silver→Gold). Las
alternativas típicas en ingeniería de datos son detener todo el batch,
aislar solo los registros inválidos, o aplicar una política distinta según
la capa.

## Decisión

Ante cualquier registro que falle la validación en una transición de capa,
se descarta el batch completo y el DAG falla. No hay cuarentena parcial en
el MVP.

## Alternativas consideradas

- **Cuarentena por registro:** los registros inválidos se mueven a una tabla
  de rechazados con el motivo del fallo; los válidos continúan.
- **Híbrido por capa:** cuarentena en Bronze→Silver (los datos crudos
  siempre traen ruido real), hard-stop en Silver→Gold (el modelo dimensional
  no debe contaminarse).

## Por qué

Para un proyecto de portafolio, el hard-stop total es más fácil de razonar,
de testear y de explicar en una entrevista técnica, y demuestra el concepto
de "fail-fast real" sin ambigüedad. La cuarentena es un patrón más realista
para producción, pero introduce decisiones adicionales (qué es "tolerable",
cómo se reprocesan los rechazados) que no aportan al objetivo central de
este proyecto.

## Consecuencias

- Queda anotada como posible extensión futura (cuarentena) si el tiempo del
  proyecto lo permite.
- Simplifica la implementación de Fase 3/4: no hay que diseñar una tabla de
  rechazados ni su ciclo de vida.
