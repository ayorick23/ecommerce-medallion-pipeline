# Plantilla de ADR

Estructura que siguen las ADRs 0001–0038. Ejemplos completos: la 0029
(decisión con alternativas medidas) y la 0037 (supera parcialmente a otra).

```markdown
# NNNN — Título que enuncia la decisión, no el problema

**Estado:** Aceptada
**Fecha:** AAAA-MM-DD

## Contexto

Qué problema o fuerza obliga a decidir. Hechos y números medidos, con
referencias a las ADRs relacionadas. Sin la decisión todavía.

## Decisión

Qué se hace, en concreto: archivos, comandos, reglas. Lo bastante preciso
para implementarlo sin volver a discutirlo.

## Alternativas consideradas

- **Alternativa A:** qué es, y por qué no (su desventaja concreta).
- **Alternativa B:** ...

## Por qué

El criterio que inclinó la balanza. No repite la decisión.

## Consecuencias

- Qué cambia, incluidos los costos y lo que queda pendiente.
- Qué tests o verificaciones la respaldan.
```
