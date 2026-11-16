---
name: nueva-adr
description: Registra una decisión de arquitectura ya tomada como ADR en docs/decisions/, con numeración, plantilla, índice y, si corresponde, la marca de "superada" en la ADR anterior.
argument-hint: "[título de la decisión]"
disable-model-invocation: true
allowed-tools: Bash(ls docs/decisions*) Bash(git branch --show-current)
---

# Nueva ADR: $ARGUMENTS

## Estado actual

Rama: !`git branch --show-current`

ADRs existentes (la nueva lleva el número siguiente al último):

!`ls docs/decisions`

## Antes de escribir

1. **La decisión tiene que estar tomada con el usuario.** En este proyecto las
   decisiones se toman en conjunto (CLAUDE.md). Si de la conversación no queda
   clara la opción elegida, las alternativas y el porqué, pregúntalo antes de
   escribir. No inventes alternativas que no se discutieron.
2. **Nunca en `main`.** Si la rama de arriba es `main`, crea una rama
   `docs/...` (o la del trabajo en curso) antes de tocar archivos.
3. **¿Cambia una decisión ya aceptada?** Entonces es una ADR nueva que la
   supera; la anterior **no** se edita salvo su línea de estado (paso 3 abajo).

## Pasos

1. Crea `docs/decisions/NNNN-slug-descriptivo.md`: `NNNN` es el siguiente
   número de 4 dígitos y el slug va en minúsculas, sin tildes, con guiones.
2. Escríbela con la estructura de [plantilla.md](plantilla.md). Fecha de hoy,
   estado `Aceptada`.
3. Si supera a otra, total o parcialmente:
   - En la nueva: `**Estado:** Aceptada — supera parcialmente a la [NNNN](...) (qué parte)`.
   - En la anterior, solo la línea de estado:
     `**Estado:** Aceptada — parcialmente superada por [NNNN](...) (qué parte)`.
   - En el índice, la fila de la anterior también lo dice.
4. Agrega la fila al índice al final de `docs/decisions/plan-fases.md`, en
   orden: `| [NNNN](archivo.md) | Título | Aceptada |`.
5. Si la decisión cambia algo que describen `docs/schemas.md`, el
   `CLAUDE.md` o una regla de `.claude/rules/`, actualízalos en el mismo
   commit: la documentación y el contexto del agente no se desfasan.
6. Muestra al usuario la ADR y los archivos tocados. No hagas commit hasta
   que la revise.

## Estilo

Español, tuteo, frases cortas. Cada alternativa con su desventaja concreta.
"Por qué" explica el criterio, no repite la decisión. Las consecuencias
incluyen los costos, no solo los beneficios. Números medidos cuando los hay.
