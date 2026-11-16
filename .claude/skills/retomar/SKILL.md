---
name: retomar
description: Resume en qué quedó el proyecto para retomar una sesión nueva — fase actual del plan y su criterio de "hecho", trabajo sin mergear, PRs abiertos y opciones para hoy. Úsala al empezar una sesión o cuando el usuario pregunte en qué nos quedamos, desde dónde seguimos o qué falta.
allowed-tools: Bash(git branch *) Bash(git status *) Bash(git log *) Bash(git fetch *) Bash(gh pr list *) Bash(grep *) Bash(ls docs/decisions*)
---

# Retomar el proyecto

El estado se **deriva** de fuentes versionadas, no de la memoria (ADR 0038).

## Estado de git

Rama actual: !`git branch --show-current`

Cambios sin commitear:

!`git status --short`

Últimos commits:

!`git log --oneline --decorate -8`

Ramas con trabajo que todavía no está en `origin/main` (después de un fetch):

!`git fetch --quiet --prune 2>/dev/null; git branch -a --no-merged origin/main || true`

PRs abiertos:

!`gh pr list --state open --json number,title,headRefName,createdAt 2>/dev/null || echo "(gh no disponible)"`

## Estado del plan

Fases y cierres en `docs/decisions/plan-fases.md` (con número de línea):

!`grep -n "^### Fase\|^\*\*Cerrada" docs/decisions/plan-fases.md`

Últimas ADRs:

!`ls docs/decisions | grep -E "^[0-9]{4}" | tail -3`

## Qué hacer

1. **Fase actual:** la siguiente a la *última* marcada `**Cerrada**` (las
   fases 0 a 2 se cerraron antes de que existiera esa marca). Lee en
   `plan-fases.md` su sección completa: entregables, diseño cerrado y
   criterio de "hecho".
2. **Cruza el plan con git:** qué puntos del criterio ya tienen código o
   tests en la rama actual o en ramas sin mergear, y cuáles no. Si hay
   trabajo sin mergear o cambios sin commitear, va primero: el usuario tiene
   que decidir qué hacer con eso.
3. **La memoria automática es una pista, no la fuente.** Si dice algo sobre
   acuerdos o próximos pasos, verifícalo contra lo anterior antes de usarlo,
   y si está desactualizada, corrígela.
4. **Responde corto, con este formato:**
   - **Dónde quedamos:** 2–3 líneas con hechos (rama, último trabajo, PRs).
   - **Pendiente de la fase actual:** los puntos del criterio sin cumplir.
   - **Sin mergear / sin commitear:** si hay algo, qué y qué decidir.
   - **Opciones para hoy:** 2–3 caminos (seguir el plan, cerrar lo abierto,
     cambiar el orden si hay un motivo como una fecha límite), con tu
     recomendación y el porqué.
5. **No empieces a trabajar.** Termina preguntando por dónde seguimos: el
   usuario decide el plan del día.
