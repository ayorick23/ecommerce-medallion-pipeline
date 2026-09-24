# 0010 — No usar DVC para versionar datos

**Estado:** Aceptada
**Fecha:** 2026-09-23

## Contexto

`CLAUDE.md` dejó abierta la evaluación de DVC (Data Version Control) hasta
el diseño de la capa Bronze (Fase 2). DVC versiona datos y artefactos
grandes fuera de git (con punteros en el repo) y es útil cuando los datos
cambian y hay que poder volver a una versión anterior exacta.

## Decisión

No se incorpora DVC al proyecto. `data/` sigue fuera de git.

## Alternativas consideradas

- **DVC sobre `data/raw/`**: garantizaría que todos usan exactamente los
  mismos CSV de Olist.
- **DVC sobre Bronze/Silver/Gold**: versionaría las salidas del pipeline.

## Por qué

- La fuente (`data/raw/`) es un dataset público **estático** de Kaggle:
  no cambia, no hay versiones que rastrear.
- Bronze/Silver/Gold son **derivados reproducibles**: se regeneran
  corriendo el pipeline sobre `raw/`. Bronze append-only con linaje
  (`dia_simulado`, `source_file`, `batch_hash` — ADR 0004) ya da la
  trazabilidad que interesa demostrar aquí.
- DVC ya está demostrado en el Proyecto 2 del portafolio (MLOps), donde sí
  tiene justificación real (versionar datasets de entrenamiento y
  modelos). Repetirlo aquí no aporta una habilidad nueva.

## Consecuencias

- El README (Fase 8) debe indicar cómo obtener los CSV de Olist (Kaggle) y
  dónde colocarlos.
- Si en el futuro la fuente pasara a ser dinámica, esta decisión se
  revisa con una nueva ADR.
