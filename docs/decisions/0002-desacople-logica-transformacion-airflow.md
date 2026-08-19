# 0002 — Desacoplar la lógica de transformación de Airflow

**Estado:** Aceptada
**Fecha:** 2026-08-18

## Contexto

Hay que decidir dónde vive la lógica de transformación de Bronze/Silver/Gold:
directamente dentro de los operators/tasks de Airflow, o como módulos
independientes que Airflow solo invoca.

## Decisión

Bronze/Silver/Gold se implementan como funciones Python puras en `src/`,
testeables con pytest sin levantar Airflow. Los DAGs en `dags/` son
envoltorios delgados que importan y llaman a esas funciones.

## Alternativas consideradas

- Escribir la lógica de transformación directamente dentro de los
  `PythonOperator`/`@task` de los DAGs.

## Por qué

Es la práctica estándar en la industria. Permite testear la lógica de
negocio en segundos (sin scheduler, sin metastore), y separa claramente
"qué hace el pipeline" de "cómo se orquesta" — dos responsabilidades
distintas que no deben mezclarse.

## Consecuencias

- Determina la estructura de `src/` desde la Fase 0/2.
- `tests/unit/` puede cubrir toda la lógica de negocio sin dependencia de
  Airflow corriendo.
