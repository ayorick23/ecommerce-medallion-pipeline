# CLAUDE.md

## Proyecto

Pipeline de Data Engineering para e-commerce (arquitectura medallion:
Bronze/Silver/Gold) con Polars, Airflow y DuckDB. Es el Proyecto 3 de un
portafolio profesional en construcción. El plan completo por fases vive en
`docs/plan-fases.md`; las decisiones de arquitectura, en `docs/decisions/`.
Ambos son la fuente de verdad — léelos antes de retomar trabajo en este repo.

## Control de versiones (obligatorio, sin excepciones)

- **Nunca** trabajar ni commitear directamente sobre `main`.
- Cada fase o sesión de trabajo se hace en una rama nueva, prefijada según
  su naturaleza: `feat/...`, `fix/...`, `docs/...`.
- Al terminar una sesión de trabajo: commit + push de la rama. El merge a
  `main` en GitHub lo hace el usuario manualmente — Claude nunca hace merge
  ni push a `main`.
- Los mensajes de commit **no** deben incluir una línea "Co-Authored-By" ni
  ninguna línea de coautoría, bajo ningún motivo. Esto anula la convención
  por defecto de agregar esa línea.

## Entorno y dependencias

- Gestión de paquetes y entorno con `uv`.
- Versión de Python fijada en `.python-version`.

## Calidad de código

- Linting y formateo con `ruff`.
- Type-checking con `mypy`.
- Ambos integrados en `.pre-commit-config.yaml`; deben pasar antes de cada
  commit.
- Un workflow de GitHub Actions replica estas mismas verificaciones (lint,
  type-check, tests) en cada push.

## Decisiones de arquitectura (ADR)

- Cada decisión de diseño no trivial se documenta en `docs/decisions/` como
  un archivo independiente: `NNNN-slug-descriptivo.md` (numeración
  secuencial de 4 dígitos, empezando en `0001`).
- Una ADR ya aceptada no se edita para cambiar la decisión: si la decisión
  cambia, se crea una nueva ADR que la reemplaza y se referencia la anterior
  como superada.
- El índice de ADRs vive al final de `docs/plan-fases.md`.

## Tests

- `tests/unit/`: espejo de la estructura de `src/`, una carpeta por
  responsabilidad (bronze, silver, gold, common, etc.).
- `tests/integration/`: pruebas que cruzan capas o validan el pipeline
  end-to-end.

## Versionamiento de datos

DVC se evaluará si resulta necesario para este proyecto — aún no decidido.
No agregarlo preventivamente; se revisa al diseñar la capa Bronze
(Fase 2 de `docs/plan-fases.md`).

## Cómo colaborar

Este proyecto se construye con enfoque de mentoría: diseño y discusión antes
que código, decisiones de arquitectura se toman en conjunto (no
unilateralmente, salvo lo ya decidido y registrado como ADR), y cada fase se
cierra con una revisión conjunta antes de avanzar a la siguiente.
