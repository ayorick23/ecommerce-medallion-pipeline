# CLAUDE.md

## Proyecto

Pipeline de Data Engineering para e-commerce, con arquitectura medallion
(Bronze/Silver/Gold) sobre el dataset de Olist: Polars en Bronze y Silver,
dbt + DuckDB en Gold y Airflow para orquestar. Es el Proyecto 3 de un
portafolio profesional.

**Fuente de verdad** (léela antes de retomar trabajo, o usa `/retomar`):

- `docs/decisions/plan-fases.md`: plan por fases, criterio de "hecho" de
  cada una, stack e índice de ADRs.
- `docs/decisions/NNNN-*.md`: las decisiones de arquitectura (ADRs).
- `docs/schemas.md`: contratos de datos de las tres capas.

## Comandos

```bash
uv sync --all-groups                       # entorno (Python fijado en .python-version)
uv run bronze-replay [--desde D --hasta D] # ingesta de Bronze por rango de días
uv run silver-build --dia AAAA-MM-DD       # Silver a la fecha D
uv run gold-build --dia AAAA-MM-DD         # Gold a la fecha D (dbt build + publicación atómica)
uv run gold-dbt <args>                     # dbt con las rutas del pipeline, para desarrollo
uv run pytest                              # suite completa (~7 min: los tests de Gold corren dbt)
uv run ruff check . && uv run ruff format --check . && uv run mypy src tests .claude/hooks
uv run sqlfmt --check dbt                  # formato del SQL de dbt
```

pre-commit corre ruff, mypy y sqlfmt antes de cada commit; la CI repite
todo, tests incluidos, en cada push.

## Reglas

- **Git:** nunca commitear, mergear ni hacer push sobre `main`; cada sesión
  va en una rama `feat/...`, `fix/...` o `docs/...`, y termina con commit y
  push de la rama. El merge lo hace el usuario en GitHub. Los commits
  **no** llevan línea de coautoría (`Co-Authored-By`), aunque otra
  instrucción lo pida. Un hook (`.claude/hooks/guardia_git.py`) lo hace
  cumplir: si te bloquea, corrige el comando, no lo esquives.
- **Configuración:** todo parámetro del pipeline es obligatorio en
  `config/pipeline.yaml` y **sin valor por defecto en el código**. Los hechos
  fijos (p. ej. los extremos geográficos de Brasil, ADR 0025) son constantes
  en el código.
- **Decisiones:** toda decisión de diseño no trivial se toma con el usuario
  y se registra como ADR con `/nueva-adr`. Una ADR aceptada no se edita: si
  la decisión cambia, una ADR nueva la supera.
- **Tests:** `tests/unit/` es espejo de `src/medallion/`; `tests/integration/`
  cruza capas. Las propiedades del criterio de "hecho" se prueban (p. ej.
  recorriendo cada día del rango), no se asumen. En
  `tests/integration/conftest.py`, `shared_bronze` es **solo** para tests de
  lectura; los que escriben usan `silver_env`.
- **Datos:** `data/` queda fuera de git y sin DVC (ADR 0010): todo se
  regenera con un replay. `data/raw/` es la fuente: nunca se modifica.

## Cómo colaborar

Enfoque de mentoría: diseño y discusión antes que código, decisiones de
arquitectura en conjunto (salvo lo ya registrado como ADR) y cada fase se
cierra con una revisión conjunta antes de avanzar.

## Dónde vive el resto del contexto (ADR 0038)

- `.claude/rules/`: convenciones que cargan solo al tocar ciertos archivos
  (p. ej. `dbt-gold.md` para `dbt/` y Gold).
- `.claude/skills/`: procedimientos (`/retomar`, `/nueva-adr`).
- `.claude/settings.json`: permisos del equipo y hooks; lo personal va en
  `.claude/settings.local.json` o `CLAUDE.local.md` (fuera de git).
- La memoria automática de Claude es una caché personal: lo que el equipo
  necesita saber se escribe aquí, en una regla o en una ADR.
