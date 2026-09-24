# 0016 — Proyecto instalable como paquete `medallion`

**Estado:** Aceptada
**Fecha:** 2026-09-24
**Revierte:** la configuración `[tool.uv] package = false` de la Fase 0
(no estaba registrada como ADR)

## Contexto

En la Fase 0 el proyecto se marcó como no instalable (`package = false`):
es una aplicación que se ejecuta, no una librería que otros instalan, y así
se evitaba configurar un *build backend*. El código vive en `src/` como
cuatro paquetes de primer nivel (`bronze`, `common`, `silver`, `gold`).

Como el proyecto no está instalado, Python solo encuentra esos paquetes si
alguien agrega `src/` a la ruta de importación. Hasta la Fase 2 cada
herramienta lo resolvía por su cuenta:

- pytest, con `pythonpath = ["src"]` en `pyproject.toml`;
- mypy, con `mypy_path = "src"`;
- el replay por terminal, con `$env:PYTHONPATH = "src"` antes de cada
  comando.

Cada consumidor nuevo del código necesitaría su propio parche: los
notebooks, un script cualquiera y, sobre todo, Airflow en la Fase 5, que
importa el código desde sus tareas.

## Decisión

- El proyecto se instala como paquete (se quita `package = false`) con el
  backend de uv (`uv_build`). `uv sync` lo instala en modo **editable**:
  los cambios en `src/` se ven sin reinstalar.
- Todo el código vive bajo **un solo paquete raíz, `medallion`**:
  `src/medallion/{common,bronze,silver,gold}`. Los imports pasan a ser
  `from medallion.bronze.ingest import ingest_day`.
- El replay se expone como comando del proyecto:
  `uv run bronze-replay [--desde] [--hasta] [--config]`.
- pytest deja de necesitar `pythonpath`. mypy conserva `mypy_path = "src"`
  para analizar el código fuente directamente.
- `tests/unit/` sigue siendo espejo de las capas (`tests/unit/bronze/` ↔
  `src/medallion/bronze/`).

## Alternativas consideradas

- **Seguir con `PYTHONPATH=src`:** funciona, pero cada entorno nuevo
  (notebooks, Airflow, un colaborador) tiene que conocer el truco, y el
  error cuando se olvida (`ModuleNotFoundError`) no dice cómo arreglarlo.
- **Instalar los cuatro paquetes sueltos** (`bronze`, `common`, ...): no
  cambia ningún import, pero instala nombres muy genéricos en el entorno.
  `common` o `gold` pueden chocar con otras librerías, sobre todo en un
  entorno compartido como el de Airflow.
- **`hatchling` como backend:** el más usado, pero agrega una herramienta
  más; `uv_build` viene del mismo gestor que ya usa el proyecto y soporta
  el *src layout* sin configuración extra.

## Por qué

Un paquete instalado es la forma estándar de que Python encuentre el
código, sin parches por herramienta. Un único paquete raíz es la
convención del *src layout*: deja claro qué es código del proyecto
(`medallion.*`) y qué es una dependencia externa, y no ocupa nombres
genéricos. El argumento de la Fase 0 ("es una aplicación, no una
librería") sigue siendo cierto, pero instalar el paquete no lo contradice:
aquí no se publica en ningún lado, solo se instala en el propio entorno.

## Consecuencias

- Cambian todos los imports (`bronze.` → `medallion.bronze.`,
  `common.` → `medallion.common.`) en código y tests.
- Airflow (Fase 5) podrá importar `medallion` con solo instalar el
  proyecto en su imagen.
- El nombre del paquete (`medallion`) es distinto del nombre del proyecto
  (`ecommerce-medallion-pipeline`), así que hay que indicarlo en la
  configuración del backend.
