"""Puente entre la configuración del pipeline y dbt (ADR 0027).

dbt no lee ``config/pipeline.yaml``: sus rutas llegan por variables de entorno
sin valor por defecto (``profiles.yml`` y los *sources* fallan si faltan). Este
módulo las calcula a partir de la configuración y ejecuta dbt en el mismo
proceso con ``dbtRunner``.

Uso para desarrollo, desde la raíz del repo::

    uv run gold-dbt <argumentos de dbt>     # p. ej. gold-dbt test -s dim_cliente

``gold-dbt`` escribe directo en ``warehouse.duckdb`` y **no** verifica el
manifiesto de Silver: es para iterar sobre modelos. El comando del pipeline es
``gold-build`` (ADR 0028).
"""

import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

from medallion.common.config import PipelineConfig, load_config
from medallion.common.storage import local_path

# Proyecto dbt, relativo a la raíz del repo (igual que config/pipeline.yaml).
DBT_PROJECT_DIR: Final = Path("dbt")
WAREHOUSE_FILE: Final = "warehouse.duckdb"

SILVER_URI_ENV: Final = "MEDALLION_SILVER_URI"
GOLD_DB_ENV: Final = "MEDALLION_GOLD_DB"


def warehouse_path(config: PipelineConfig) -> Path:
    """Ruta absoluta de ``warehouse.duckdb``; una raíz remota es un error (ADR 0027)."""
    return (local_path(config.layer_uri("gold")) / WAREHOUSE_FILE).resolve()


def dbt_env(config: PipelineConfig, gold_db: Path) -> dict[str, str]:
    """Variables de entorno que espera el proyecto dbt.

    Las rutas van absolutas y con ``/``: las vistas de staging guardan la ruta
    de Silver dentro del archivo DuckDB, y una ruta relativa dependería del
    directorio desde donde se abra (ADR 0026).
    """
    silver = local_path(config.layer_uri("silver")).resolve()
    return {SILVER_URI_ENV: silver.as_posix(), GOLD_DB_ENV: gold_db.resolve().as_posix()}


def run_dbt(args: Sequence[str], env: Mapping[str, str]) -> bool:
    """Ejecuta ``dbt <args>`` con ``env`` definido; devuelve si terminó bien.

    Pisa las variables ``MEDALLION_*`` que ya existieran: las define el comando,
    no la persona (ADR 0027).
    """
    # dbt tarda en importarse y solo lo instala el grupo `gold` (ADR 0026).
    from dbt.cli.main import dbtRunner

    os.environ.update(env)
    project = str(DBT_PROJECT_DIR.resolve())
    result = dbtRunner().invoke([*args, "--project-dir", project, "--profiles-dir", project])
    return result.success


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0].startswith("-"):
        print("uso: gold-dbt <comando de dbt> [argumentos]", file=sys.stderr)
        return 2
    config = load_config()
    gold_db = warehouse_path(config)
    gold_db.parent.mkdir(parents=True, exist_ok=True)
    return 0 if run_dbt(args, dbt_env(config, gold_db)) else 1


if __name__ == "__main__":
    raise SystemExit(main())
