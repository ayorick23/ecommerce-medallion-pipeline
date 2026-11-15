"""Puente entre la configuración del pipeline y dbt (ADR 0027).

dbt no lee ``config/pipeline.yaml``: sus rutas llegan por variables de entorno
sin valor por defecto (``profiles.yml`` y los *sources* fallan si faltan). Este
módulo las calcula a partir de la configuración y ejecuta dbt en un proceso
hijo (ADR 0037).

Uso para desarrollo, desde la raíz del repo::

    uv run gold-dbt <argumentos de dbt>     # p. ej. gold-dbt test -s dim_cliente

``gold-dbt`` escribe directo en ``warehouse.duckdb`` y **no** verifica el
manifiesto de Silver: es para iterar sobre modelos. El comando del pipeline es
``gold-build`` (ADR 0028).
"""

import os
import subprocess
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

# Lo mismo que ejecuta el script `dbt` (entry point dbt.cli.main:cli), con el
# intérprete actual. `-m dbt.cli.main` funciona pero dispara un RuntimeWarning
# de runpy, porque el paquete dbt.cli ya importa main.
DBT_ENTRY_POINT: Final = "from dbt.cli.main import cli; cli()"


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
    """Ejecuta ``dbt <args>`` en un proceso hijo con ``env`` definido; devuelve si terminó bien.

    En un proceso aparte y no con ``dbtRunner`` (ADR 0037): dbt-duckdb guarda la
    base abierta en un atributo de clase que sobrevive a la corrida, y un
    segundo ``gold_build`` en el mismo proceso no podría abrir ``warehouse.duckdb``.
    Al terminar el hijo, el sistema operativo libera el archivo.

    Las variables ``MEDALLION_*`` pisan las que ya existieran: las define el
    comando, no la persona (ADR 0027). Solo llegan al hijo; el entorno de quien
    llama no cambia.
    """
    project = str(DBT_PROJECT_DIR.resolve())
    command = [sys.executable, "-c", DBT_ENTRY_POINT, *args]
    command += ["--project-dir", project, "--profiles-dir", project]
    return subprocess.run(command, env={**os.environ, **env}, check=False).returncode == 0


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
