"""Punto de entrada que usa Airflow: una capa para un día (ADR 0040, 0041).

Uso, con el Python del venv del pipeline::

    python -m medallion.orchestration {bronze,silver,gold} AAAA-MM-DD [--config RUTA]

El código de salida es el contrato con el DAG, que corre en otro entorno y no
puede ver el tipo de excepción:

- ``0``: la capa quedó construida a la fecha D.
- ``3`` (``EXIT_DETERMINISTIC``): falló por una excepción conocida del
  pipeline (datos o configuración). Reintentar da el mismo error.
- Cualquier otro: lo que no se atrapa aquí escapa, Python sale con ``1`` y
  Airflow reintenta (disco lleno, ``warehouse.duckdb`` bloqueado, etc.).
"""

import argparse
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any, Final

from medallion.bronze.ingest import ingest_day, load_sources
from medallion.bronze.routing import RoutingError
from medallion.common.config import ConfigError, PipelineConfig, load_config
from medallion.common.storage import StorageError
from medallion.gold.build import GoldBuildError, gold_build, read_gold_build
from medallion.gold.dbt import warehouse_path
from medallion.silver.build import silver_build
from medallion.silver.validation import SilverValidationError

EXIT_DETERMINISTIC: Final = 3

# Fallos esperados: datos o configuración inválidos. Una excepción nueva de este
# tipo hay que agregarla aquí, o se reintentará como si fuera transitoria.
DETERMINISTIC_ERRORS: Final = (
    ConfigError,
    RoutingError,
    StorageError,
    SilverValidationError,
    GoldBuildError,
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def gold_needs_full_refresh(previous: Mapping[str, Any] | None, dia: date) -> bool:
    """Volver a correr un día que Gold ya alcanzó rebobina con full refresh (ADR 0040).

    También el mismo día: el incremental no vuelve a cargar las filas
    corregidas de un día que su marca de agua ya pasó.
    """
    return previous is not None and dia <= previous["as_of"]


def run_bronze(dia: date, config: PipelineConfig) -> str:
    results = ingest_day(dia, load_sources(config), config, _utc_now())
    written = sum(r.rows for r in results if r.action == "written")
    return f"Bronze({dia}): {written:,} filas escritas"


def run_silver(dia: date, config: PipelineConfig) -> str:
    manifest = silver_build(dia, config, _utc_now())
    return f"Silver({dia}): {sum(manifest['row_counts'].values()):,} filas"


def run_gold(dia: date, config: PipelineConfig) -> str:
    full_refresh = gold_needs_full_refresh(read_gold_build(warehouse_path(config)), dia)
    gold_build(dia, config, _utc_now(), full_refresh)
    return f"Gold({dia}) publicado ({'full refresh' if full_refresh else 'incremental'})"


LAYERS: Final[dict[str, Callable[[date, PipelineConfig], str]]] = {
    "bronze": run_bronze,
    "silver": run_silver,
    "gold": run_gold,
}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Corre una capa del pipeline para un día.")
    parser.add_argument("capa", choices=list(LAYERS))
    parser.add_argument("dia", type=date.fromisoformat, help="fecha D (AAAA-MM-DD)")
    parser.add_argument("--config", help="YAML de configuración (por defecto, el del pipeline)")
    args = parser.parse_args(argv)

    try:
        print(LAYERS[args.capa](args.dia, load_config(args.config)))
    except DETERMINISTIC_ERRORS as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return EXIT_DETERMINISTIC
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
