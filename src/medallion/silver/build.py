"""Construcción de Silver "a la fecha D" (ADR 0017-0025).

Uso, desde la raíz del repo::

    uv run silver-build --dia AAAA-MM-DD [--config RUTA]

Lee Bronze hasta D, construye y valida todas las tablas en memoria y, solo si
no hay ninguna falla, las escribe (ADR 0023, 0024). En la Fase 5 Airflow llama
a ``silver_build`` después de ingerir Bronze del día.
"""

import argparse
import json
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final

import polars as pl

from medallion.bronze.storage import PARTITION_COLUMN
from medallion.bronze.tables import EVENT_TABLES
from medallion.common.config import PipelineConfig, load_config
from medallion.common.storage import atomic_write, local_path
from medallion.silver.facts import build_fact_tables
from medallion.silver.orders import build_order_tables
from medallion.silver.parsing import parse_tables
from medallion.silver.read import read_bronze
from medallion.silver.reference import build_reference
from medallion.silver.validation import SilverValidationError, raise_if_failed, validate_silver

TABLE_FILE: Final = "part-0.parquet"
MANIFEST_FILE: Final = "_manifest.json"
PENDING_FILE: Final = Path("_pendientes") / "order_reviews.parquet"


def _utc_now() -> datetime:
    return datetime.now(UTC)


def table_path(silver_uri: str, table: str) -> Path:
    return local_path(silver_uri) / table / TABLE_FILE


def pending_path(silver_uri: str) -> Path:
    return local_path(silver_uri) / PENDING_FILE


def manifest_path(silver_uri: str) -> Path:
    return local_path(silver_uri) / MANIFEST_FILE


def build_tables(
    raw: Mapping[str, pl.DataFrame], dia: date, grace_days: int
) -> tuple[dict[str, pl.DataFrame], pl.DataFrame]:
    """Todas las tablas de Silver(``dia``) y las reviews pendientes, ya validadas.

    Junta las fallas de conversión y de validación y lanza una sola
    ``SilverValidationError`` si hay alguna (ADR 0023).
    """
    parsed, failures = parse_tables(raw)
    orders = build_order_tables(parsed["orders"], dia)
    facts, pending = build_fact_tables(parsed, orders["orders"], dia)
    tables = {**orders, **facts, **build_reference(parsed)}
    failures.extend(validate_silver(tables, pending, grace_days))
    raise_if_failed(failures)
    return tables, pending


def _bronze_days(raw: Mapping[str, pl.DataFrame]) -> dict[str, Any]:
    days = pl.concat([raw[table][PARTITION_COLUMN] for table in EVENT_TABLES]).unique()
    first, last = days.min(), days.max()
    return {
        "first": first.isoformat() if isinstance(first, date) else None,
        "last": last.isoformat() if isinstance(last, date) else None,
        "count": days.len(),
    }


def write_silver(
    tables: Mapping[str, pl.DataFrame],
    pending: pl.DataFrame,
    manifest: Mapping[str, Any],
    silver_uri: str,
) -> None:
    """Escribe Silver con el manifiesto al final (ADR 0024).

    El manifiesto se borra antes de tocar cualquier tabla: si la escritura se
    corta a mitad, Silver queda sin manifiesto, es decir, marcado como
    incompleto. Volver a correr el mismo día lo repara.
    """
    manifest_path(silver_uri).unlink(missing_ok=True)
    for table, df in tables.items():
        atomic_write(table_path(silver_uri, table), silver_uri, df.write_parquet)
    atomic_write(pending_path(silver_uri), silver_uri, pending.write_parquet)
    content = json.dumps(manifest, indent=2, ensure_ascii=False) + "\n"
    atomic_write(
        manifest_path(silver_uri),
        silver_uri,
        lambda path: path.write_text(content, encoding="utf-8"),
    )


def read_manifest(silver_uri: str) -> dict[str, Any] | None:
    """Manifiesto de la última corrida completa, o ``None`` si Silver está incompleto."""
    path = manifest_path(silver_uri)
    if not path.is_file():
        return None
    manifest: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return manifest


def silver_build(dia: date, config: PipelineConfig, built_at: datetime) -> dict[str, Any]:
    """Construye, valida y escribe Silver(``dia``); devuelve el manifiesto escrito.

    Si la validación falla, lanza ``SilverValidationError`` sin escribir nada:
    el Silver anterior, con su manifiesto, queda intacto.
    """
    if built_at.tzinfo is None:
        raise ValueError("built_at debe tener zona horaria (p. ej. datetime.now(UTC))")
    raw = read_bronze(config.layer_uri("bronze"), dia)
    tables, pending = build_tables(raw, dia, config.early_arriving_grace_days)
    manifest = {
        "as_of": dia.isoformat(),
        "built_at": built_at.astimezone(UTC).isoformat(),
        "bronze_days": _bronze_days(raw),
        "row_counts": {table: df.height for table, df in tables.items()},
        "pending_reviews": pending.height,
    }
    write_silver(tables, pending, manifest, config.layer_uri("silver"))
    return manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Construye Silver a la fecha D.")
    parser.add_argument(
        "--dia", type=date.fromisoformat, required=True, help="fecha D (AAAA-MM-DD)"
    )
    parser.add_argument("--config", help="YAML de configuración (por defecto, el del pipeline)")
    args = parser.parse_args(argv)

    start = time.perf_counter()
    try:
        manifest = silver_build(args.dia, load_config(args.config), _utc_now())
    except SilverValidationError as error:
        print(error, file=sys.stderr)
        return 1

    for table, rows in manifest["row_counts"].items():
        print(f"{table:22} {rows:>9,}")
    print(f"{'reviews pendientes':22} {manifest['pending_reviews']:>9,}")
    print(f"\nSilver({args.dia}) escrito en {time.perf_counter() - start:.1f} s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
