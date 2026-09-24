"""Replay de Bronze: ingiere un rango de días simulados, en orden (ADR 0004).

Uso, desde la raíz del repo::

    uv run bronze-replay [--desde AAAA-MM-DD] [--hasta AAAA-MM-DD] [--config RUTA]

Sin fechas, recorre todo el rango de la fuente. En la Fase 5 Airflow llama a
``ingest_day`` día por día; este módulo sirve para correr el replay a mano.
"""

import argparse
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta

import polars as pl

from medallion.bronze.ingest import TableResult, ingest_day, load_sources
from medallion.bronze.routing import anchor_day
from medallion.bronze.tables import (
    EVENT_TABLES,
    ORDER_EVENT_ANCHORS,
    REFERENCE_TABLES,
    REVIEW_ANCHOR,
)
from medallion.common.config import PipelineConfig, load_config


def _utc_now() -> datetime:
    return datetime.now(UTC)


def source_day_range(sources: Mapping[str, pl.DataFrame]) -> tuple[date, date]:
    """Primer y último día con algún evento ancla en la fuente."""
    days = pl.concat(
        [anchor_day(sources["orders"], c).alias("dia") for c in ORDER_EVENT_ANCHORS]
        + [anchor_day(sources["order_reviews"], REVIEW_ANCHOR).alias("dia")]
    )
    first, last = days.min(), days.max()
    if not isinstance(first, date) or not isinstance(last, date):
        raise ValueError("La fuente no tiene ningún evento con fecha")
    return first, last


def replay(
    sources: Mapping[str, pl.DataFrame],
    config: PipelineConfig,
    desde: date,
    hasta: date,
    *,
    clock: Callable[[], datetime] = _utc_now,
) -> Iterator[tuple[date, list[TableResult]]]:
    """Ingiere cada día de ``desde`` a ``hasta`` (inclusive), en orden.

    Es un generador: cada día se ingiere recién cuando se pide el siguiente
    resultado. ``ingested_at`` se toma de ``clock`` al empezar cada día.
    """
    if desde > hasta:
        raise ValueError(f"Rango vacío: desde={desde} es posterior a hasta={hasta}")
    dia = desde
    while dia <= hasta:
        yield dia, ingest_day(dia, sources, config, clock())
        dia += timedelta(days=1)


def _day_line(dia: date, results: list[TableResult]) -> str:
    rows = {r.table: r.rows for r in results if r.action == "written"}
    events = " ".join(f"{t}={rows.get(t, 0)}" for t in EVENT_TABLES)
    snapshots = [t for t in REFERENCE_TABLES if t in rows]
    return f"{dia}  {events}" + (f"  snapshots={','.join(snapshots)}" if snapshots else "")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay de Bronze sobre un rango de días.")
    parser.add_argument("--desde", type=date.fromisoformat, help="primer día (AAAA-MM-DD)")
    parser.add_argument("--hasta", type=date.fromisoformat, help="último día (AAAA-MM-DD)")
    parser.add_argument("--config", help="YAML de configuración (por defecto, el del pipeline)")
    args = parser.parse_args(argv)

    config = load_config(args.config)
    sources = load_sources(config)
    first, last = source_day_range(sources)
    desde, hasta = args.desde or first, args.hasta or last

    start = time.perf_counter()
    totals = dict.fromkeys(EVENT_TABLES, 0)
    days = 0
    for dia, results in replay(sources, config, desde, hasta):
        days += 1
        for r in results:
            if r.table in totals:
                totals[r.table] += r.rows
        print(_day_line(dia, results))

    summary = " ".join(f"{t}={n}" for t, n in totals.items())
    print(f"\n{days} días ({desde} → {hasta}) en {time.perf_counter() - start:.1f} s")
    print(f"Filas escritas: {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
