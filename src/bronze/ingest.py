"""Ingesta de un ``dia_simulado`` a Bronze (ADR 0004, 0012-0015).

``ingest_day`` recibe las fuentes ya cargadas: el replay completo las lee una
sola vez con ``load_sources``, y una tarea de Airflow, que corre en su propio
proceso, las carga antes de cada día.

Cada tabla se escribe de forma atómica, pero el día completo no: si una tabla
falla, las anteriores de ese día ya quedaron escritas. Reprocesar el día es
idempotente (ADR 0012) y lo deja completo.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

import polars as pl

from bronze.hashing import batch_hash
from bronze.lineage import add_lineage
from bronze.routing import order_children_for_day, orders_for_day, reviews_for_day
from bronze.storage import read_source, snapshot_batch_hash, write_partition, write_snapshot
from bronze.tables import EVENT_TABLES, ORDER_CHILD_TABLES, REFERENCE_TABLES
from common.config import PipelineConfig

Action = Literal["written", "empty", "unchanged"]


@dataclass(frozen=True)
class TableResult:
    """Qué pasó con una tabla en la ingesta de un día.

    ``written``: partición o snapshot escrito. ``empty``: el día no tiene filas
    para la tabla (si existía una partición, se borró). ``unchanged``: el
    snapshot ya tenía el mismo ``batch_hash`` y no se reescribió (ADR 0013).
    """

    table: str
    action: Action
    rows: int


def load_sources(config: PipelineConfig) -> dict[str, pl.DataFrame]:
    """Lee los CSV fuente de todas las tablas de Bronze, como texto."""
    return {
        table: read_source(config.source_uri(table)) for table in (*EVENT_TABLES, *REFERENCE_TABLES)
    }


def _event_batch(table: str, sources: Mapping[str, pl.DataFrame], dia: date) -> pl.DataFrame:
    if table == "orders":
        return orders_for_day(sources["orders"], dia)
    if table == "order_reviews":
        return reviews_for_day(sources["order_reviews"], dia)
    if table in ORDER_CHILD_TABLES:
        return order_children_for_day(sources[table], sources["orders"], dia, table=table)
    raise ValueError(f"Tabla de eventos sin regla de ruteo: {table!r}")


def ingest_day(
    dia: date,
    sources: Mapping[str, pl.DataFrame],
    config: PipelineConfig,
    ingested_at: datetime,
) -> list[TableResult]:
    """Escribe en Bronze las particiones de ``dia`` y actualiza los snapshots."""
    bronze_uri = config.layer_uri("bronze")
    results: list[TableResult] = []

    for table in EVENT_TABLES:
        batch = _event_batch(table, sources, dia)
        with_lineage = add_lineage(
            batch,
            dia=dia,
            ingested_at=ingested_at,
            source_file=config.sources[table],
            batch_hash=batch_hash(batch),
        )
        write_partition(with_lineage, bronze_uri, table, dia)
        results.append(TableResult(table, "written" if batch.height else "empty", batch.height))

    for table in REFERENCE_TABLES:
        snapshot = sources[table]
        content_hash = batch_hash(snapshot)
        if snapshot_batch_hash(bronze_uri, table) == content_hash:
            results.append(TableResult(table, "unchanged", snapshot.height))
            continue
        with_lineage = add_lineage(
            snapshot,
            dia=dia,
            ingested_at=ingested_at,
            source_file=config.sources[table],
            batch_hash=content_hash,
        )
        write_snapshot(with_lineage, bronze_uri, table)
        results.append(TableResult(table, "written", snapshot.height))

    return results
