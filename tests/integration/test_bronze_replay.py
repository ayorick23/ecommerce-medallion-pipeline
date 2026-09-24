"""Replay de Bronze de punta a punta: CSV en disco -> particiones y snapshots.

Criterio de "hecho" de la Fase 2: ingerir días nuevos hace crecer Bronze sin
tocar lo ya escrito, y reprocesar días ya ingeridos da el mismo resultado, sin
duplicar ni corromper (ADR 0012), comparando todo menos ``ingested_at``.
"""

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from medallion.bronze.ingest import ingest_day, load_sources
from medallion.bronze.replay import main
from medallion.bronze.storage import STAGING_DIR
from medallion.bronze.tables import EVENT_TABLES, REFERENCE_TABLES
from medallion.common.config import PipelineConfig

DAYS = (date(2017, 3, 15), date(2017, 3, 16), date(2017, 3, 17))

CSV_FILES = {
    "orders": (
        "order_id,order_status,order_purchase_timestamp,order_approved_at,"
        "order_delivered_carrier_date,order_delivered_customer_date\n"
        "o1,delivered,2017-03-15 10:00:00,2017-03-15 11:00:00,2017-03-16 08:00:00,"
        "2017-03-17 18:00:00\n"
        "o2,processing,2017-03-16 09:00:00,,,\n"
    ),
    "order_items": "order_id,order_item_id,price\no1,1,10.50\no1,2,3.00\no2,1,99.90\n",
    "order_payments": "order_id,payment_sequential,payment_value\no1,1,13.50\no2,1,99.90\n",
    "order_reviews": (
        "review_id,order_id,review_comment_message,review_creation_date\n"
        'r1,o1,"chegou antes,\nmuito bom",2017-03-17 00:00:00\n'
    ),
    "customers": "customer_id,customer_city\nc1,são paulo\n",
    "products": "product_id,product_category_name\np1,\n",
    "sellers": "seller_id,seller_state\ns1,SP\n",
    "geolocation": "geolocation_zip_code_prefix,geolocation_lat\n01037,-23.54\n",
    "category_translation": (
        "product_category_name,product_category_name_english\nbeleza_saude,health_beauty\n"
    ),
}


def _setup(root: Path) -> PipelineConfig:
    raw = root / "raw"
    raw.mkdir()
    for table, content in CSV_FILES.items():
        (raw / f"{table}.csv").write_bytes(content.encode("utf-8"))
    return PipelineConfig(storage_root=str(root), sources={t: f"{t}.csv" for t in CSV_FILES})


def _bronze_state(bronze: Path) -> dict[str, pl.DataFrame]:
    """Contenido completo de Bronze sin ``ingested_at``, en orden determinista."""
    state = {}
    for table in EVENT_TABLES:
        if (bronze / table).exists():
            df = pl.read_parquet(bronze / table, hive_partitioning=True).drop("ingested_at")
            state[table] = df.sort(df.columns)
    for table in REFERENCE_TABLES:
        df = pl.read_parquet(bronze / table / "snapshot.parquet").drop("ingested_at")
        state[table] = df.sort(df.columns)
    return state


def _files(bronze: Path) -> dict[Path, bytes]:
    return {p: p.read_bytes() for p in bronze.rglob("*.parquet")}


def test_each_new_day_grows_bronze_without_touching_previous_files(tmp_path: Path) -> None:
    config = _setup(tmp_path)
    bronze = tmp_path / "bronze"
    sources = load_sources(config)
    previous: dict[Path, bytes] = {}

    for hour, dia in enumerate(DAYS):
        # Una hora distinta por corrida: reescribir un archivo previo cambiaría sus bytes.
        ingest_day(dia, sources, config, datetime(2026, 9, 24, hour, 0, tzinfo=UTC))
        current = _files(bronze)

        assert {p: current.get(p) for p in previous} == previous, f"{dia} modificó días previos"
        assert any(f"dia_simulado={dia}" in str(p) for p in current.keys() - previous.keys())
        previous = current


def test_replay_of_three_days_and_reprocessing_is_idempotent(tmp_path: Path) -> None:
    config = _setup(tmp_path)
    bronze = tmp_path / "bronze"
    sources = load_sources(config)

    for dia in DAYS:
        ingest_day(dia, sources, config, datetime(2026, 9, 24, 12, 0, tzinfo=UTC))
    first = _bronze_state(bronze)

    for dia in (DAYS[2], DAYS[0], DAYS[1]):
        ingest_day(dia, sources, config, datetime(2026, 9, 25, 8, 0, tzinfo=UTC))
    second = _bronze_state(bronze)

    assert first.keys() == second.keys()
    for table in first:
        assert second[table].equals(first[table]), table
    assert list((bronze / STAGING_DIR).iterdir()) == []


def test_command_line_replays_the_whole_source_range(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("PIPELINE_STORAGE_ROOT", raising=False)
    config = _setup(tmp_path)
    config_file = tmp_path / "pipeline.yaml"
    sources_yaml = "".join(f"  {t}: {f}\n" for t, f in config.sources.items())
    config_file.write_text(
        f"storage:\n  root: {tmp_path.as_posix()}\nsources:\n{sources_yaml}", encoding="utf-8"
    )

    assert main(["--config", str(config_file)]) == 0

    output = capsys.readouterr().out
    assert "3 días (2017-03-15 → 2017-03-17)" in output
    assert "orders=4 order_items=3 order_payments=2 order_reviews=1" in output
    assert set(_bronze_state(tmp_path / "bronze")) == {*EVENT_TABLES, *REFERENCE_TABLES}


def test_replay_writes_the_expected_bronze_layout(tmp_path: Path) -> None:
    config = _setup(tmp_path)
    sources = load_sources(config)
    for dia in DAYS:
        ingest_day(dia, sources, config, datetime(2026, 9, 24, 12, 0, tzinfo=UTC))

    state = _bronze_state(tmp_path / "bronze")

    assert state["orders"].select("order_id", "dia_simulado").sort(pl.all()).rows() == [
        ("o1", DAYS[0]),
        ("o1", DAYS[1]),
        ("o1", DAYS[2]),
        ("o2", DAYS[1]),
    ]
    assert state["order_reviews"]["review_comment_message"].to_list() == [
        "chegou antes,\nmuito bom"
    ]
    assert state["products"]["product_category_name"].to_list() == [None]
    assert all(dtype == pl.String for dtype in state["order_items"].drop("dia_simulado").dtypes)
    assert state["customers"]["dia_simulado"].to_list() == [DAYS[0]]
