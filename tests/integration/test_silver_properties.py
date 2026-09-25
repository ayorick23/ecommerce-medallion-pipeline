"""Propiedades de Silver del criterio de "hecho" de la Fase 3 (docs/decisions/plan-fases.md).

Se prueban recorriendo cada día del rango del dataset en miniatura, no
asumiendo que valen: idempotencia (2), sin datos del futuro (3), historial
SCD2 coherente en el tiempo (4), fail-fast por cada regla (5) y ciclo de vida
de una review adelantada (6).
"""

import dataclasses
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from medallion.silver.build import build_tables, read_manifest, silver_build
from medallion.silver.facts import PENDING_DAYS
from medallion.silver.read import read_bronze
from medallion.silver.validation import SilverValidationError
from tests.integration.conftest import FIRST_DAY, LAST_DAY, SILVER_CSV, SilverEnv, write_sources

# Un día antes de la primera partición y uno después de la última respuesta (r1, 20/03).
DAYS = [FIRST_DAY - timedelta(days=1) + timedelta(days=n) for n in range(11)]
CONSECUTIVE = list(zip(DAYS, DAYS[1:], strict=False))

# Datos conocidos desde la compra que apuntan al futuro: no se enmascaran (ADR 0018).
KNOWN_AT_PURCHASE = {"order_estimated_delivery_date", "shipping_limit_date"}

SCD2_KEY = ["order_id", "status_event"]


def _silver(env: SilverEnv, dia: date) -> tuple[dict[str, pl.DataFrame], pl.DataFrame]:
    raw = read_bronze(env.config.layer_uri("bronze"), dia)
    return build_tables(raw, dia, env.config.early_arriving_grace_days)


def _files(silver: Path) -> dict[str, bytes]:
    return {
        p.relative_to(silver).as_posix(): p.read_bytes()
        for p in silver.rglob("*.parquet")
        if "_staging" not in p.parts
    }


# --- 2. Idempotencia ---------------------------------------------------------------


def test_rebuilding_a_day_gives_identical_files(silver_env: SilverEnv) -> None:
    silver_build(LAST_DAY, silver_env.config, datetime(2026, 9, 25, 10, tzinfo=UTC))
    first = _files(silver_env.silver)
    first_manifest = read_manifest(str(silver_env.silver))

    # Otros días en el medio: Silver no depende de ninguna corrida previa (ADR 0017).
    for dia in (DAYS[3], DAYS[0], LAST_DAY):
        silver_build(dia, silver_env.config, datetime(2026, 9, 26, 10, tzinfo=UTC))

    assert _files(silver_env.silver) == first
    manifest = read_manifest(str(silver_env.silver))
    assert first_manifest is not None and manifest is not None
    assert {**manifest, "built_at": None} == {**first_manifest, "built_at": None}


# --- 3. Sin datos del futuro -------------------------------------------------------


@pytest.mark.parametrize("dia", DAYS, ids=str)
def test_no_table_knows_anything_after_the_day(shared_bronze: SilverEnv, dia: date) -> None:
    tables, pending = _silver(shared_bronze, dia)

    for table, df in {**tables, "_pendientes": pending}.items():
        for column, dtype in df.schema.items():
            if dtype == pl.Datetime and column not in KNOWN_AT_PURCHASE:
                late = df.filter(pl.col(column).dt.date() > dia)
                assert late.height == 0, f"{table}.{column} tiene valores posteriores a {dia}"


def test_masked_timestamps_appear_on_their_day(shared_bronze: SilverEnv) -> None:
    def delivered(dia: date) -> list[object]:
        orders = _silver(shared_bronze, dia)[0]["orders"]
        return orders.filter(pl.col("order_id") == "o1")["order_delivered_customer_date"].to_list()

    def answered(dia: date) -> list[object]:
        reviews = _silver(shared_bronze, dia)[0]["order_reviews"]
        return reviews.filter(pl.col("review_id") == "r1")["review_answer_timestamp"].to_list()

    assert delivered(date(2017, 3, 17)) == [None]
    assert delivered(date(2017, 3, 18)) == [datetime(2017, 3, 18, 18)]
    assert answered(date(2017, 3, 19)) == [None]
    assert answered(date(2017, 3, 20)) == [datetime(2017, 3, 20, 10)]


# --- 4. Coherencia del SCD2 en el tiempo --------------------------------------------


@pytest.mark.parametrize(("d1", "d2"), CONSECUTIVE, ids=lambda d: str(d))
def test_history_of_a_day_is_the_beginning_of_the_next_one(
    shared_bronze: SilverEnv, d1: date, d2: date
) -> None:
    h1 = _silver(shared_bronze, d1)[0]["order_status_history"]
    h2 = _silver(shared_bronze, d2)[0]["order_status_history"]

    joined = h1.join(h2, on=SCD2_KEY, how="left", suffix="_2")

    assert joined["valid_from_2"].null_count() == 0, "un evento conocido desapareció"
    for column in ("order_status_raw", "event_timestamp", "valid_from", "is_adjusted"):
        assert joined[column].equals(joined[f"{column}_2"]), column
    closed = joined.filter(~pl.col("is_current"))
    assert closed["valid_to"].equals(closed["valid_to_2"]), "se reabrió un intervalo cerrado"
    reopened = joined.filter(pl.col("is_current") & (pl.col("valid_to_2").dt.date() <= d1))
    assert reopened.height == 0, "un intervalo vigente se cerró con un evento ya conocido"


def test_the_order_shipped_before_approval_is_adjusted_once_approved(
    shared_bronze: SilverEnv,
) -> None:
    history = _silver(shared_bronze, date(2017, 3, 15))[0]["order_status_history"]

    o3 = history.filter(pl.col("order_id") == "o3")
    assert o3.select("status_event", "valid_from", "is_adjusted", "is_current").rows() == [
        ("creado", datetime(2017, 3, 14, 8), False, False),
        ("aprobado", datetime(2017, 3, 15, 12), False, False),
        ("despachado", datetime(2017, 3, 15, 12), True, True),
    ]


# --- 5. Fail-fast por regla ---------------------------------------------------------


def _replace(table: str, old: str, new: str) -> Callable[[], dict[str, str]]:
    def override() -> dict[str, str]:
        assert old in SILVER_CSV[table], f"{old!r} no está en {table}"
        return {table: SILVER_CSV[table].replace(old, new)}

    return override


FAIL_FAST_CASES = {
    "tipo-no-parseable": (
        _replace("customers", "c2,u2,77410", "c2,u2,7741"),
        [("customers", "tipo_no_parseable"), ("customers", "nulo_no_permitido")],
    ),
    "columna-faltante": (
        _replace(
            "sellers", "seller_city,seller_state\ns1,01151,sao paulo,", "seller_state\ns1,01151,"
        ),
        # La columna vuelve nula, así que la regla de nulos también la reporta.
        [("sellers", "columna_faltante"), ("sellers", "nulo_no_permitido")],
    ),
    "nulo": (
        _replace("order_payments", "o1,1,credit_card", "o1,1,"),
        [("order_payments", "nulo_no_permitido")],
    ),
    "pk-duplicada": (
        _replace("order_payments", "o1,1,credit_card,2,19.50\n", "o1,1,credit_card,2,19.50\n" * 2),
        [("order_payments", "pk_duplicada")],
    ),
    "fk-huerfana": (
        _replace("order_items", "o1,2,p2", "o1,2,p9"),
        [("order_items", "fk_huerfana")],
    ),
    "medida-imposible": (
        _replace("order_reviews", "r1,o1,5", "r1,o1,6"),
        [("order_reviews", "medida_imposible")],
    ),
}


@pytest.mark.parametrize(("override", "expected"), FAIL_FAST_CASES.values(), ids=FAIL_FAST_CASES)
def test_each_rule_stops_the_build_without_writing(
    silver_env: SilverEnv,
    override: Callable[[], dict[str, str]],
    expected: list[tuple[str, str]],
) -> None:
    write_sources(silver_env.root, override())
    silver_env.ingest()

    with pytest.raises(SilverValidationError) as error:
        silver_build(LAST_DAY, silver_env.config, datetime(2026, 9, 25, tzinfo=UTC))

    assert [(f.table, f.rule) for f in error.value.failures] == expected
    assert not silver_env.silver.exists()


# --- 6. Review adelantada: pendiente, luego en Silver; o vencida ---------------------


def test_an_early_review_waits_for_its_order_and_then_enters_silver(
    shared_bronze: SilverEnv,
) -> None:
    for dia in (date(2017, 3, n) for n in range(12, 17)):
        tables, pending = _silver(shared_bronze, dia)
        assert pending.select("review_id", PENDING_DAYS).rows() == [
            ("r2", (dia - date(2017, 3, 12)).days)
        ], dia
        assert "r2" not in tables["order_reviews"]["review_id"].to_list()

    tables, pending = _silver(shared_bronze, date(2017, 3, 17))  # llega la compra de o2
    assert pending.height == 0
    assert "r2" in tables["order_reviews"]["review_id"].to_list()


def test_an_early_review_past_the_grace_period_stops_the_build(silver_env: SilverEnv) -> None:
    config = dataclasses.replace(silver_env.config, early_arriving_grace_days=2)

    silver_build(date(2017, 3, 14), config, datetime(2026, 9, 25, tzinfo=UTC))  # 2 días: espera
    with pytest.raises(SilverValidationError) as error:
        silver_build(date(2017, 3, 15), config, datetime(2026, 9, 25, tzinfo=UTC))  # 3 días

    [failure] = error.value.failures
    assert (failure.table, failure.rule, failure.examples) == (
        "order_reviews",
        "fk_huerfana",
        ("'o2'",),
    )
