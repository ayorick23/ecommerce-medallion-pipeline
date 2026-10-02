"""Propiedades de Gold del criterio de "hecho" de la Fase 4 (docs/decisions/plan-fases.md).

Se prueban recorriendo cada día del fixture de Gold con los comandos reales:
construcción (1), atomicidad (3), idempotencia (4), equivalencia incremental ==
full refresh (5), sin datos del futuro (6) y fail-fast con Silver corrompido (8).
"""

import json
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from medallion.gold.build import BUILD_DIR, GoldBuildError, gold_build, read_gold_build
from medallion.gold.dbt import DBT_PROJECT_DIR
from medallion.silver.build import table_path
from tests.integration.gold.conftest import (
    BUILT_AT,
    DAYS,
    GoldEnv,
    GoldWalk,
    fetch,
    read_tables,
)

# Fechas que son promesas conocidas desde la compra: pueden ser futuras (ADR 0018).
PROMISES = {"fecha_entrega_estimada", "fecha_limite_envio"}
# Calendarios: listan fechas, no eventos.
CALENDARS = {"dim_tiempo", "feriados_brasil"}


# --- 1 y 5. Construcción y equivalencia ----------------------------------------------


@pytest.mark.parametrize("dia", DAYS, ids=str)
def test_incremental_day_by_day_equals_full_refresh(gold_walk: GoldWalk, dia: date) -> None:
    incremental = read_tables(gold_walk.incremental[dia])
    full_refresh = read_tables(gold_walk.full_refresh[dia])

    assert incremental.keys() == full_refresh.keys()
    for table, (columns, rows) in incremental.items():
        assert (columns, rows) == full_refresh[table], table


def test_every_day_is_published_with_its_date(gold_walk: GoldWalk) -> None:
    for dia, warehouse in gold_walk.incremental.items():
        info = read_gold_build(warehouse)
        assert info is not None
        assert info["as_of"] == dia
        # El primer día no hay Gold previo, pero se pidió incremental: el flag
        # registra lo que se pidió.
        assert info["full_refresh"] is False


# --- 6. Sin datos del futuro ---------------------------------------------------------


@pytest.mark.parametrize("dia", DAYS, ids=str)
def test_no_table_knows_anything_after_the_day(gold_walk: GoldWalk, dia: date) -> None:
    warehouse = gold_walk.incremental[dia]
    columns = fetch(
        warehouse,
        "select c.table_name, c.column_name, c.data_type from information_schema.columns c "
        "join information_schema.tables t using (table_schema, table_name) "
        "where c.table_schema = 'main' and t.table_type = 'BASE TABLE' "
        "and c.data_type in ('DATE', 'TIMESTAMP') and c.table_name not like '\\_%' escape '\\'",
    )
    end_of_day = datetime.combine(dia + timedelta(days=1), datetime.min.time())
    checked = []
    for table, column, data_type in columns:
        if table in CALENDARS or column in PROMISES:
            continue
        # Un TIMESTAMP se compara contra el fin del día D, sin pasar a DATE.
        ((later,),) = fetch(
            warehouse,
            f"select count(*) from main.{table} where {column} "
            + ("> ?" if data_type == "DATE" else ">= ?"),
            [dia if data_type == "DATE" else end_of_day],
        )
        assert later == 0, f"{table}.{column}: {later} filas posteriores a {dia}"
        checked.append(f"{table}.{column}")

    assert "fct_pedidos._visible_desde" in checked  # el filtro no se quedó sin columnas


# --- 4. Idempotencia -----------------------------------------------------------------


def test_running_a_day_twice_gives_the_same_gold(
    gold_env_at: Callable[[date], GoldEnv],
) -> None:
    # El día 8 tiene merges en fct_pedidos (o8 entregado) y en el puente (r2, o10),
    # y append en fct_pagos (o10).
    dia = date(2017, 6, 8)
    env = gold_env_at(dia)
    first = gold_build(dia, env.config, BUILT_AT)
    tables = read_tables(env.warehouse)

    second = gold_build(dia, env.config, BUILT_AT + timedelta(hours=1))

    assert read_tables(env.warehouse) == tables
    assert {**second, "built_at": None} == {**first, "built_at": None}


# --- 3 y 8. Fail-fast con Silver corrompido, y atomicidad ----------------------------


def _drop_product(silver: str) -> None:
    path = table_path(silver, "products")
    pl.read_parquet(path).filter(pl.col("product_id") != "p2").write_parquet(path)


def _duplicate_line(silver: str) -> None:
    path = table_path(silver, "order_items")
    items = pl.read_parquet(path)
    pl.concat([items, items.filter(pl.col("order_id") == "o10")]).write_parquet(path)


def _negative_price(silver: str) -> None:
    path = table_path(silver, "order_items")
    items = pl.read_parquet(path)
    negative = pl.when(pl.col("order_id") == "o10").then(-pl.col("price")).otherwise("price")
    items.with_columns(negative.alias("price")).write_parquet(path)


def _failed_dbt_nodes() -> list[str]:
    results = json.loads((DBT_PROJECT_DIR / "target" / "run_results.json").read_text("utf-8"))
    return [r["unique_id"] for r in results["results"] if r["status"] in ("fail", "error")]


# Se corrompen filas que entran el día 8 (o10): una fila vieja que no avanza
# ninguna marca de agua no se reprocesa en incremental (ADR 0029). La FK no
# depende de eso: dim_producto se reconstruye completa.
@pytest.mark.parametrize(
    ("corrupt", "failing_test"),
    [
        (_drop_product, "relationships_fct_pedidos_product_id"),
        (_duplicate_line, "unique_combination_of_columns_fct_pedidos"),
        (_negative_price, "accepted_range_fct_pedidos_price"),
    ],
    ids=["fk", "grano", "rango"],
)
def test_corrupted_silver_stops_the_build_and_leaves_gold_intact(
    gold_env_at: Callable[[date], GoldEnv],
    corrupt: Callable[[str], None],
    failing_test: str,
) -> None:
    dia = date(2017, 6, 8)
    env = gold_env_at(dia)
    corrupt(env.config.layer_uri("silver"))
    before = env.warehouse.read_bytes()

    with pytest.raises(GoldBuildError):
        gold_build(dia, env.config, datetime(2026, 10, 3, tzinfo=UTC))

    assert any(failing_test in node for node in _failed_dbt_nodes())
    assert env.warehouse.read_bytes() == before
    assert read_gold_build(env.warehouse)["as_of"] == dia - timedelta(days=1)  # type: ignore[index]
    staging = env.root / "gold" / "_staging"
    assert not (staging / BUILD_DIR).exists()


def test_a_failed_build_after_another_in_the_same_process_leaves_gold_intact(
    gold_env_at: Callable[[date], GoldEnv],
) -> None:
    # Con keep_open (el valor por defecto de dbt-duckdb), la segunda corrida
    # reusaría la conexión al temporal ya publicado y un full refresh escribiría
    # directo sobre Gold (dbt/profiles.yml).
    dia = date(2017, 6, 8)
    env = gold_env_at(dia)
    gold_build(dia, env.config, BUILT_AT)
    before = env.warehouse.read_bytes()
    _negative_price(env.config.layer_uri("silver"))

    with pytest.raises(GoldBuildError):
        gold_build(dia, env.config, BUILT_AT, full_refresh=True)

    assert env.warehouse.read_bytes() == before
