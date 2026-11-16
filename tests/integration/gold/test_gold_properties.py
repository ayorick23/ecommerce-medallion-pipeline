"""Propiedades del criterio de "hecho" de la Fase 4, con dbt real (plan-fases.md, ADR 0036).

Cada test recorre los días del fixture de Gold o rompe a propósito su Silver:
las propiedades se prueban, no se asumen.
"""

import os
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pytest

from medallion.gold.build import BUILD_DIR, GoldBuildError, read_gold_build
from medallion.gold.compare import compare_warehouses
from tests.integration.gold.conftest import DAYS, FIRST_DAY, LAST_DAY, GoldEnv, Walk

# Fechas que son promesas, no eventos: pueden ser posteriores a D (criterio 6).
PROMESAS = {("fct_pedidos", "fecha_entrega_estimada"), ("fct_pedidos", "fecha_limite_envio")}
# Tablas sin fechas de evento: el calendario fijo y los metadatos de la corrida.
SIN_EVENTOS = {"dim_tiempo", "_gold_build"}


def _query(db: Path, sql: str) -> list[tuple[Any, ...]]:
    with duckdb.connect(str(db), read_only=True) as con:
        return con.execute(sql).fetchall()


# --- Construcción, equivalencia y tiempo (criterios 1, 5 y 6) -------------------


def test_every_day_publishes_gold_at_that_day(walk: Walk) -> None:
    for dia in DAYS:
        info = read_gold_build(walk.incremental[dia])
        assert info is not None and info["as_of"] == dia
        assert info["full_refresh"] is (dia == FIRST_DAY)


def test_incremental_equals_full_refresh_every_day(walk: Walk) -> None:
    for dia in DAYS:
        diferencias = compare_warehouses(walk.incremental[dia], walk.full_refresh[dia])
        assert diferencias == [], f"Gold({dia}): {diferencias}"


def test_cliente_sk_is_stable_across_days(walk: Walk) -> None:
    # Una versión, una vez publicada, conserva su clave y su inicio (ADR 0030).
    sql = "select cliente_sk, customer_unique_id, valid_from from dim_cliente"
    final = set(_query(walk.incremental[LAST_DAY], sql))
    for dia in DAYS:
        assert set(_query(walk.incremental[dia], sql)) <= final, f"Gold({dia})"


def _event_columns(db: Path) -> list[tuple[str, str]]:
    rows = _query(
        db,
        "select table_name, column_name from information_schema.columns "
        "where table_schema = 'main' and data_type in ('DATE', 'TIMESTAMP')",
    )
    return [
        (str(table), str(column))
        for table, column in rows
        if table not in SIN_EVENTOS and (table, column) not in PROMESAS
    ]


def test_no_event_date_after_d(walk: Walk) -> None:
    for dia in DAYS:
        db = walk.incremental[dia]
        columnas = _event_columns(db)
        assert ("fct_pedidos", "fecha_entrega") in columnas  # no pasa por falta de columnas
        for table, column in columnas:
            ((maximo,),) = _query(db, f"select max(cast({column} as date)) from {table}")
            assert maximo is None or maximo <= dia, f"Gold({dia}): {table}.{column} = {maximo}"


# --- Los casos del fixture llegan a Gold (si no, lo anterior probaría poco) -----


def test_fixture_cases_reach_gold(walk: Walk) -> None:
    db = walk.incremental[LAST_DAY]
    estados = {
        str(order_id): str(estado)
        for order_id, estado in _query(db, "select distinct order_id, estado from fct_pedidos")
    }
    assert estados == {
        "g1": "entregado",
        "g2": "entregado",
        "g3": "cancelado",  # cancelado después de entregado
        "g4": "aprobado",  # invoiced
        "g5": "aprobado",  # processing
        "g6": "no_disponible",
        "g7": "entregado",
        "g9": "entregado",
    }
    assert _query(db, "select count(*) from dim_cliente where customer_unique_id = 'u1'") == [(3,)]
    assert _query(db, "select order_id from fct_pedidos where es_entrega_tardia") == [("g2",)]
    # Entregado a las 23:00 del día prometido: no es tarde (ADR 0039).
    assert _query(
        db, "select dias_retraso, es_entrega_tardia from fct_pedidos where order_id = 'g9'"
    ) == [(0, False)]
    assert _query(db, "select count(*) from fct_reviews") == [(4,)]
    assert _query(db, "select count(*) from puente_review_pedido") == [(5,)]


def test_early_review_waits_for_its_order(walk: Walk) -> None:
    sql = "select count(*) from fct_reviews where review_id = 'rg3'"
    assert _query(walk.incremental[date(2017, 4, 7)], sql) == [(0,)]
    assert _query(walk.incremental[date(2017, 4, 8)], sql) == [(1,)]


# --- Idempotencia, atomicidad y fail-fast (criterios 3, 4 y 8) ------------------


def test_rerunning_same_day_gives_same_content(gold_env: GoldEnv, tmp_path: Path) -> None:
    before = tmp_path / "antes.duckdb"
    before.write_bytes(gold_env.warehouse.read_bytes())

    gold_env.gold(LAST_DAY)

    assert compare_warehouses(before, gold_env.warehouse) == []


def _rewrite(env: GoldEnv, table: str, change: Callable[[pl.DataFrame], pl.DataFrame]) -> None:
    path = env.silver_file(table)
    change(pl.read_parquet(path)).write_parquet(path)


CORRUPCIONES: dict[str, tuple[str, Callable[[pl.DataFrame], pl.DataFrame]]] = {
    # Línea de pedido con un producto que no existe: relationships contra dim_producto.
    "fk": (
        "order_items",
        lambda df: df.with_columns(
            pl.when(pl.col("order_id") == "g1")
            .then(pl.lit("p_inexistente"))
            .otherwise(pl.col("product_id"))
            .alias("product_id")
        ),
    ),
    # Línea de pedido duplicada: grano único de fct_pedidos.
    "grano": ("order_items", lambda df: pl.concat([df, df.head(1)])),
    # Precio negativo: accepted_range de price.
    "rango": (
        "order_items",
        lambda df: df.with_columns(
            pl.when(pl.col("order_id") == "g2")
            .then(-pl.col("price"))
            .otherwise(pl.col("price"))
            .alias("price")
        ),
    ),
}


@pytest.mark.parametrize("regla", sorted(CORRUPCIONES))
def test_corrupted_silver_fails_and_leaves_gold_intact(
    gold_env: GoldEnv, tmp_path: Path, regla: str
) -> None:
    before = tmp_path / "antes.duckdb"
    before.write_bytes(gold_env.warehouse.read_bytes())
    info_before = read_gold_build(gold_env.warehouse)
    table, change = CORRUPCIONES[regla]
    _rewrite(gold_env, table, change)

    # Full refresh: un incremental no relee las líneas ya cargadas, así que no
    # vería la corrupción de un pedido viejo.
    with pytest.raises(GoldBuildError, match="dbt build falló"):
        gold_env.gold(LAST_DAY, full_refresh=True)

    assert compare_warehouses(before, gold_env.warehouse) == []
    assert read_gold_build(gold_env.warehouse) == info_before
    staging = gold_env.root / "gold" / "_staging" / BUILD_DIR
    assert not staging.exists() or not os.listdir(staging)
