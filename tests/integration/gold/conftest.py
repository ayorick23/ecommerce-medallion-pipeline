"""Olist en miniatura para Gold: 9 días con los casos que las ADRs 0030–0037 tienen que resolver.

El fixture de Silver (``tests/integration/conftest.py``) no cubre ningún cliente
con dos direcciones, cancelación después de una entrega ni review con varios
pedidos (ADR 0036). Este reusa sus productos, vendedores y categorías y
reemplaza el resto con ``write_sources(root, overrides)``.

Día a día (junio de 2017; ``u*`` son personas, ``customer_unique_id``):

1. ``o1`` (u1, dirección A, 2 líneas, 2 pagos). **Ninguna review**: los hechos
   de reviews arrancan vacíos.
2. ``o2`` (u2) se compra y se aprueba; ``o3`` (u3, ``invoiced``); ``o8`` (u7),
   con entrega estimada el día 6.
3. ``o4`` (u1, **dirección B**, otro estado); ``o5`` (u3, ``processing``, misma
   dirección que ``o3``); se crea ``r3``, review **adelantada** de ``o11``;
   se despachan ``o1`` y ``o2``.
4. ``o7`` (u4, ``unavailable``); ``o1`` se entrega antes de lo estimado;
   se despacha ``o8``.
5. Se crea ``r1`` (de ``o1``): vigente la versión B de u1. ``o2`` se entrega,
   pero su ``order_status`` es ``canceled`` (**cancelado después de entregado**).
6. ``o6`` (u1 **vuelve a A**: tercera versión); ``o9`` (u5).
7. ``r1`` recibe su respuesta; se crea ``r2``, ligada a ``o9`` y ``o10``
   (``o10`` todavía no existe); ``o4`` se entrega.
8. ``o10`` (u5): entra el vínculo ``(r2, o10)``; ``o8`` se entrega **2 días
   tarde**.
9. ``o11`` (u6, cancelado antes de aprobarse): entra ``r3``; ``o9`` se entrega
   a las 18:00 del **día prometido**, que no es tarde (ADR 0037).

La respuesta de ``r2`` es del día 10, fuera del rango: nunca es visible.
"""

import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pytest

from medallion.bronze.ingest import load_sources
from medallion.bronze.replay import replay, source_day_range
from medallion.common.config import PipelineConfig
from medallion.gold.build import BUILD_TABLE, gold_build
from medallion.gold.dbt import WAREHOUSE_FILE
from medallion.silver.build import silver_build
from tests.integration.conftest import GRACE_DAYS, SILVER_CSV, write_sources

FIRST_DAY, LAST_DAY = date(2017, 6, 1), date(2017, 6, 9)
DAYS = [FIRST_DAY + timedelta(days=n) for n in range((LAST_DAY - FIRST_DAY).days + 1)]

BUILT_AT = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)

GOLD_CSV = {
    "orders": (
        "order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,"
        "order_delivered_carrier_date,order_delivered_customer_date,"
        "order_estimated_delivery_date\n"
        "o1,c1,delivered,2017-06-01 10:00:00,2017-06-01 11:00:00,2017-06-03 09:00:00,"
        "2017-06-04 15:00:00,2017-06-10 00:00:00\n"
        "o2,c2,canceled,2017-06-02 09:00:00,2017-06-02 09:30:00,2017-06-03 10:00:00,"
        "2017-06-05 12:00:00,2017-06-12 00:00:00\n"
        "o3,c3,invoiced,2017-06-02 14:00:00,2017-06-02 15:00:00,,,2017-06-15 00:00:00\n"
        "o8,c8,delivered,2017-06-02 16:00:00,2017-06-02 17:00:00,2017-06-04 08:00:00,"
        "2017-06-08 11:00:00,2017-06-06 00:00:00\n"
        "o4,c4,delivered,2017-06-03 10:00:00,2017-06-03 10:30:00,2017-06-05 09:00:00,"
        "2017-06-07 14:00:00,2017-06-20 00:00:00\n"
        "o5,c5,processing,2017-06-03 12:00:00,2017-06-03 13:00:00,,,2017-06-18 00:00:00\n"
        "o7,c7,unavailable,2017-06-04 09:00:00,2017-06-04 10:00:00,,,2017-06-20 00:00:00\n"
        "o6,c6,shipped,2017-06-06 08:00:00,2017-06-06 09:00:00,2017-06-08 10:00:00,,"
        "2017-06-25 00:00:00\n"
        "o9,c9,delivered,2017-06-06 11:00:00,2017-06-06 12:00:00,2017-06-07 09:00:00,"
        "2017-06-09 18:00:00,2017-06-09 00:00:00\n"
        "o10,c10,approved,2017-06-08 10:00:00,2017-06-08 11:00:00,,,2017-06-22 00:00:00\n"
        "o11,c11,canceled,2017-06-09 13:00:00,,,,2017-06-23 00:00:00\n"
    ),
    "order_items": (
        "order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value\n"
        "o1,1,p1,s1,2017-06-04 00:00:00,10.50,5.00\n"
        "o1,2,p2,s1,2017-06-04 00:00:00,3.00,1.00\n"
        "o2,1,p1,s1,2017-06-05 00:00:00,99.90,10.00\n"
        "o3,1,p2,s1,2017-06-05 00:00:00,20.00,4.00\n"
        "o8,1,p1,s1,2017-06-05 00:00:00,15.00,3.00\n"
        "o4,1,p1,s1,2017-06-06 00:00:00,30.00,6.00\n"
        "o5,1,p2,s1,2017-06-06 00:00:00,12.00,2.00\n"
        "o7,1,p1,s1,2017-06-07 00:00:00,25.00,5.00\n"
        "o6,1,p2,s1,2017-06-09 00:00:00,8.00,2.00\n"
        "o9,1,p1,s1,2017-06-09 00:00:00,40.00,7.00\n"
        "o10,1,p2,s1,2017-06-11 00:00:00,18.00,3.00\n"
        "o11,1,p1,s1,2017-06-12 00:00:00,22.00,4.00\n"
    ),
    "order_payments": (
        "order_id,payment_sequential,payment_type,payment_installments,payment_value\n"
        "o1,1,voucher,1,5.00\n"
        "o1,2,credit_card,3,14.50\n"
        "o2,1,boleto,1,109.90\n"
        "o3,1,credit_card,1,24.00\n"
        "o8,1,credit_card,2,18.00\n"
        "o4,1,boleto,1,36.00\n"
        "o5,1,credit_card,1,14.00\n"
        "o7,1,credit_card,1,30.00\n"
        "o6,1,debit_card,1,10.00\n"
        "o9,1,credit_card,4,47.00\n"
        "o10,1,credit_card,1,21.00\n"
        "o11,1,boleto,1,26.00\n"
    ),
    "order_reviews": (
        "review_id,order_id,review_score,review_comment_title,review_comment_message,"
        "review_creation_date,review_answer_timestamp\n"
        "r1,o1,5,,chegou antes do prazo,2017-06-05 00:00:00,2017-06-07 10:00:00\n"
        "r2,o9,4,,,2017-06-07 00:00:00,2017-06-10 09:00:00\n"
        "r2,o10,4,,,2017-06-07 00:00:00,2017-06-10 09:00:00\n"
        "r3,o11,1,atraso,,2017-06-03 00:00:00,2017-06-04 08:00:00\n"
    ),
    "customers": (
        "customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\n"
        "c1,u1,01151,sao paulo,SP\n"
        "c4,u1,20040,rio de janeiro,RJ\n"
        "c6,u1,01151,sao paulo,SP\n"
        "c2,u2,77410,gurupi,TO\n"
        "c3,u3,01151,sao paulo,SP\n"
        "c5,u3,01151,sao paulo,SP\n"
        "c7,u4,77410,gurupi,TO\n"
        "c8,u7,20040,rio de janeiro,RJ\n"
        "c9,u5,01151,sao paulo,SP\n"
        "c10,u5,01151,sao paulo,SP\n"
        "c11,u6,77410,gurupi,TO\n"
    ),
    "geolocation": (
        "geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,"
        "geolocation_state\n"
        "01151,-23.50,-46.60,sao paulo,SP\n"
        "20040,-22.90,-43.17,rio de janeiro,RJ\n"
        "77410,-11.70,-49.00,gurupi,TO\n"
    ),
}


def make_config(root: Path) -> PipelineConfig:
    return PipelineConfig(
        storage_root=str(root),
        sources={table: f"{table}.csv" for table in SILVER_CSV},
        early_arriving_grace_days=GRACE_DAYS,
    )


def ingest(root: Path) -> PipelineConfig:
    """Escribe la fuente de Gold en ``root/raw`` y la ingiere completa en Bronze."""
    root.mkdir(parents=True, exist_ok=True)
    write_sources(root, GOLD_CSV)
    config = make_config(root)
    sources = load_sources(config)
    first, last = source_day_range(sources)
    assert (first, last) == (FIRST_DAY, LAST_DAY)
    clock = lambda: datetime(2026, 10, 2, 9, 0, tzinfo=UTC)  # noqa: E731
    for _ in replay(sources, config, first, last, clock=clock):
        pass
    return config


def warehouse(root: Path) -> Path:
    return root / "gold" / WAREHOUSE_FILE


@dataclass
class GoldWalk:
    """Gold recorrido día por día en incremental, y un full refresh de cada día.

    Las copias de cada día quedan en ``incremental[D]`` y ``full_refresh[D]``
    para que los tests las lean sin volver a correr dbt.
    """

    bronze: Path
    incremental: dict[date, Path] = field(default_factory=dict)
    full_refresh: dict[date, Path] = field(default_factory=dict)
    seconds: dict[date, float] = field(default_factory=dict)


def _snapshot(source: Path, target: Path) -> Path:
    # Mismo nombre de archivo: DuckDB nombra el catálogo por el archivo (ADR 0028).
    target.mkdir(parents=True)
    return Path(shutil.copy2(source, target / WAREHOUSE_FILE))


def _copy_silver(source_root: Path, target_root: Path) -> None:
    shutil.rmtree(target_root / "silver", ignore_errors=True)
    shutil.copytree(
        source_root / "silver", target_root / "silver", ignore=shutil.ignore_patterns("_staging")
    )


def fetch(db: Path, sql: str, params: list[object] | None = None) -> list[tuple[Any, ...]]:
    with duckdb.connect(str(db), read_only=True) as con:
        return con.execute(sql, params or []).fetchall()


def read_tables(db: Path) -> dict[str, tuple[list[str], list[tuple[Any, ...]]]]:
    """Columnas y filas (ordenadas) de cada tabla de Gold, sin ``_gold_build``."""
    with duckdb.connect(str(db), read_only=True) as con:
        names = [
            name
            for (name,) in con.execute(
                "select table_name from information_schema.tables "
                "where table_schema = 'main' and table_type = 'BASE TABLE' "
                "and table_name <> ? order by table_name",
                [BUILD_TABLE],
            ).fetchall()
        ]
        tables = {}
        for name in names:
            cursor = con.execute(f"select * from main.{name} order by all")
            tables[name] = ([col[0] for col in cursor.description], cursor.fetchall())
        return tables


@pytest.fixture(scope="package")
def gold_walk(tmp_path_factory: pytest.TempPathFactory) -> GoldWalk:
    """Recorre los 9 días con silver-build y gold-build reales. Solo para tests de lectura."""
    base = tmp_path_factory.mktemp("gold-recorrido")
    inc_root, full_root = base / "incremental", base / "full-refresh"
    inc_config = ingest(inc_root)
    full_config = make_config(full_root)
    walk = GoldWalk(bronze=inc_root / "bronze")

    for dia in DAYS:
        silver_build(dia, inc_config, BUILT_AT)
        start = time.perf_counter()
        gold_build(dia, inc_config, BUILT_AT)
        walk.seconds[dia] = time.perf_counter() - start
        walk.incremental[dia] = _snapshot(warehouse(inc_root), base / "copias" / f"inc-{dia}")

        _copy_silver(inc_root, full_root)
        gold_build(dia, full_config, BUILT_AT, full_refresh=True)
        walk.full_refresh[dia] = _snapshot(warehouse(full_root), base / "copias" / f"full-{dia}")
    return walk


@dataclass(frozen=True)
class GoldEnv:
    """Una raíz propia con Bronze, Silver(D) y el Gold de D − 1 del recorrido."""

    root: Path
    config: PipelineConfig

    @property
    def warehouse(self) -> Path:
        return warehouse(self.root)


@pytest.fixture
def gold_env_at(gold_walk: GoldWalk, tmp_path: Path) -> Callable[[date], GoldEnv]:
    """Raíz propia lista para correr gold-build en D: Silver(D) y el Gold(D − 1) del recorrido.

    Para tests que escriben: no tocan las copias de ``gold_walk``.
    """

    def make(dia: date) -> GoldEnv:
        root = tmp_path / f"gold-{dia}"
        shutil.copytree(gold_walk.bronze, root / "bronze")
        config = make_config(root)
        silver_build(dia, config, BUILT_AT)
        warehouse(root).parent.mkdir(parents=True)
        shutil.copy2(gold_walk.incremental[dia - timedelta(days=1)], warehouse(root))
        return GoldEnv(root, config)

    return make
