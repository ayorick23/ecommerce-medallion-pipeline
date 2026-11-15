"""Olist en miniatura para Gold, con los casos que el modelado tiene que resolver (ADR 0036).

El fixture de Silver (``tests/integration/conftest.py``) no tiene ningún caso
que ejercite Gold, y no se toca: sus resultados esperados son de la Fase 3.
Este reusa ``write_sources`` con otras fuentes. Cada caso apunta a una ADR:

- ``u1`` compra en SP (``g1``), en RJ (``g4``) y vuelve a SP (``g5``): tres
  versiones de ``dim_cliente``, y volver a una dirección vieja es versión
  nueva (ADR 0030).
- ``g2``: entrega tardía (ADR 0033).
- ``g3``: cancelado después de entregado; gana la cancelación (ADR 0031).
- ``g4`` ``invoiced``, ``g5`` ``processing`` y ``g6`` ``unavailable`` (ADR 0031).
- ``g7``: despachado antes de aprobarse; el SCD2 ajusta ``valid_from``, que
  es la marca de agua de ``fct_pedidos`` (ADR 0019, 0029).
- ``g8``: cancelado antes de aprobarse.
- ``rg2``: una review de dos pedidos de ``u1``; el segundo llega un día después
  de la review y agrega su vínculo en el puente (ADR 0034).
- ``rg3``: review creada 5 días antes de la compra de su pedido (y la primera
  review del rango: Silver exige una partición por tabla de eventos); espera en
  pendientes y entra con su pedido (ADR 0020).
- Las respuestas de ``rg1`` y ``rg4`` llegan días después de la review, así
  que esas reviews cambian en una corrida posterior (ADR 0018, 0029).
"""

import shutil
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from medallion.bronze.ingest import load_sources
from medallion.bronze.replay import replay
from medallion.common.config import PipelineConfig
from medallion.gold.build import gold_build
from medallion.gold.dbt import warehouse_path
from medallion.silver.build import silver_build
from tests.integration.conftest import GRACE_DAYS, SILVER_CSV, write_sources

_ORDERS_HEADER = (
    "order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,"
    "order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date\n"
)
_REVIEWS_HEADER = (
    "review_id,order_id,review_score,review_comment_title,review_comment_message,"
    "review_creation_date,review_answer_timestamp\n"
)

GOLD_CSV = {
    **SILVER_CSV,
    "orders": _ORDERS_HEADER
    + "g1,c1,delivered,2017-04-03 10:00:00,2017-04-03 11:00:00,2017-04-04 09:00:00,"
    "2017-04-06 15:00:00,2017-04-20 00:00:00\n"
    "g2,c2,delivered,2017-04-03 14:00:00,2017-04-03 15:00:00,2017-04-05 10:00:00,"
    "2017-04-12 12:00:00,2017-04-10 00:00:00\n"
    "g3,c3,canceled,2017-04-04 09:00:00,2017-04-04 10:00:00,2017-04-05 08:00:00,"
    "2017-04-07 16:00:00,2017-04-15 00:00:00\n"
    "g4,c4,invoiced,2017-04-05 08:00:00,2017-04-05 09:00:00,,,2017-04-25 00:00:00\n"
    "g5,c5,processing,2017-04-07 11:00:00,2017-04-07 12:00:00,,,2017-04-27 00:00:00\n"
    "g6,c6,unavailable,2017-04-06 10:00:00,2017-04-06 10:30:00,,,2017-04-26 00:00:00\n"
    "g7,c7,delivered,2017-04-08 08:00:00,2017-04-09 12:00:00,2017-04-09 09:00:00,"
    "2017-04-11 10:00:00,2017-04-22 00:00:00\n"
    "g8,c8,canceled,2017-04-08 15:00:00,,,,2017-04-28 00:00:00\n",
    "order_items": (
        "order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value\n"
        "g1,1,p1,s1,2017-04-05 00:00:00,10.50,5.00\n"
        "g1,2,p2,s1,2017-04-05 00:00:00,3.00,1.00\n"
        "g2,1,p1,s1,2017-04-06 00:00:00,99.90,10.00\n"
        "g3,1,p2,s1,2017-04-06 00:00:00,20.00,4.50\n"
        "g4,1,p1,s1,2017-04-08 00:00:00,15.00,2.00\n"
        "g5,1,p2,s1,2017-04-10 00:00:00,7.25,0.00\n"
        "g6,1,p1,s1,2017-04-09 00:00:00,50.00,8.00\n"
        "g7,1,p1,s1,2017-04-10 00:00:00,12.00,3.00\n"
        "g7,2,p1,s1,2017-04-10 00:00:00,12.00,3.00\n"
    ),
    "order_payments": (
        "order_id,payment_sequential,payment_type,payment_installments,payment_value\n"
        "g1,1,credit_card,2,19.50\n"
        "g2,1,boleto,1,109.90\n"
        "g3,1,credit_card,1,24.50\n"
        "g4,1,voucher,1,10.00\n"
        "g4,2,credit_card,3,7.00\n"
        "g5,1,debit_card,1,7.25\n"
        "g6,1,boleto,1,58.00\n"
        "g7,1,credit_card,4,30.00\n"
        "g8,1,credit_card,1,0.00\n"
    ),
    "order_reviews": _REVIEWS_HEADER
    + "rg1,g1,5,,muito bom,2017-04-07 00:00:00,2017-04-09 10:00:00\n"
    "rg2,g4,4,,,2017-04-06 00:00:00,2017-04-06 18:00:00\n"
    "rg2,g5,4,,,2017-04-06 00:00:00,2017-04-06 18:00:00\n"
    "rg3,g7,3,entrega,demorou,2017-04-03 00:00:00,2017-04-03 12:00:00\n"
    "rg4,g2,1,,,2017-04-13 00:00:00,2017-04-14 09:00:00\n",
    "customers": (
        "customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\n"
        "c1,u1,01151,sao paulo,SP\n"
        "c4,u1,22041,rio de janeiro,RJ\n"
        "c5,u1,01151,sao paulo,SP\n"
        "c2,u2,77410,gurupi,TO\n"
        "c3,u3,01151,sao paulo,SP\n"
        "c6,u4,22041,rio de janeiro,RJ\n"
        "c7,u5,77410,gurupi,TO\n"
        "c8,u6,01151,sao paulo,SP\n"
    ),
    "geolocation": (
        "geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,"
        "geolocation_state\n"
        "01151,-23.50,-46.60,sao paulo,SP\n"
        "22041,-22.97,-43.19,rio de janeiro,RJ\n"
        "77410,-11.70,-49.00,gurupi,TO\n"
    ),
}

# Primer evento: compra de g1 y g2 (03/04). Último: respuesta de rg4 (14/04).
FIRST_DAY, LAST_DAY = date(2017, 4, 3), date(2017, 4, 14)
DAYS = [FIRST_DAY + timedelta(days=n) for n in range((LAST_DAY - FIRST_DAY).days + 1)]

_CLOCK = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)


@dataclass(frozen=True)
class GoldEnv:
    """Una raíz de almacenamiento con las fuentes de Gold en ``raw/``."""

    root: Path
    config: PipelineConfig

    @property
    def warehouse(self) -> Path:
        return warehouse_path(self.config)

    def silver_file(self, table: str) -> Path:
        return self.root / "silver" / table / "part-0.parquet"

    def ingest(self, dia: date) -> None:
        """``bronze-replay`` del día ``dia``."""
        for _ in replay(load_sources(self.config), self.config, dia, dia, clock=lambda: _CLOCK):
            pass

    def silver(self, dia: date) -> None:
        silver_build(dia, self.config, _CLOCK)

    def gold(self, dia: date, full_refresh: bool = False) -> dict[str, object]:
        return gold_build(dia, self.config, _CLOCK, full_refresh=full_refresh)

    def advance(self, dia: date, full_refresh: bool = False) -> None:
        """Los tres comandos del pipeline para el día ``dia``, en orden."""
        self.ingest(dia)
        self.silver(dia)
        self.gold(dia, full_refresh=full_refresh)


def make_gold_env(root: Path) -> GoldEnv:
    root.mkdir(parents=True, exist_ok=True)
    write_sources(root, GOLD_CSV)
    config = PipelineConfig(
        storage_root=str(root),
        sources={table: f"{table}.csv" for table in GOLD_CSV},
        early_arriving_grace_days=GRACE_DAYS,
    )
    return GoldEnv(root, config)


@dataclass
class Walk:
    """Gold recorrido día a día: una copia por día del incremental y del full refresh."""

    env: GoldEnv
    incremental: dict[date, Path] = field(default_factory=dict)
    full_refresh: dict[date, Path] = field(default_factory=dict)


@pytest.fixture(scope="module")
def walk(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Walk]:
    """Recorre ``DAYS`` con incremental y, cada día, construye también un full refresh.

    El full refresh se construye en otra raíz con una copia del Silver del día,
    así el incremental sigue su camino sin interrupciones. Es caro (dos
    ``dbt build`` por día), por eso es uno por módulo y solo para tests que
    leen las copias.
    """
    base = tmp_path_factory.mktemp("gold-recorrido")
    env = make_gold_env(base / "incremental")
    full = make_gold_env(base / "full")
    snapshots = base / "copias"
    snapshots.mkdir()
    result = Walk(env)
    for dia in DAYS:
        env.advance(dia, full_refresh=dia == FIRST_DAY)
        result.incremental[dia] = _copy(env.warehouse, snapshots / f"inc-{dia}.duckdb")

        shutil.rmtree(full.root / "silver", ignore_errors=True)
        shutil.copytree(env.root / "silver", full.root / "silver")
        full.gold(dia, full_refresh=True)
        result.full_refresh[dia] = _copy(full.warehouse, snapshots / f"full-{dia}.duckdb")
    yield result


def _copy(source: Path, target: Path) -> Path:
    shutil.copyfile(source, target)
    return target


@pytest.fixture
def gold_env(tmp_path: Path) -> GoldEnv:
    """Una raíz propia del test, con Gold construido al último día: se puede corromper."""
    env = make_gold_env(tmp_path)
    for dia in DAYS:
        env.ingest(dia)
    env.silver(LAST_DAY)
    env.gold(LAST_DAY, full_refresh=True)
    return env
