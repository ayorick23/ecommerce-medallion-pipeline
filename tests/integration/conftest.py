"""Dataset Olist en miniatura, con todas las columnas del contrato, para los tests de Silver.

Cubre los casos que Silver tiene que resolver: un pedido entregado (``o1``), un
pedido cancelado después de aprobarse (``o2``), un pedido despachado antes de
aprobarse y sin items ni pagos (``o3``, ADR 0019), una review creada 5 días
antes de la compra de su pedido (``r2``, ADR 0020), un producto sin categoría y
una coordenada fuera de Brasil (ADR 0025).
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from medallion.bronze.ingest import load_sources
from medallion.bronze.replay import replay, source_day_range
from medallion.common.config import PipelineConfig

GRACE_DAYS = 120

SILVER_CSV = {
    "orders": (
        "order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,"
        "order_delivered_carrier_date,order_delivered_customer_date,"
        "order_estimated_delivery_date\n"
        "o1,c1,delivered,2017-03-15 10:00:00,2017-03-15 11:00:00,2017-03-16 08:00:00,"
        "2017-03-18 18:00:00,2017-03-30 00:00:00\n"
        "o2,c2,canceled,2017-03-17 09:00:00,2017-03-17 09:30:00,,,2017-03-31 00:00:00\n"
        "o3,c3,delivered,2017-03-14 08:00:00,2017-03-15 12:00:00,2017-03-15 09:00:00,"
        "2017-03-18 10:00:00,2017-03-28 00:00:00\n"
    ),
    "order_items": (
        "order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value\n"
        "o1,1,p1,s1,2017-03-20 00:00:00,10.50,5.00\n"
        "o1,2,p2,s1,2017-03-20 00:00:00,3.00,1.00\n"
        "o2,1,p1,s1,2017-03-21 00:00:00,99.90,10.00\n"
    ),
    "order_payments": (
        "order_id,payment_sequential,payment_type,payment_installments,payment_value\n"
        "o1,1,credit_card,2,19.50\n"
        "o2,1,boleto,1,109.90\n"
    ),
    "order_reviews": (
        "review_id,order_id,review_score,review_comment_title,review_comment_message,"
        "review_creation_date,review_answer_timestamp\n"
        'r1,o1,5,,"chegou antes,\nmuito bom",2017-03-19 00:00:00,2017-03-20 10:00:00\n'
        "r2,o2,1,,,2017-03-12 00:00:00,2017-03-13 08:00:00\n"
    ),
    "customers": (
        "customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\n"
        "c1,u1,01151,sao paulo,SP\n"
        "c2,u2,77410,gurupi,TO\n"
        "c3,u1,01151,sao paulo,SP\n"
    ),
    "products": (
        "product_id,product_category_name,product_name_lenght,product_description_lenght,"
        "product_photos_qty,product_weight_g,product_length_cm,product_height_cm,"
        "product_width_cm\n"
        "p1,perfumaria,40,300,1,500,20,10,15\n"
        "p2,,,,,,,,\n"
    ),
    "sellers": "seller_id,seller_zip_code_prefix,seller_city,seller_state\ns1,01151,sao paulo,SP\n",
    "geolocation": (
        "geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,"
        "geolocation_state\n"
        "01151,-23.50,-46.60,sao paulo,SP\n"
        "77410,-11.70,-49.00,gurupi,TO\n"
        "77410,42.00,-8.00,gurupi,TO\n"
    ),
    "category_translation": (
        "product_category_name,product_category_name_english\nperfumaria,perfumery\n"
    ),
}

# Días con eventos en Bronze: r2 (12/03), o3 (14, 15 y 18/03), o1 (15, 16 y 18/03),
# o2 (17/03), r1 (19/03). La respuesta de r1 (20/03) no es ancla: no crea partición.
FIRST_DAY, LAST_DAY = date(2017, 3, 12), date(2017, 3, 19)
BRONZE_DAYS_WITH_DATA = 7


@dataclass(frozen=True)
class SilverEnv:
    """Raíz de almacenamiento con Bronze ya ingerido y un YAML de configuración."""

    root: Path
    config: PipelineConfig
    config_file: Path
    ingest: Callable[[], None]

    @property
    def silver(self) -> Path:
        return self.root / "silver"


def write_sources(root: Path, overrides: dict[str, str] | None = None) -> None:
    raw = root / "raw"
    raw.mkdir(exist_ok=True)
    for table, content in {**SILVER_CSV, **(overrides or {})}.items():
        (raw / f"{table}.csv").write_bytes(content.encode("utf-8"))


def _make_env(root: Path) -> SilverEnv:
    write_sources(root)
    config = PipelineConfig(
        storage_root=str(root),
        sources={table: f"{table}.csv" for table in SILVER_CSV},
        early_arriving_grace_days=GRACE_DAYS,
    )
    config_file = root / "pipeline.yaml"
    sources_yaml = "".join(f"  {t}: {f}\n" for t, f in config.sources.items())
    config_file.write_text(
        f"storage:\n  root: {root.as_posix()}\nsources:\n{sources_yaml}"
        f"silver:\n  early_arriving_grace_days: {GRACE_DAYS}\n",
        encoding="utf-8",
    )

    def ingest() -> None:
        """Ingiere en Bronze todo el rango de la fuente actual en ``raw/``."""
        sources = load_sources(config)
        first, last = source_day_range(sources)
        clock = lambda: datetime(2026, 9, 25, 12, 0, tzinfo=UTC)  # noqa: E731
        for _ in replay(sources, config, first, last, clock=clock):
            pass

    ingest()
    return SilverEnv(root, config, config_file, ingest)


@pytest.fixture
def silver_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SilverEnv:
    """Bronze ingerido en un directorio propio del test: se puede modificar y escribir."""
    monkeypatch.delenv("PIPELINE_STORAGE_ROOT", raising=False)
    return _make_env(tmp_path)


@pytest.fixture(scope="module")
def shared_bronze(tmp_path_factory: pytest.TempPathFactory) -> SilverEnv:
    """Bronze ingerido una vez por módulo. Solo para tests que **no** escriben nada."""
    return _make_env(tmp_path_factory.mktemp("bronze-compartido"))
