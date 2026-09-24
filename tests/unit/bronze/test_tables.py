from pathlib import Path

from medallion.bronze.tables import EVENT_TABLES, REFERENCE_TABLES
from medallion.common.config import load_config


def test_catalog_matches_configured_sources() -> None:
    config = load_config(Path(__file__).parents[3] / "config" / "pipeline.yaml")

    assert set(EVENT_TABLES) | set(REFERENCE_TABLES) == set(config.sources)
    assert not set(EVENT_TABLES) & set(REFERENCE_TABLES)
