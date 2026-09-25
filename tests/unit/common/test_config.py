from pathlib import Path

import pytest

from medallion.common.config import (
    CONFIG_PATH_ENV,
    STORAGE_ROOT_ENV,
    ConfigError,
    PipelineConfig,
    join_uri,
    load_config,
)

VALID_YAML = """
storage:
  root: ./data
sources:
  orders: olist_orders_dataset.csv
silver:
  early_arriving_grace_days: 120
"""

BASE = "storage:\n  root: ./data\nsources:\n  orders: a.csv\n"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(CONFIG_PATH_ENV, raising=False)
    monkeypatch.delenv(STORAGE_ROOT_ENV, raising=False)


def _write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "pipeline.yaml"
    path.write_text(content, encoding="utf-8")
    return path


def test_loads_valid_config(tmp_path: Path) -> None:
    config = load_config(_write(tmp_path, VALID_YAML))

    assert config.storage_root == "./data"
    assert config.sources == {"orders": "olist_orders_dataset.csv"}
    assert config.early_arriving_grace_days == 120


def test_env_var_overrides_storage_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(STORAGE_ROOT_ENV, "abfs://lake@cuenta.dfs.core.windows.net")

    config = load_config(_write(tmp_path, VALID_YAML))

    assert config.storage_root == "abfs://lake@cuenta.dfs.core.windows.net"


def test_config_path_can_come_from_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CONFIG_PATH_ENV, str(_write(tmp_path, VALID_YAML)))

    assert load_config().storage_root == "./data"


def test_repo_config_declares_all_olist_sources() -> None:
    config = load_config(Path(__file__).parents[3] / "config" / "pipeline.yaml")

    assert config.early_arriving_grace_days == 120
    assert set(config.sources) == {
        "orders",
        "order_items",
        "order_payments",
        "order_reviews",
        "customers",
        "products",
        "sellers",
        "geolocation",
        "category_translation",
    }


@pytest.mark.parametrize(
    "content",
    [
        "- no es un mapeo",
        "sources:\n  orders: a.csv\n",
        "storage:\n  root: ./data\n",
        "storage:\n  root: ./data\nsources: {}\n",
        "storage:\n  root: ./data\nsources:\n  orders: 3\n",
        BASE,
        BASE + "silver:\n  early_arriving_grace_days: -1\n",
        BASE + "silver:\n  early_arriving_grace_days: 120.5\n",
        BASE + "silver:\n  early_arriving_grace_days: true\n",
        BASE + "silver: 120\n",
    ],
    ids=[
        "raiz-no-mapeo",
        "sin-storage",
        "sin-sources",
        "sources-vacio",
        "archivo-no-texto",
        "sin-plazo",
        "plazo-negativo",
        "plazo-decimal",
        "plazo-booleano",
        "silver-no-mapeo",
    ],
)
def test_invalid_config_raises(tmp_path: Path, content: str) -> None:
    with pytest.raises(ConfigError):
        load_config(_write(tmp_path, content))


def test_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="No existe"):
        load_config(tmp_path / "no-existe.yaml")


@pytest.mark.parametrize(
    ("root", "expected"),
    [
        ("./data", "./data/raw/orders.csv"),
        ("./data/", "./data/raw/orders.csv"),
        (
            "abfs://lake@cuenta.dfs.core.windows.net",
            "abfs://lake@cuenta.dfs.core.windows.net/raw/orders.csv",
        ),
    ],
)
def test_source_uri_works_for_local_and_remote_roots(root: str, expected: str) -> None:
    config = PipelineConfig(
        storage_root=root, sources={"orders": "orders.csv"}, early_arriving_grace_days=120
    )

    assert config.source_uri("orders") == expected


def test_source_uri_unknown_table_raises() -> None:
    config = PipelineConfig(storage_root="./data", sources={}, early_arriving_grace_days=120)

    with pytest.raises(ConfigError, match="order_items"):
        config.source_uri("order_items")


def test_join_uri_strips_redundant_separators() -> None:
    assert join_uri("root/", "/bronze/", "orders") == "root/bronze/orders"
