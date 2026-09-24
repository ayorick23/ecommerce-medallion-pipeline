import importlib


def test_layer_packages_are_importable() -> None:
    for layer in ("common", "bronze", "silver", "gold"):
        importlib.import_module(f"medallion.{layer}")
