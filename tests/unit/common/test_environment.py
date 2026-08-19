import importlib


def test_src_packages_are_importable() -> None:
    for package in ("common", "bronze", "silver", "gold"):
        importlib.import_module(package)
