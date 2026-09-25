from pathlib import Path

import pytest

from medallion.common.storage import STAGING_DIR, StorageError, atomic_write, local_path


class WriteText:
    """Escritor de prueba que registra la ruta temporal que recibe."""

    def __init__(self, content: str) -> None:
        self.content = content
        self.calls: list[Path] = []

    def __call__(self, path: Path) -> None:
        self.calls.append(path)
        path.write_text(self.content, encoding="utf-8")


def _staging_files(layer: Path) -> list[Path]:
    staging = layer / STAGING_DIR
    return list(staging.iterdir()) if staging.exists() else []


def test_writes_through_the_layer_staging_area(tmp_path: Path) -> None:
    target = tmp_path / "orders" / "part-0.parquet"
    write = WriteText("v1")

    atomic_write(target, str(tmp_path), write)

    assert target.read_text(encoding="utf-8") == "v1"
    [tmp] = write.calls
    assert tmp.parent == tmp_path / STAGING_DIR
    assert tmp.name.endswith("-part-0.parquet")
    assert _staging_files(tmp_path) == []


def test_replaces_an_existing_file(tmp_path: Path) -> None:
    target = tmp_path / "_manifest.json"
    atomic_write(target, str(tmp_path), WriteText("v1"))

    atomic_write(target, str(tmp_path), WriteText("v2"))

    assert target.read_text(encoding="utf-8") == "v2"
    assert _staging_files(tmp_path) == []


def test_failed_write_keeps_the_previous_file_and_leaves_no_temp_file(tmp_path: Path) -> None:
    target = tmp_path / "orders" / "part-0.parquet"
    atomic_write(target, str(tmp_path), WriteText("v1"))

    def broken_write(path: Path) -> None:
        path.write_text("a medio escribir", encoding="utf-8")
        raise OSError("disco lleno")

    with pytest.raises(OSError, match="disco lleno"):
        atomic_write(target, str(tmp_path), broken_write)

    assert target.read_text(encoding="utf-8") == "v1"
    assert _staging_files(tmp_path) == []


@pytest.mark.parametrize("root", ["abfs://lake@cuenta.dfs.core.windows.net/silver", "s3://b/x"])
def test_remote_storage_is_rejected(root: str) -> None:
    with pytest.raises(StorageError, match="Solo se admite almacenamiento local"):
        local_path(root)
