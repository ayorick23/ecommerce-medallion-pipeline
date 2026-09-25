"""Escritura atómica de archivos dentro de una capa (ADR 0015, 0024).

Un archivo se escribe primero en ``<capa>/_staging/`` y se mueve a su destino
con ``os.replace``, que reemplaza el archivo de forma atómica en Windows y en
POSIX: un lector nunca ve un archivo a medio escribir. El staging vive dentro
de la raíz de la capa, fuera de las carpetas de las tablas, para que el
movimiento no cruce sistemas de archivos y un huérfano no rompa a los lectores.
Solo almacenamiento local.
"""

import os
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Final

STAGING_DIR: Final = "_staging"


class StorageError(ValueError):
    """La escritura o lectura no se puede hacer con las garantías de la capa."""


def local_path(uri: str) -> Path:
    """Ruta local de una URI de almacenamiento; una URI remota es un error explícito."""
    if "://" in uri:
        raise StorageError(
            f"Solo se admite almacenamiento local (ADR 0015, 0024); recibido: {uri!r}"
        )
    return Path(uri)


def atomic_write(target: Path, layer_uri: str, write: Callable[[Path], None]) -> None:
    """Escribe ``target`` de forma atómica.

    ``write`` recibe una ruta temporal dentro de ``<layer_uri>/_staging/`` y
    escribe ahí el archivo completo; después se mueve a ``target``. Si algo
    falla, se borra el temporal y ``target`` queda como estaba.
    """
    staging = local_path(layer_uri) / STAGING_DIR
    staging.mkdir(parents=True, exist_ok=True)
    tmp = staging / f"{uuid.uuid4().hex}-{target.name}"
    try:
        write(tmp)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
