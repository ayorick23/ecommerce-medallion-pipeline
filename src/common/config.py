"""Carga de la configuración externalizada del pipeline (ADR 0008).

La raíz de almacenamiento puede ser una ruta local o una URI remota
(p. ej. ``abfs://...``); por eso las rutas se manejan como texto unido con
``/`` y no con ``pathlib``, que no entiende esquemas de URI.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml

DEFAULT_CONFIG_PATH = Path("config/pipeline.yaml")
CONFIG_PATH_ENV = "PIPELINE_CONFIG"
STORAGE_ROOT_ENV = "PIPELINE_STORAGE_ROOT"

Layer = Literal["raw", "bronze", "silver", "gold"]


class ConfigError(ValueError):
    """La configuración falta o no tiene la forma esperada."""


def join_uri(*parts: str) -> str:
    """Une segmentos de ruta con ``/``, válido tanto local como en URIs remotas."""
    head, *tail = parts
    return "/".join([head.rstrip("/\\"), *(p.strip("/\\") for p in tail)])


@dataclass(frozen=True)
class PipelineConfig:
    storage_root: str
    sources: dict[str, str]

    def layer_uri(self, layer: Layer) -> str:
        return join_uri(self.storage_root, layer)

    def source_uri(self, table: str) -> str:
        try:
            filename = self.sources[table]
        except KeyError:
            raise ConfigError(
                f"No hay archivo fuente configurado para la tabla {table!r}"
            ) from None
        return join_uri(self.layer_uri("raw"), filename)


def load_config(path: str | Path | None = None) -> PipelineConfig:
    """Lee el YAML de configuración.

    Orden de resolución de la ruta: argumento ``path`` → variable de entorno
    ``PIPELINE_CONFIG`` → ``config/pipeline.yaml``. La variable de entorno
    ``PIPELINE_STORAGE_ROOT``, si está definida, sobrescribe ``storage.root``.
    """
    config_path = Path(path or os.environ.get(CONFIG_PATH_ENV) or DEFAULT_CONFIG_PATH)
    if not config_path.is_file():
        raise ConfigError(f"No existe el archivo de configuración: {config_path}")

    raw: Any = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ConfigError(f"{config_path}: se esperaba un mapeo YAML en la raíz")

    storage = raw.get("storage")
    if not isinstance(storage, dict) or not isinstance(storage.get("root"), str):
        raise ConfigError(f"{config_path}: falta 'storage.root' (texto)")

    sources = raw.get("sources")
    if (
        not isinstance(sources, dict)
        or not sources
        or not all(isinstance(k, str) and isinstance(v, str) for k, v in sources.items())
    ):
        raise ConfigError(f"{config_path}: 'sources' debe ser un mapeo no vacío tabla → archivo")

    storage_root = os.environ.get(STORAGE_ROOT_ENV) or storage["root"]
    return PipelineConfig(storage_root=storage_root, sources=dict(sources))
