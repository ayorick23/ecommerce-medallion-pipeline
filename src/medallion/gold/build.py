"""Construcción de Gold "a la fecha D" (ADR 0027, 0028).

Uso, desde la raíz del repo::

    uv run gold-build --dia AAAA-MM-DD [--full-refresh] [--config RUTA]

Verifica que Silver esté completo y sea de D, construye Gold con ``dbt build``
sobre una copia de ``warehouse.duckdb`` y, solo si todo pasa, la publica
reemplazando el archivo. Si algo falla, Gold queda exactamente como estaba. En
la Fase 5 Airflow llama a este comando, no a ``dbt build`` directo.
"""

import argparse
import os
import shutil
import sys
import time
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final

import duckdb

from medallion.common.config import PipelineConfig, load_config
from medallion.common.storage import STAGING_DIR, StorageError, local_path
from medallion.gold.dbt import dbt_env, run_dbt, warehouse_path
from medallion.silver.build import read_manifest

BUILD_TABLE: Final = "_gold_build"
MARTS_SCHEMA: Final = "main"
BUILD_DIR: Final = "build"


class GoldBuildError(RuntimeError):
    """Gold no se construyó; el ``warehouse.duckdb`` anterior quedó intacto."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _naive_utc(moment: datetime) -> datetime:
    # DuckDB guarda TIMESTAMP sin zona: se guarda en UTC y así se documenta.
    return moment.astimezone(UTC).replace(tzinfo=None)


def read_gold_build(warehouse: Path) -> dict[str, Any] | None:
    """Metadatos de la última publicación, o ``None`` si Gold no se construyó con gold-build."""
    if not warehouse.is_file():
        return None
    with duckdb.connect(str(warehouse), read_only=True) as con:
        exists = con.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = ? and table_name = ?",
            [MARTS_SCHEMA, BUILD_TABLE],
        ).fetchone()
        if not exists or exists[0] == 0:
            return None
        cursor = con.execute(f"select * from {MARTS_SCHEMA}.{BUILD_TABLE}")
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(zip([col[0] for col in cursor.description], row, strict=True))


def check_silver(silver_uri: str, dia: date) -> dict[str, Any]:
    """Manifiesto de Silver, si Silver está completo y es de ``dia`` (ADR 0024, 0027)."""
    manifest = read_manifest(silver_uri)
    if manifest is None:
        raise GoldBuildError(
            "Silver está incompleto (falta _manifest.json): corre silver-build antes que gold-build"
        )
    if manifest["as_of"] != dia.isoformat():
        raise GoldBuildError(
            f"Silver es del {manifest['as_of']} y se pidió Gold del {dia}: "
            f"corre silver-build --dia {dia} antes"
        )
    return manifest


def check_not_backwards(previous: dict[str, Any] | None, dia: date, full_refresh: bool) -> None:
    """Ir a un D anterior al de Gold exige ``--full-refresh`` (ADR 0028, 0029)."""
    if previous is None or full_refresh:
        return
    if previous["as_of"] > dia:
        raise GoldBuildError(
            f"Gold ya está al {previous['as_of']}: para reconstruirlo al {dia} "
            "hace falta --full-refresh (los hechos incrementales no retroceden)"
        )


def write_build_info(
    db: Path, dia: date, built_at: datetime, silver_built_at: datetime, full_refresh: bool
) -> dict[str, int]:
    """Escribe ``_gold_build`` con las filas de cada mart y deja todo en el archivo."""
    with duckdb.connect(str(db)) as con:
        marts = [
            name
            for (name,) in con.execute(
                "select table_name from information_schema.tables "
                "where table_schema = ? and table_name <> ? order by table_name",
                [MARTS_SCHEMA, BUILD_TABLE],
            ).fetchall()
        ]
        rows: dict[str, int] = {}
        for mart in marts:
            count = con.execute(f"select count(*) from {MARTS_SCHEMA}.{mart}").fetchone()
            rows[mart] = count[0] if count else 0
        con.execute(
            f"create or replace table {MARTS_SCHEMA}.{BUILD_TABLE} as select "
            "cast(? as date) as as_of, "
            "cast(? as timestamp) as built_at, "
            "cast(? as timestamp) as silver_built_at, "
            "cast(? as boolean) as full_refresh, "
            "map(cast(? as varchar[]), cast(? as bigint[])) as filas",
            [
                dia,
                _naive_utc(built_at),
                _naive_utc(silver_built_at),
                full_refresh,
                list(rows),
                list(rows.values()),
            ],
        )
        # Sin esto, lo escrito podría quedar en un .wal aparte que os.replace no mueve.
        con.execute("checkpoint")
    return rows


def publish(target: Path, build_dir: Path, build: Callable[[Path], None]) -> None:
    """Construye en ``build_dir/<nombre de target>`` y lo mueve sobre ``target`` (ADR 0028).

    El temporal se llama igual que el archivo publicado, a diferencia de
    ``atomic_write``: DuckDB nombra el catálogo por el nombre del archivo, y dbt
    escribe ese catálogo en las vistas de staging e intermediate. Con otro
    nombre, esas vistas quedarían rotas en el Gold publicado.

    Si ``build`` falla, se borra la carpeta y ``target`` queda como estaba.
    """
    shutil.rmtree(build_dir, ignore_errors=True)  # restos de una corrida interrumpida
    build_dir.mkdir(parents=True)
    try:
        tmp = build_dir / target.name
        build(tmp)
        os.replace(tmp, target)
    finally:
        shutil.rmtree(build_dir, ignore_errors=True)


def gold_build(
    dia: date, config: PipelineConfig, built_at: datetime, full_refresh: bool = False
) -> dict[str, Any]:
    """Construye y publica Gold(``dia``); devuelve los metadatos escritos en ``_gold_build``.

    Si una guarda o ``dbt build`` falla, lanza ``GoldBuildError`` y el
    ``warehouse.duckdb`` anterior queda intacto.
    """
    if built_at.tzinfo is None:
        raise ValueError("built_at debe tener zona horaria (p. ej. datetime.now(UTC))")
    target = warehouse_path(config)  # StorageError si la raíz no es local (ADR 0027)
    manifest = check_silver(config.layer_uri("silver"), dia)
    check_not_backwards(read_gold_build(target), dia, full_refresh)
    silver_built_at = datetime.fromisoformat(manifest["built_at"])
    rows: dict[str, int] = {}

    def build_into(tmp: Path) -> None:
        # Sobre una copia: los incrementales parten del Gold anterior. Un full
        # refresh parte vacío y no arrastra tablas de modelos que ya no existen.
        if target.is_file() and not full_refresh:
            shutil.copyfile(target, tmp)
        args = ["build", "--full-refresh"] if full_refresh else ["build"]
        if not run_dbt(args, dbt_env(config, tmp)):
            raise GoldBuildError(
                f"dbt build falló para Gold({dia}); el Gold anterior quedó intacto"
            )
        rows.update(write_build_info(tmp, dia, built_at, silver_built_at, full_refresh))

    build_dir = local_path(config.layer_uri("gold")) / STAGING_DIR / BUILD_DIR
    publish(target, build_dir, build_into)
    return {
        "as_of": dia.isoformat(),
        "built_at": built_at.astimezone(UTC).isoformat(),
        "silver_built_at": manifest["built_at"],
        "full_refresh": full_refresh,
        "filas": rows,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Construye Gold a la fecha D.")
    parser.add_argument(
        "--dia", type=date.fromisoformat, required=True, help="fecha D (AAAA-MM-DD)"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="reconstruye Gold desde cero (obligatorio para ir a un D anterior)",
    )
    parser.add_argument("--config", help="YAML de configuración (por defecto, el del pipeline)")
    args = parser.parse_args(argv)

    start = time.perf_counter()
    try:
        info = gold_build(args.dia, load_config(args.config), _utc_now(), args.full_refresh)
    except (GoldBuildError, StorageError) as error:
        print(error, file=sys.stderr)
        return 1

    for mart, rows in info["filas"].items():
        print(f"{mart:22} {rows:>9,}")
    modo = "full refresh" if info["full_refresh"] else "incremental"
    print(f"\nGold({args.dia}) publicado ({modo}) en {time.perf_counter() - start:.1f} s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
