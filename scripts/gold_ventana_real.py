"""Ventana real de Gold: incremental día a día contra full refresh (ADR 0029, 0036).

Uso, desde la raíz del repo::

    uv run python scripts/gold_ventana_real.py --desde 2018-01-01 --dias 30 --raiz RUTA

Copia ``raw/`` y ``bronze/`` del pipeline a ``RUTA`` (nunca escribe en la raíz
del pipeline) y ahí:

1. construye Silver y Gold con full refresh en D0;
2. avanza día por día hasta D0 + N con ``gold-build`` incremental;
3. guarda ese Gold como ``incremental.duckdb`` y lo reconstruye con full
   refresh en D0 + N (``full_refresh.duckdb``);
4. compara las dos versiones tabla por tabla e imprime los tiempos.

Es lento (cada día corre Silver y dbt), así que no forma parte de la CI: la
CI recorre cada día del fixture de Gold. El resultado se documenta en
``docs/schemas.md``.
"""

import argparse
import shutil
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from medallion.common.config import PipelineConfig, load_config
from medallion.gold.build import gold_build
from medallion.gold.dbt import warehouse_path
from medallion.silver.build import silver_build


def _paso(dia: date, config: PipelineConfig, full_refresh: bool) -> tuple[float, float]:
    inicio = time.perf_counter()
    silver_build(dia, config, datetime.now(UTC))
    medio = time.perf_counter()
    gold_build(dia, config, datetime.now(UTC), full_refresh=full_refresh)
    return medio - inicio, time.perf_counter() - medio


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--desde", type=date.fromisoformat, required=True, help="D0")
    parser.add_argument("--dias", type=int, required=True, help="N: días incrementales")
    parser.add_argument("--raiz", type=Path, required=True, help="raíz de trabajo (se vacía)")
    parser.add_argument("--config", help="YAML del pipeline (por defecto, el del repo)")
    parser.add_argument(
        "--solo-comparar",
        action="store_true",
        help="no construye: compara los dos .duckdb que ya están en --raiz",
    )
    args = parser.parse_args()
    if args.solo_comparar:
        return _comparar(args.raiz.resolve())

    origen = load_config(args.config)
    fuente = Path(origen.storage_root)
    raiz: Path = args.raiz.resolve()
    if raiz == fuente.resolve():
        parser.error("--raiz no puede ser la raíz del pipeline")
    shutil.rmtree(raiz, ignore_errors=True)
    for capa in ("raw", "bronze"):
        shutil.copytree(fuente / capa, raiz / capa)
    config = PipelineConfig(
        storage_root=str(raiz),
        sources=origen.sources,
        early_arriving_grace_days=origen.early_arriving_grace_days,
    )
    warehouse = warehouse_path(config)

    d0, dn = args.desde, args.desde + timedelta(days=args.dias)
    s, g = _paso(d0, config, full_refresh=True)
    print(f"{d0}  full refresh  silver {s:5.1f} s  gold {g:5.1f} s")
    tiempos_gold = []
    dia = d0
    while dia < dn:
        dia += timedelta(days=1)
        s, g = _paso(dia, config, full_refresh=False)
        tiempos_gold.append(g)
        print(f"{dia}  incremental   silver {s:5.1f} s  gold {g:5.1f} s")

    incremental = raiz / "incremental.duckdb"
    shutil.copyfile(warehouse, incremental)
    inicio = time.perf_counter()
    gold_build(dn, config, datetime.now(UTC), full_refresh=True)
    t_full = time.perf_counter() - inicio
    completo = raiz / "full_refresh.duckdb"
    shutil.copyfile(warehouse, completo)

    print(f"\nGold incremental promedio por día: {sum(tiempos_gold) / len(tiempos_gold):.1f} s")
    print(f"Gold full refresh en {dn}: {t_full:.1f} s")
    print(f"Tamaño de warehouse.duckdb: {warehouse.stat().st_size / 2**20:.1f} MB")
    return _comparar(raiz)


def _comparar(raiz: Path) -> int:
    # Import tardío: con --solo-comparar no hace falta nada más del pipeline.
    from medallion.gold.compare import compare_warehouses

    diferencias = compare_warehouses(raiz / "incremental.duckdb", raiz / "full_refresh.duckdb")
    if diferencias:
        print("\nDIFERENCIAS entre incremental y full refresh:")
        for linea in diferencias:
            print(f"  {linea}")
        return 1
    print("\nIncremental == full refresh: todas las tablas iguales")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
