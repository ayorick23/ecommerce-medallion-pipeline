"""Genera el seed de feriados nacionales de Brasil para ``dim_tiempo`` (ADR 0032).

Se corre una sola vez (o al cambiar el calendario) y el CSV se versiona; el
pipeline no depende de la librería ``holidays``, que es de desarrollo::

    uv run python -m medallion.gold.feriados

El rango sale de las ``vars`` de ``dbt_project.yml``, la misma fuente que usa
``dim_tiempo``: el seed y el calendario no pueden desalinearse.
"""

import csv
from datetime import date
from pathlib import Path
from typing import Any, Final

import yaml

from medallion.gold.dbt import DBT_PROJECT_DIR

SEED_PATH: Final = DBT_PROJECT_DIR / "seeds" / "feriados_brasil.csv"


def calendar_range(project_dir: Path = DBT_PROJECT_DIR) -> tuple[date, date]:
    raw: Any = yaml.safe_load((project_dir / "dbt_project.yml").read_text(encoding="utf-8"))
    return (
        date.fromisoformat(raw["vars"]["calendario_inicio"]),
        date.fromisoformat(raw["vars"]["calendario_fin"]),
    )


def feriados_brasil(inicio: date, fin: date) -> list[tuple[date, str]]:
    """Feriados nacionales de Brasil entre ``inicio`` y ``fin``, con su nombre en portugués."""
    import holidays

    feriados = holidays.country_holidays(
        "BR", years=range(inicio.year, fin.year + 1), language="pt_BR"
    )
    return sorted((dia, nombre) for dia, nombre in feriados.items() if inicio <= dia <= fin)


def write_seed(rows: list[tuple[date, str]], path: Path = SEED_PATH) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerow(["fecha", "nombre_feriado"])
        writer.writerows((dia.isoformat(), nombre) for dia, nombre in rows)


def main() -> int:
    inicio, fin = calendar_range()
    rows = feriados_brasil(inicio, fin)
    write_seed(rows)
    print(f"{len(rows)} feriados entre {inicio} y {fin} escritos en {SEED_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
