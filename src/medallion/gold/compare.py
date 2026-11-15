"""Comparación de contenido entre dos ``warehouse.duckdb`` (ADR 0029, 0036).

La usan las pruebas de equivalencia (incremental == full refresh), de
idempotencia y de atomicidad, y la ventana real (``scripts/gold_ventana_real.py``).

Qué se compara y qué no:

- Solo las **tablas** del esquema ``main``: los marts. Las vistas de staging e
  intermediate guardan la ruta absoluta de Silver, así que leerlas en una
  copia lee el Silver de *hoy* de su raíz, no el que se usó para construirla.
- ``_gold_build`` no se compara: son metadatos de la corrida (``built_at``,
  ``full_refresh``), que difieren por definición entre un incremental y un
  full refresh del mismo día.
- Primero el esquema (nombres, orden y tipos de las columnas) y después las
  filas como multiconjunto, con ``EXCEPT ALL`` en los dos sentidos: una fila
  duplicada en un solo lado es una diferencia, y ``EXCEPT`` trata dos ``NULL``
  como iguales, a diferencia de un ``JOIN ... ON a.x = b.x``.
"""

from pathlib import Path
from typing import Final

import duckdb

EXCLUDED: Final = frozenset({"_gold_build"})


def _tables(con: duckdb.DuckDBPyConnection, catalog: str) -> set[str]:
    rows = con.execute(
        "select table_name from information_schema.tables "
        "where table_catalog = ? and table_schema = 'main' and table_type = 'BASE TABLE'",
        [catalog],
    ).fetchall()
    return {name for (name,) in rows} - EXCLUDED


def _schema(con: duckdb.DuckDBPyConnection, catalog: str, table: str) -> list[tuple[str, str]]:
    return [
        (str(name), str(kind))
        for name, kind in con.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_catalog = ? and table_schema = 'main' and table_name = ? "
            "order by ordinal_position",
            [catalog, table],
        ).fetchall()
    ]


def _count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(f"select count(*) from ({sql})").fetchone()
    return int(row[0]) if row else 0


def compare_warehouses(a: Path, b: Path) -> list[str]:
    """Diferencias legibles entre dos Gold; lista vacía si son iguales."""
    with duckdb.connect() as con:
        # Alias propios: los dos archivos pueden llamarse igual (warehouse.duckdb).
        con.execute(f"attach '{a.resolve().as_posix()}' as a (read_only)")
        con.execute(f"attach '{b.resolve().as_posix()}' as b (read_only)")
        tables_a, tables_b = _tables(con, "a"), _tables(con, "b")
        diffs = [f"{t}: solo en {a.name}" for t in sorted(tables_a - tables_b)]
        diffs += [f"{t}: solo en {b.name}" for t in sorted(tables_b - tables_a)]

        for table in sorted(tables_a & tables_b):
            schema_a, schema_b = _schema(con, "a", table), _schema(con, "b", table)
            if schema_a != schema_b:
                diffs.append(f"{table}: esquemas distintos {schema_a} != {schema_b}")
                continue
            only_a = _count(con, f"from a.main.{table} except all from b.main.{table}")
            only_b = _count(con, f"from b.main.{table} except all from a.main.{table}")
            if only_a or only_b:
                diffs.append(
                    f"{table}: {only_a} filas solo en {a.name}, {only_b} filas solo en {b.name}"
                )
    return diffs
