from datetime import date
from pathlib import Path

from medallion.gold.feriados import SEED_PATH, calendar_range, feriados_brasil, write_seed


def test_calendar_range_comes_from_dbt_project() -> None:
    assert calendar_range() == (date(2016, 1, 1), date(2020, 12, 31))


def test_only_national_holidays_in_range() -> None:
    rows = feriados_brasil(date(2017, 1, 1), date(2017, 12, 31))

    assert len(rows) == 9
    assert (date(2017, 4, 14), "Sexta-feira Santa") in rows  # móvil: depende de la Pascua
    assert all(date(2017, 1, 1) <= dia <= date(2017, 12, 31) for dia, _ in rows)


def test_versioned_seed_matches_generator(tmp_path: Path) -> None:
    # El seed no se edita a mano: regenerarlo tiene que dar el mismo archivo.
    regenerated = tmp_path / "feriados.csv"
    write_seed(feriados_brasil(*calendar_range()), regenerated)

    assert regenerated.read_bytes() == SEED_PATH.read_bytes().replace(b"\r\n", b"\n")
