import pytest

from medallion import orchestration
from medallion_dag import contract
from medallion_dag.contract import (
    EXIT_DETERMINISTIC,
    Outcome,
    alert_message,
    layer_command,
    outcome,
    pipeline_python,
)


def test_exit_code_matches_the_pipeline_side() -> None:
    # El DAG no importa medallion: el 3 está duplicado y aquí se mantiene en sincronía.
    assert EXIT_DETERMINISTIC == orchestration.EXIT_DETERMINISTIC


def test_layers_match_the_pipeline_side() -> None:
    assert list(contract.LAYERS) == list(orchestration.LAYERS)


@pytest.mark.parametrize(
    ("returncode", "expected"),
    [
        (0, Outcome.SUCCESS),
        (3, Outcome.FAIL),
        (1, Outcome.RETRY),  # excepción inesperada
        (2, Outcome.RETRY),  # argparse: se acepta reintentarlo (ADR 0041)
        (-9, Outcome.RETRY),  # proceso matado (p. ej. sin memoria)
    ],
)
def test_outcome_by_exit_code(returncode: int, expected: Outcome) -> None:
    assert outcome(returncode) is expected


def test_layer_command() -> None:
    assert layer_command("/venv/bin/python", "gold", "2017-03-10") == [
        "/venv/bin/python",
        "-m",
        "medallion.orchestration",
        "gold",
        "2017-03-10",
    ]


@pytest.mark.parametrize("home", ["/opt/medallion", "/opt/medallion/"])
def test_pipeline_python(home: str) -> None:
    assert pipeline_python(home) == "/opt/medallion/.venv/bin/python"


def test_unknown_layer_is_rejected() -> None:
    with pytest.raises(ValueError, match="platino"):
        layer_command("/venv/bin/python", "platino", "2017-03-10")


def test_alert_message_has_run_metadata() -> None:
    subject, body = alert_message(
        "medallion_diario", "silver", "2017-03-10", 3, "http://localhost:8080/log?x=1"
    )
    assert subject == "[medallion] Falló medallion_diario.silver del 2017-03-10"
    for fragment in ("silver", "medallion_diario", "2017-03-10", "intento 3", "log?x=1"):
        assert fragment in body
