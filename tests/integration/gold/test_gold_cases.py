"""Cada caso del fixture de Gold, con nombre, sobre el Gold incremental de cada día.

Las propiedades generales (equivalencia, sin futuro) están en
``test_gold_properties.py``; aquí se verifica *qué* resultado da cada caso de
las ADRs 0030, 0031, 0033, 0034 y 0037.
"""

from datetime import date, datetime

import pytest

from tests.integration.gold.conftest import DAYS, LAST_DAY, GoldWalk, fetch

JUNE = {n: date(2017, 6, n) for n in range(1, 11)}


def _at(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2017, 6, day, hour, minute)


def _version(walk: GoldWalk, persona: str, valid_from: datetime) -> str:
    ((sk,),) = fetch(
        walk.incremental[LAST_DAY],
        "select cliente_sk from dim_cliente where customer_unique_id = ? and valid_from = ?",
        [persona, valid_from],
    )
    return str(sk)


# --- dim_cliente (ADR 0030) ----------------------------------------------------------


def test_a_person_who_moves_and_returns_has_three_versions(gold_walk: GoldWalk) -> None:
    versions = fetch(
        gold_walk.incremental[LAST_DAY],
        "select customer_state, valid_from, valid_to, is_current from dim_cliente "
        "where customer_unique_id = 'u1' order by valid_from",
    )
    assert versions == [
        ("SP", _at(1, 10), _at(3, 10), False),
        ("RJ", _at(3, 10), _at(6, 8), False),
        ("SP", _at(6, 8), None, True),
    ]


def test_several_orders_from_the_same_address_are_one_version(gold_walk: GoldWalk) -> None:
    counts = fetch(
        gold_walk.incremental[LAST_DAY],
        "select customer_unique_id, count(*) from dim_cliente "
        "where customer_unique_id in ('u3', 'u5') group by all order by all",
    )
    assert counts == [("u3", 1), ("u5", 1)]


@pytest.mark.parametrize("dia", DAYS, ids=str)
def test_order_lines_keep_the_version_of_their_purchase(gold_walk: GoldWalk, dia: date) -> None:
    keys = fetch(
        gold_walk.incremental[dia],
        "select distinct cliente_sk from fct_pedidos where order_id = 'o1'",
    )
    assert keys == [(_version(gold_walk, "u1", _at(1, 10)),)]


def test_a_review_takes_the_version_current_at_its_creation(gold_walk: GoldWalk) -> None:
    # r1 es de o1 (dirección A), pero se crea el día 5, con la B vigente.
    ((sk,),) = fetch(
        gold_walk.incremental[LAST_DAY], "select cliente_sk from fct_reviews where review_id = 'r1'"
    )
    assert sk == _version(gold_walk, "u1", _at(3, 10))


# --- Estado del pedido (ADR 0031) ----------------------------------------------------


def test_each_order_has_its_combined_state_on_the_last_day(gold_walk: GoldWalk) -> None:
    states = dict(
        fetch(gold_walk.incremental[LAST_DAY], "select distinct order_id, estado from fct_pedidos")
    )
    assert states == {
        "o1": "entregado",
        "o2": "cancelado",  # cancelado después de entregado
        "o3": "aprobado",  # invoiced
        "o4": "entregado",
        "o5": "aprobado",  # processing
        "o6": "despachado",
        "o7": "no_disponible",
        "o8": "entregado",
        "o9": "entregado",
        "o10": "aprobado",
        "o11": "cancelado",  # cancelado antes de aprobarse
    }


@pytest.mark.parametrize("dia", DAYS[1:], ids=str)
def test_an_order_canceled_after_delivery_is_canceled_from_its_purchase(
    gold_walk: GoldWalk, dia: date
) -> None:
    # La cancelación no tiene fecha: figura desde la compra (fuga aceptada, ADR 0031).
    # La entrega, en cambio, aparece recién el día que ocurre.
    rows = fetch(
        gold_walk.incremental[dia],
        "select estado, fecha_entrega from fct_pedidos where order_id = 'o2'",
    )
    assert rows == [("cancelado", JUNE[5] if dia >= JUNE[5] else None)]


# --- Entrega tardía (ADR 0033, 0037) -------------------------------------------------


@pytest.mark.parametrize("dia", DAYS[1:], ids=str)
def test_a_late_delivery_is_unknown_until_it_happens(gold_walk: GoldWalk, dia: date) -> None:
    rows = fetch(
        gold_walk.incremental[dia],
        "select fecha_entrega, dias_retraso, es_entrega_tardia from fct_pedidos "
        "where order_id = 'o8'",
    )
    assert rows == [(JUNE[8], 2, True) if dia >= JUNE[8] else (None, None, None)]


def test_delivering_late_on_the_promised_day_is_not_late(gold_walk: GoldWalk) -> None:
    # o9: prometido el 9, entregado el 9 a las 18:00. o1: 6 días antes de lo prometido.
    rows = fetch(
        gold_walk.incremental[LAST_DAY],
        "select distinct order_id, dias_retraso, es_entrega_tardia from fct_pedidos "
        "where order_id in ('o1', 'o9') order by order_id",
    )
    assert rows == [("o1", -6, False), ("o9", 0, False)]


# --- Reviews (ADR 0034) --------------------------------------------------------------


def test_reviews_start_empty(gold_walk: GoldWalk) -> None:
    first = gold_walk.incremental[DAYS[0]]
    assert fetch(first, "select count(*) from fct_reviews") == [(0,)]
    assert fetch(first, "select count(*) from puente_review_pedido") == [(0,)]


@pytest.mark.parametrize("dia", DAYS[4:], ids=str)
def test_an_answer_appears_on_its_day(gold_walk: GoldWalk, dia: date) -> None:
    rows = fetch(
        gold_walk.incremental[dia],
        "select fecha_respuesta, round(dias_hasta_respuesta, 4) from fct_reviews "
        "where review_id = 'r1'",
    )
    assert rows == [(JUNE[7], round(2 + 10 / 24, 4)) if dia >= JUNE[7] else (None, None)]


@pytest.mark.parametrize("dia", DAYS[6:], ids=str)
def test_a_review_of_two_orders_is_one_row_with_two_links(gold_walk: GoldWalk, dia: date) -> None:
    warehouse = gold_walk.incremental[dia]
    links = fetch(
        warehouse,
        "select order_id from puente_review_pedido where review_id = 'r2' order by order_id",
    )
    assert links == ([("o10",), ("o9",)] if dia >= JUNE[8] else [("o9",)])
    # Su respuesta es del día 10: nunca es visible en el rango.
    assert fetch(
        warehouse,
        "select fecha_respuesta, tiene_comentario from fct_reviews where review_id = 'r2'",
    ) == [(None, False)]


@pytest.mark.parametrize("dia", DAYS[2:], ids=str)
def test_an_early_review_enters_with_its_order(gold_walk: GoldWalk, dia: date) -> None:
    # r3 se crea el día 3; su pedido o11 llega el 9, el primero de u6.
    rows = fetch(
        gold_walk.incremental[dia],
        "select cliente_sk, fecha_creacion, tiene_comentario, _visible_desde from fct_reviews "
        "where review_id = 'r3'",
    )
    if dia < JUNE[9]:
        assert rows == []
    else:
        # Título sin mensaje: no cuenta como comentario.
        assert rows == [(_version(gold_walk, "u6", _at(9, 13)), JUNE[3], False, _at(9, 13))]
