from datetime import date, datetime

import polars as pl

from medallion.silver.facts import (
    PENDING_DAYS,
    build_fact_tables,
    build_reviews,
    expired_reviews,
)
from medallion.silver.tables import DTYPES, TABLE_COLUMNS
from medallion.silver.validation import Failure

D = date(2018, 6, 22)
REVIEW_COLUMNS = [spec.name for spec in TABLE_COLUMNS["order_reviews"]]


def _typed(table: str, rows: list[dict[str, object]]) -> pl.DataFrame:
    schema = {spec.name: DTYPES[spec.kind] for spec in TABLE_COLUMNS[table]}
    return pl.DataFrame(rows, schema=schema)


def _orders(*order_ids: str) -> pl.DataFrame:
    return _typed("orders", [{"order_id": order_id} for order_id in order_ids])


def _review(
    review_id: str,
    order_id: str,
    created: datetime = datetime(2018, 6, 22),
    answered: datetime | None = datetime(2018, 6, 22, 18, 8, 44),
) -> dict[str, object]:
    return {
        "review_id": review_id,
        "order_id": order_id,
        "review_score": 3,
        "review_creation_date": created,
        "review_answer_timestamp": answered,
    }


def test_reviews_with_their_order_go_to_silver_and_the_rest_wait() -> None:
    reviews = _typed("order_reviews", [_review("r1", "o1"), _review("r2", "o-futuro")])

    tables = build_reviews(reviews, _orders("o1"), D)

    assert tables.reviews["review_id"].to_list() == ["r1"]
    assert tables.pending["review_id"].to_list() == ["r2"]


def test_a_review_shared_by_two_orders_keeps_both_links() -> None:
    reviews = _typed("order_reviews", [_review("r1", "o2"), _review("r1", "o1")])

    tables = build_reviews(reviews, _orders("o1", "o2"), D)

    assert tables.reviews.select("review_id", "order_id").rows() == [("r1", "o1"), ("r1", "o2")]


def test_an_answer_after_the_day_is_masked() -> None:
    reviews = _typed(
        "order_reviews",
        [
            _review("r1", "o1", answered=datetime(2018, 6, 22, 23, 59, 59)),  # mismo día: visible
            _review("r2", "o1", answered=datetime(2018, 6, 23, 0, 0, 0)),  # día siguiente
            _review("r3", "o1", answered=None),
        ],
    )

    tables = build_reviews(reviews, _orders("o1"), D)

    assert tables.reviews["review_answer_timestamp"].to_list() == [
        datetime(2018, 6, 22, 23, 59, 59),
        None,
        None,
    ]


def test_pending_reviews_count_the_days_they_have_waited() -> None:
    reviews = _typed("order_reviews", [_review("r1", "o-futuro", created=datetime(2018, 6, 1))])

    pending = build_reviews(reviews, _orders(), D).pending

    assert pending.columns == [*REVIEW_COLUMNS, PENDING_DAYS]
    assert pending[PENDING_DAYS].to_list() == [21]


def test_a_pending_review_enters_silver_the_day_its_order_arrives() -> None:
    reviews = _typed("order_reviews", [_review("r1", "o1", created=datetime(2018, 6, 1))])

    before = build_reviews(reviews, _orders(), D)
    after = build_reviews(reviews, _orders("o1"), D)

    assert (before.reviews.height, before.pending.height) == (0, 1)
    assert (after.reviews.height, after.pending.height) == (1, 0)


def test_only_reviews_past_the_grace_period_are_orphans() -> None:
    pending = pl.DataFrame({"order_id": ["o1", "o2", "o3"], PENDING_DAYS: [120, 121, 300]})

    assert expired_reviews(pending, 120) == [
        Failure("order_reviews", "fk_huerfana", ("order_id",), 2, ("'o2'", "'o3'"))
    ]
    assert expired_reviews(pending, 300) == []


def test_items_and_payments_are_not_filtered_by_their_order() -> None:
    parsed = {
        "order_items": _typed(
            "order_items",
            [{"order_id": "o2", "order_item_id": 1}, {"order_id": "huerfano", "order_item_id": 1}],
        ),
        "order_payments": _typed(
            "order_payments",
            [
                {"order_id": "o2", "payment_sequential": 2},
                {"order_id": "o2", "payment_sequential": 1},
            ],
        ),
        "order_reviews": _typed("order_reviews", []),
    }

    tables, pending = build_fact_tables(parsed, _orders("o2"), D)

    assert tables["order_items"]["order_id"].to_list() == ["huerfano", "o2"]
    assert tables["order_payments"]["payment_sequential"].to_list() == [1, 2]
    assert list(tables) == ["order_items", "order_payments", "order_reviews"]
    assert pending.height == 0
