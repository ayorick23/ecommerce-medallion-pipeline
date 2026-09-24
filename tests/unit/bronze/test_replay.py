from collections.abc import Mapping
from datetime import UTC, date, datetime

import polars as pl
import pytest

import medallion.bronze.replay as replay_module
from medallion.bronze.ingest import TableResult
from medallion.bronze.replay import replay, source_day_range
from medallion.common.config import PipelineConfig

CONFIG = PipelineConfig(storage_root="unused", sources={})


def _text(data: dict[str, list[str | None]]) -> pl.DataFrame:
    return pl.DataFrame(data, schema=dict.fromkeys(data, pl.String))


def _anchors(
    purchase: list[str | None], delivered: list[str | None], review: list[str | None]
) -> dict[str, pl.DataFrame]:
    n = len(purchase)
    return {
        "orders": _text(
            {
                "order_purchase_timestamp": purchase,
                "order_approved_at": [None] * n,
                "order_delivered_carrier_date": [None] * n,
                "order_delivered_customer_date": delivered,
            }
        ),
        "order_reviews": _text({"review_creation_date": review}),
    }


def test_source_range_spans_every_event_anchor() -> None:
    sources = _anchors(
        purchase=["2017-03-15 10:00:00", "2016-09-04 21:15:19"],
        delivered=["2017-03-20 18:00:00", None],
        review=["2018-10-17 00:00:00"],
    )

    assert source_day_range(sources) == (date(2016, 9, 4), date(2018, 10, 17))


def test_source_without_events_is_rejected() -> None:
    with pytest.raises(ValueError, match="ningún evento"):
        source_day_range(_anchors(purchase=[], delivered=[], review=[]))


def test_replay_ingests_every_day_in_order_with_its_own_clock_reading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[date, datetime]] = []

    def fake_ingest_day(
        dia: date,
        sources: Mapping[str, pl.DataFrame],
        config: PipelineConfig,
        ingested_at: datetime,
    ) -> list[TableResult]:
        calls.append((dia, ingested_at))
        return [TableResult("orders", "written", dia.day)]

    monkeypatch.setattr(replay_module, "ingest_day", fake_ingest_day)
    readings = iter(datetime(2026, 9, 24, h, tzinfo=UTC) for h in range(10))

    results = list(
        replay({}, CONFIG, date(2017, 2, 27), date(2017, 3, 2), clock=lambda: next(readings))
    )

    expected_days = [date(2017, 2, 27), date(2017, 2, 28), date(2017, 3, 1), date(2017, 3, 2)]
    assert [dia for dia, _ in results] == expected_days
    assert calls == [(d, datetime(2026, 9, 24, h, tzinfo=UTC)) for h, d in enumerate(expected_days)]


def test_single_day_range_is_inclusive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(replay_module, "ingest_day", lambda *_: [])

    assert [d for d, _ in replay({}, CONFIG, date(2017, 3, 15), date(2017, 3, 15))] == [
        date(2017, 3, 15)
    ]


def test_inverted_range_is_rejected() -> None:
    with pytest.raises(ValueError, match="Rango vacío"):
        list(replay({}, CONFIG, date(2017, 3, 16), date(2017, 3, 15)))
