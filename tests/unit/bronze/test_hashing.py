import hashlib
import re

import polars as pl
import pytest

from medallion.bronze.hashing import batch_hash


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _reference_hash(columns: list[str], rows: list[tuple[str | None, ...]]) -> str:
    """Reimplementación de la ADR 0014 en Python puro, sin Polars."""

    def encode(values: tuple[str | None, ...]) -> bytes:
        parts = [b"N" if v is None else b"S%d:%s" % (len(v.encode()), v.encode()) for v in values]
        return b"".join(parts) + b"\n"

    records = [encode(tuple(columns)), *sorted(encode(row) for row in rows)]
    return hashlib.sha256(b"".join(records)).hexdigest()


def _batch(columns: list[str], rows: list[tuple[str | None, ...]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=dict.fromkeys(columns, pl.String), orient="row")


def test_matches_the_adr_example_byte_for_byte() -> None:
    batch = _batch(
        ["order_id", "review_comment_title"],
        [("xyz", "São Paulo"), ("abc", "N"), ("abc", None), ("abc", "")],
    )
    expected = _sha256(
        "".join(
            [
                "S8:order_idS20:review_comment_title\n",
                "S3:abcN\n",
                "S3:abcS0:\n",
                "S3:abcS1:N\n",
                "S3:xyzS10:São Paulo\n",
            ]
        )
    )

    assert batch_hash(batch) == expected


def test_matches_pure_python_reference_on_tricky_values() -> None:
    columns = ["a", "b"]
    rows: list[tuple[str | None, ...]] = [
        ("Z", "a,b"),
        ("á", '"quoted"'),
        ("a", "line\nbreak"),
        (None, None),
        ("", "N"),
        ("S1:x", "\\N"),
        ("a", "line\nbreak"),
    ]

    assert batch_hash(_batch(columns, rows)) == _reference_hash(columns, rows)


def test_hash_is_lowercase_sha256_hex() -> None:
    result = batch_hash(_batch(["a"], [("x",)]))

    assert re.fullmatch(r"[0-9a-f]{64}", result)


def test_row_order_does_not_change_the_hash() -> None:
    rows: list[tuple[str | None, ...]] = [("1", "x"), ("2", None), ("3", "z")]

    assert batch_hash(_batch(["a", "b"], rows)) == batch_hash(_batch(["a", "b"], rows[::-1]))


@pytest.mark.parametrize(
    ("left", "right"),
    [
        pytest.param((["a"], [(None,)]), (["a"], [("",)]), id="nulo-vs-vacio"),
        pytest.param((["a"], [(None,)]), (["a"], [("N",)]), id="nulo-vs-texto-N"),
        pytest.param(
            (["a", "b"], [("ab", "c")]), (["a", "b"], [("a", "bc")]), id="frontera-entre-valores"
        ),
        pytest.param(
            (["a"], [("x\nS1:y",)]), (["a"], [("x",), ("y",)]), id="salto-de-linea-en-valor"
        ),
        pytest.param((["a"], [("x",), ("x",)]), (["a"], [("x",)]), id="fila-duplicada"),
        pytest.param((["a"], [("x",)]), (["b"], [("x",)]), id="columna-renombrada"),
        pytest.param(
            (["a", "b"], [("x", "y")]), (["b", "a"], [("y", "x")]), id="columnas-reordenadas"
        ),
    ],
)
def test_distinct_batches_have_distinct_hashes(
    left: tuple[list[str], list[tuple[str | None, ...]]],
    right: tuple[list[str], list[tuple[str | None, ...]]],
) -> None:
    assert batch_hash(_batch(*left)) != batch_hash(_batch(*right))


def test_empty_batch_hashes_only_the_header() -> None:
    assert batch_hash(_batch(["order_id"], [])) == _sha256("S8:order_id\n")


def test_non_text_columns_are_rejected() -> None:
    batch = pl.DataFrame({"order_id": ["a"], "price": [1.5]})

    with pytest.raises(ValueError, match="price"):
        batch_hash(batch)


def test_lineage_columns_are_rejected() -> None:
    batch = _batch(["order_id", "batch_hash"], [("a", "abc")])

    with pytest.raises(ValueError, match="batch_hash"):
        batch_hash(batch)
