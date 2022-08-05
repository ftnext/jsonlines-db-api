from unittest.mock import mock_open

import pytest
from pytest_mock import MockerFixture
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Float, Integer, Order, String
from shillelagh.filters import (
    Equal,
    Impossible,
    IsNotNull,
    IsNull,
    NotEqual,
    Range,
)

from jsonlinesdb.adapter import JsonlFile

CONTENTS = """{"index": 10, "temperature": 15.2, "site": "Diamond_St"}
{"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"}
{"index": 12, "temperature": 13.3, "site": "Platinum_St"}
{"index": 13, "temperature": 12.1, "site": "Kodiak_Trail"}
"""


def test_jsonlfile_get_columns(mocker: MockerFixture):
    mocker.patch("builtins.open", mock_open(read_data=CONTENTS))

    adapter = JsonlFile("test.jsonl")

    assert adapter.get_columns() == {
        "index": Integer(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.ASCENDING,
            exact=True,
        ),
        "temperature": Float(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.NONE,
            exact=True,
        ),
        "site": String(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.NONE,
            exact=True,
        ),
    }


def test_jsonlfile_different_type(mocker: MockerFixture):
    contents = """{"a": 1}
{"a": 2.0}
{"a": "test"}"""
    mocker.patch("builtins.open", mock_open(read_data=contents))

    adapter = JsonlFile("test.jsonl")

    assert adapter.get_columns() == {
        "a": String(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.NONE,
            exact=True,
        )
    }


def test_jsonlfile_empty(mocker: MockerFixture):
    mocker.patch("builtins.open", mock_open(read_data=""))

    with pytest.raises(ProgrammingError) as excinfo:
        _ = JsonlFile("test.csv")
    assert str(excinfo.value) == "The file has no rows"


def test_jsonlfile_unordered(mocker: MockerFixture):
    contents = """{"a": 1}
{"a": 2}
{"a": 1}"""
    mocker.patch("builtins.open", mock_open(read_data=contents))

    adapter = JsonlFile("test.jsonl")

    assert adapter.get_columns() == {
        "a": Integer(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.NONE,
            exact=True,
        )
    }


def test_jsonlfile_single_row_of_data(mocker: MockerFixture):
    contents = """{"a": 1, "b": 2}"""
    mocker.patch("builtins.open", mock_open(read_data=contents))

    adapter = JsonlFile("test.jsonl")

    assert adapter.get_columns() == {
        "a": Integer(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.NONE,
            exact=True,
        ),
        "b": Integer(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.NONE,
            exact=True,
        ),
    }
    assert list(adapter.get_data({}, [])) == [{"a": 1, "b": 2, "rowid": 0}]


def test_jsonlfile_get_data(mocker: MockerFixture):
    mocker.patch("builtins.open", mock_open(read_data=CONTENTS))

    adapter = JsonlFile("test.jsonl")

    assert list(adapter.get_data({}, [])) == [
        {"rowid": 0, "index": 10, "temperature": 15.2, "site": "Diamond_St"},
        {
            "rowid": 1,
            "index": 11,
            "temperature": 13.1,
            "site": "Blacktail_Loop",
        },
        {"rowid": 2, "index": 12, "temperature": 13.3, "site": "Platinum_St"},
        {"rowid": 3, "index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
    ]

    assert list(
        adapter.get_data({"index": Range(11, None, False, False)}, [])
    ) == [
        {"rowid": 2, "index": 12, "temperature": 13.3, "site": "Platinum_St"},
        {"rowid": 3, "index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
    ]

    assert list(
        adapter.get_data({"index": Range(None, 11, False, True)}, [])
    ) == [
        {"rowid": 0, "index": 10, "temperature": 15.2, "site": "Diamond_St"},
        {
            "rowid": 1,
            "index": 11,
            "temperature": 13.1,
            "site": "Blacktail_Loop",
        },
    ]

    assert list(
        adapter.get_data(
            {
                "index": Range(None, 11, False, True),
                "temperature": Range(14, None, False, False),
            },
            [],
        )
    ) == [{"rowid": 0, "index": 10, "temperature": 15.2, "site": "Diamond_St"}]


def test_jsonlfile_get_data_impossible_filter(mocker: MockerFixture):
    mocker.patch("builtins.open", mock_open(read_data=CONTENTS))

    adapter = JsonlFile("test.jsonl")
    assert list(adapter.get_data({"index": Impossible()}, [])) == []
