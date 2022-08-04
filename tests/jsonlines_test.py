from unittest.mock import mock_open

from pytest_mock import MockerFixture
from shillelagh.fields import Float, Integer, Order, String
from shillelagh.filters import Equal, IsNotNull, IsNull, NotEqual, Range

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
