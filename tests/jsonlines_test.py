from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import mock_open

import apsw
import pytest
from freezegun import freeze_time
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture
from shillelagh.backends.apsw.vt import VTModule
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
from shillelagh.lib import serialize

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


def test_jsonlfile(fs: FakeFilesystem):
    with open("test.jsonl", "w", encoding="utf-8") as fp:
        fp.write(CONTENTS)

    connection = apsw.Connection(":memory:")
    cursor = connection.cursor()
    connection.createmodule("jsonlfile", VTModule(JsonlFile))
    cursor.execute(
        f"""CREATE VIRTUAL TABLE test USING jsonlfile('{serialize('test.jsonl')}')"""  # noqa: E501
    )

    sql = 'SELECT * FROM test WHERE "index" > 11'
    data = list(cursor.execute(sql))
    assert data == [("12", 13.3, "Platinum_St"), ("13", 12.1, "Kodiak_Trail")]


def test_jsonlfile_close_not_modified(fs: FakeFilesystem):
    """Test closing the file when it hasn't been modified."""
    with freeze_time("2022-01-01T00:00:00Z"):
        with open("test.jsonl", "w", encoding="utf-8") as fp:
            fp.write(CONTENTS)

    connection = apsw.Connection(":memory:")
    cursor = connection.cursor()
    connection.createmodule("jsonlfile", VTModule(JsonlFile))
    cursor.execute(
        f"""CREATE VIRTUAL TABLE test USING jsonlfile('{serialize('test.jsonl')}')"""  # noqa: E501
    )

    sql = 'SELECT * FROM test WHERE "index" > 11'
    data = list(cursor.execute(sql))
    assert data == [("12", 13.3, "Platinum_St"), ("13", 12.1, "Kodiak_Trail")]

    with freeze_time("2022-01-02T00:00:00Z"):
        connection.close()

    path = Path("test.jsonl")
    assert (
        path.stat().st_mtime
        == datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp()
    )
