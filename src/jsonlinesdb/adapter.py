from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import jsonlines
from shillelagh.adapters.base import Adapter
from shillelagh.adapters.file.csvfile import RowTracker
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field
from shillelagh.filters import (
    Equal,
    Filter,
    IsNotNull,
    IsNull,
    NotEqual,
    Range,
)
from shillelagh.lib import RowIDManager, analyze, filter_data
from shillelagh.typing import RequestedOrder, Row


class JsonlFile(Adapter):
    safe = False

    supports_limit = True
    supports_offset = True

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        path = Path(uri)
        return path.suffix == ".jsonl" and path.exists()

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str]:
        return (uri,)

    def __init__(self, path: str):
        super().__init__()

        self.path = Path(path)

        print(f"Opening JSONL file {self.path} to load metadata")
        with jsonlines.open(self.path) as reader:
            data = list(reader)
        try:
            first_row = data[0]
        except IndexError as ex:
            raise ProgrammingError("The file has no rows") from ex
        column_names = first_row.keys()

        row_tracker = RowTracker(data)
        num_rows, order, types = analyze(row_tracker)
        print(f"Read {num_rows} rows")

        self.columns = {
            column_name: types[column_name](
                filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
                order=order[column_name],
                exact=True,
            )
            for column_name in column_names
        }

        self.row_id_manager = RowIDManager([range(0, num_rows)])

        self.last_row = row_tracker.last_row
        self.num_rows = num_rows

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        print(f"Opening JSONL file {self.path} to load metadata")
        with jsonlines.open(self.path) as reader:
            data = (
                {"rowid": i, **row}
                for i, row in zip(self.row_id_manager, reader)
                if i != -1
            )

            for row in filter_data(data, bounds, order, limit, offset):
                print(row)
                yield row
