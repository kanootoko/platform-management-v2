"""SQLite database helper methods are located here."""

import json
import sqlite3
from typing import Any

from pmv2.logic.utils import try_load_json


class SQLiteHelper:
    """No syncronization is used because all access is performed in a single process,
    with no asyncronous calls within methods.
    """

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def execute(self, query: str, parameters: list[Any] | None = None) -> None:
        """Execute the given query with parameters which are passed to sqlite conn.execute as-is."""
        if parameters is None:
            self._conn.execute(query)
        else:
            self._conn.execute(query, parameters)

    def update(self, table: str, where: str, non_quoted_set: str | None = None, **data: Any) -> None:
        """Perform an update query for a given `table` and `where` condition to set `data` with values
        passed in a safe way. `non_quoted_set` can be used to set with expression, for example "value = value + 1".
        """
        columns = list(map(lambda name: f'"{name}"', data.keys()))
        set_string = ", ".join(f"{column} = ?" for column in columns)
        values = [
            (json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else value)
            for value in data.values()
        ]
        if non_quoted_set is not None:
            set_string += f", {non_quoted_set}"

        with self._conn:
            cur = self._conn.cursor()
            cur.execute(
                f'UPDATE "{table}" SET {set_string} WHERE {where}',
                values,
            )

    def insert(self, table: str, returning: str | None, **data: Any) -> Any:
        """Perform an insert query into a given `table` with `data` passed in a safe way. Optional `returning`
        statement to return value(s) generated in the database on insert.
        """
        columns = list(map(lambda name: f'"{name}"', data.keys()))
        placeholders = ("?",) * len(columns)
        values = [
            (json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else value)
            for value in data.values()
        ]
        returning_string = f" RETURNING {returning}" if returning is not None else ""

        with self._conn:
            cur = self._conn.cursor()
            cur.execute(
                f'INSERT INTO "{table}" ({", ".join(columns)}) VALUES ({", ".join(placeholders)}){returning_string}',
                values,
            )
            if returning is not None:
                return cur.fetchone()[0]
            return None

    def insert_many(
        self, table: str, data: list[dict[str, Any]], returning: str | None, columns: list[str] | None = None
    ) -> list[int]:
        """Same as `insert`, but inserts many values in one transaction. `returning` values come in the same order
        as `data` entries are. `columns` are optional and calculated as a union fo all `data` keys if not set.
        """
        if len(data) == 0:
            return []
        if columns is None:
            columns_set = set(data[0].keys())
            for d in data[1:]:
                columns_set.update(set(d.keys()))
            columns = sorted(columns_set)
        placeholders = ("?",) * len(columns)
        returning_string = f" RETURNING {returning}" if returning is not None else ""

        results = [0] * len(data)
        with self._conn:
            cur = self._conn.cursor()

            for i, row in enumerate(data):
                insert_data = tuple(
                    (
                        json.dumps(row.get(key), ensure_ascii=False)
                        if isinstance(row.get(key), (list, dict))
                        else row.get(key)
                    )
                    for key in columns
                )
                cur.execute(
                    f'INSERT INTO "{table}" ({", ".join(columns)})'
                    f' VALUES ({", ".join(placeholders)}){returning_string}',
                    insert_data,
                )
                if returning is not None:
                    results[i] = cur.fetchone()[0]
        return results

    def select(  # pylint: disable=too-many-arguments
        self,
        table: str,
        columns: list[str],
        where: str,
        *,
        no_quote: bool = False,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Perform a select query for a given `table` selecting `columns` with `where` condition with optional
        `order_by` and `limit` expressions. `no_quote` allows to pass expressions to `columns`, for example:
        `["a", "time('now')"]`."""
        if no_quote:
            columns_quoted = list(map(lambda name: f'"{name}"', columns))
        else:
            columns_quoted = columns
        limit_string = f" LIMIT {limit}" if limit is not None else ""
        order_by_string = f" ORDER BY {order_by}" if order_by is not None else ""

        results: list[dict] = []
        cur = self._conn.cursor()
        with self._conn:
            query = f'SELECT {", ".join(columns_quoted)} FROM {table} WHERE {where}{order_by_string}{limit_string}'
            cur.execute(query)
            for entry in cur.fetchall():
                results.append({column: try_load_json(value) for column, value in zip(columns, entry)})

        return results
