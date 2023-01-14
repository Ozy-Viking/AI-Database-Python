#!/bin/env python3.11
"""
Table class for sqlite database.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from sqlite3 import Connection, Cursor
from typing import Any, Protocol

import pandas as pd


class NormaliseMap(Protocol):
    """
    Protocol for normalizing tables.
    """

    def __call__(self, value: str, column: str) -> int:
        ...


class Database(Protocol):
    """Database protocol"""

    name: str
    tables: list[DBTable]
    folder: str | Path
    path: Path
    connection: Connection
    cursor: Cursor
    if_exists: str

    def add_table(self, table: DBTable) -> Database:
        """
        Table can only be added to one database.

        Args:
            table:

        Returns:
            Self

        """
        ...


@dataclass(slots=True)
class DBTable:
    """
    Database table.

    todo: implement a SELECT {} FROM {self.name} function.
    """

    name: str
    db_name: str
    index_label: str = field(repr=False, init=False)
    csv_path: Path = field(repr=False)  # init = True!
    dataframe: pd.DataFrame = field(init=False, repr=False)
    database: Database = field(repr=False, default=None)
    index: Any = field(repr=False, default=None)

    def __post_init__(self):
        self.csv_to_dataframe()

    # def __del__(self):
    #     if self.database is not None:
    #         self.database.cursor.execute(f"DROP TABLE IF EXISTS {self.name}")

    def normalise_data(self, data_map: NormaliseMap) -> DBTable:
        """
        Normalise a dataframe for analysis.

        Args:
            data_map: Map to normalise data.

        Returns:
            DBTable: Self

        """
        for column in self.dataframe.columns:
            partial_map = partial(data_map, column=column)
            self.dataframe[column] = self.dataframe[column].map(partial_map)
        return self

    def modify_index(self) -> DBTable:
        """
        Modifies the index of the dataframe.

        Returns:
            self
        """
        if self.index_label in self.dataframe.columns:
            self.dataframe.set_index(self.index_label, inplace=True)
            return self

        elif self.index_label not in self.dataframe.columns and self.index is None:
            self.index = list(range(len(self.dataframe)))

        self.dataframe.insert(0, self.index_label, self.index, allow_duplicates=False)
        self.dataframe.set_index(self.index_label, inplace=True)
        return self

    def csv_to_dataframe(self, separator: str = ";") -> DBTable:
        """
        Imports the CSV to a DataFrame.

        Args:
            separator: separator used by the CSV. Default is ";"

        Returns:
            DBTable: Self
        """
        self.dataframe = pd.read_csv(self.csv_path, sep=separator)
        return self

    def dataframe_to_csv(self, separator: str = ";"):
        """
        Export DataFrame to a CSV.

        Args:
            separator: separator used by the CSV. Default is ";"

        Returns:
            DBTable: Self
        """
        self.dataframe.to_csv(sep=separator, path_or_buf=self.csv_path)
        return self

    def add(self) -> DBTable:
        """
        Add self to database.

        Returns:

        """
        self.database.add_table(self)
        return self
