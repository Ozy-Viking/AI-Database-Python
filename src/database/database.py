"""
Database control modules
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from sqlite3 import Connection, Cursor
from typing import Optional, Protocol

import pandas as pd
from icecream import ic


class NormaliseMap(Protocol):
    """
    Protocol for normalizing tables.
    """

    def __call__(self, value: str, column: str) -> int:
        ...


class DBTable(Protocol):
    """
    Table Protocol.
    """

    name: str
    db_name: str
    csv_path: Path
    database: Database
    dataframe: pd.DataFrame
    index_label: str

    def normalise_data(self, data_map: NormaliseMap) -> DBTable:
        """
        Normalise a dataframe for analysis.

        Args:
            data_map: Map to normalise data.

        Returns:
            DBTable: Self

        """
        ...

    def csv_to_dataframe(self, separator: str = ";") -> DBTable:
        """
        Imports the CSV to a DataFrame.

        Args:
            separator: separator used by the CSV. Default is ";"

        Returns:
            DBTable: Self
        """
        ...

    def dataframe_to_csv(self, separator: str = ";") -> DBTable:
        """
        Export DataFrame to a CSV.

        Args:
            separator: separator used by the CSV. Default is ";"

        Returns:
            DBTable: Self
        """
        ...

    def modify_index(self) -> DBTable:
        """
        Modifies the index of the dataframe.

        Returns:
            self
        """
        ...

    def add(self) -> DBTable:
        ...


class Database:
    """
    SQL Lite Database with tables.
    """

    def __init__(
            self, name: str, folder: Optional[str | Path] = None, if_exists: str = "replace"
    ):
        self.name: str = name
        self.tables: list[DBTable] = list()
        self.folder = folder
        if folder is None:
            self.folder = Path("../data/").resolve()
        if not self.folder.is_dir():
            try:
                self.folder.mkdir(parents=True)
            except FileExistsError:
                ...
        self.path: Path = Path(self.folder, f"{name}.db")
        self.connection: Connection | None = None
        self.cursor: Cursor | None = None
        self.if_exists: str = if_exists
        self.connect()

    def __repr__(self):
        return f"Database(name='{self.name}', tables={self.table_names})"

    # def __del__(self):
    #     if hasattr(self, "connection"):
    #         self.connection.close()

    def disconnect(self):
        """
        Disconnect from sqlite database.
        """
        self.connection.close()

    def connect(self) -> Database:
        """
        Connect to the database. Sets the connection and cursor.

        Returns:
            Database: self
        """
        self.connection = sqlite3.Connection(self.path)
        self.cursor = self.connection.cursor()
        return self

    @property
    def table_names(self):
        """
        Returns:
            List of Table names.
        """
        return [table.name for table in self.tables]

    def add_table(self, table: DBTable) -> Database:
        """
        Table can only be added to one database.

        Args:
            table:

        Returns:
            Self

        """
        table.database = self
        table.db_name = self.name
        self.tables.append(table)
        if hasattr(self, table.name):
            ic(f"changing {table.name} to new value.")
        self.__setattr__(table.name, table)
        self.add_table_to_database(table)

        return self

    def add_tables(self, *tables: DBTable) -> Database:
        """
        Add multiple tables at once.

        Args:
            *tables: tuple[DBTable, ...]: tables to add.

        Returns:
            None
        """
        for table in tables:
            self.add_table(table)
        return self

    def show_tables(self):
        """
        Show tables in SQLite database.

        Returns:
            list[
        """
        command = self.cursor.execute(
            "SELECT name FROM sqlite_schema WHERE type ='table'"
            "AND name NOT LIKE 'sqlite_%';"
        )

        return tuple(table[0] for table in command)

    def add_table_to_database(self, table: DBTable) -> Database:
        """
        Adds table to sql database.

        Args:
            table:

        Returns:
            Database: self
        """
        table.dataframe.to_sql(
            name=table.name,
            con=self.connection,
            if_exists=self.if_exists,
            index_label=table.index_label,
        )
        return self


class DatabaseGenerator:
    """
    Generates Database objects from different inputs.
    """

    @classmethod
    def create_from_dict(cls, db_dict: dict[str, DBTable]) -> dict[str, Database]:
        """
        Creates all database objects from a dictionary of tables.

        Args:
            db_dict: a dictionary of [table_name: table]

        Returns:
            dict[str, Database]: dictionary of [database_name: database]
        """
        return_dict: dict[str, Database] = dict()
        for table in db_dict.values():
            if table.db_name in return_dict.keys():
                table.database = return_dict[table.db_name]

            else:
                db_name = table.db_name
                folder = Path(db_dict.copy().popitem()[1].csv_path).resolve().parent
                db = Database(name=db_name, folder=folder)
                return_dict[db.name] = db
                table.database = db
            table.add()
        return return_dict
