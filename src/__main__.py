#!/bin/env python3.11
from __future__ import annotations

from pathlib import Path

from icecream import ic

from src.database.database import DatabaseGenerator
from src.database.import_csv import gather_csv_paths


def students_str_map(value: str, column: str) -> int | str:
    """
    Map for normalising the data.
    todo: implement import map from file, either toml or yaml.

    Args:
        value: str: Value to Map
        column: str: Column Name

    Returns:
        int: value according to map

    """
    if not isinstance(value, str):
        return value  # type: ignore
    match value.upper():
        case "YES":
            return 1
        case "NO":
            return 0
        case "OTHER":
            if column == "reason":
                return 3
            elif column == "guardian":
                return 2
            elif column in {"Mjob", "Fjob"}:
                return 4
        case "GP":
            return 0
        case "MS":
            return 1
        case "F":
            return 0
        case "M":
            return 1
        case "U":
            return 0
        case "R":
            return 1
        case "LE3":
            return 0
        case "GT3":
            return 1
        case "T":
            return 0
        case "A":
            return 1
        case "TEACHER":
            return 0
        case "HEALTH":
            return 1
        case "SERVICES":
            return 2
        case "AT_HOME":
            return 3
        case "HOME":
            return 0
        case "REPUTATION":
            return 1
        case "COURSE":
            return 2
        case "MOTHER":
            return 0
        case "FATHER":
            return 1
        case _:
            return value


def prepare_tables(table_dict):
    """
    Prepares the tables for adding to the database.
    Args:
        table_dict:
    """
    table_dict["students_description"].index_label = "DID"
    table_dict["students_math"].index_label = "MUID"
    table_dict["students_portuguese"].index_label = "PUID"
    [tab.modify_index() for tab in table_dict.values()]
    [tab.dataframe_to_csv() for tab in table_dict.values()]
    ic(table_dict["students_description"].dataframe)
    # [tab.normalise_data(students_str_map) for tab in table_dict.values()]


def main() -> int:
    """

    Returns:
        int

    """
    db_folder: Path = Path("data/").resolve()

    import_csv = gather_csv_paths(db_folder)
    ic(import_csv)
    prepare_tables(import_csv)
    db_dict = DatabaseGenerator.create_from_dict(import_csv)
    database = db_dict["students"]

    return 0


if __name__ == "__main__":
    exit(main())
