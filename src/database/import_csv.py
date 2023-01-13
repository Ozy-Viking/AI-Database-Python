#!/bin/env python3.11
"""
Import data from csv files.
"""
from __future__ import annotations

from glob import glob
from pathlib import Path
from typing import Protocol

from .table import DBTable


def gather_csv_paths(data_folder: str | Path) -> dict[str, DBTable]:
    """
    Only gathers csv files with the following format:
    (database name)_(table name).csv

    Args:
        data_folder: folder which contains the databases.

    Returns:
        dict[str, DBTable]: All tables with associated connections and cursors.
    """
    import_dict: dict[str, DBTable] = dict()
    for csv in glob(f"{data_folder}/*_*.csv"):
        path = Path(csv)
        table_name: str
        database_name: str
        total_name = path.stem
        (
            database_name,
            table_name,
        ) = total_name.split("_")
        import_dict[total_name] = DBTable(
            name=table_name, db_name=database_name, csv_path=path
        )
    return import_dict
