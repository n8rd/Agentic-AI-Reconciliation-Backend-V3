import os
from typing import Dict, Optional

import pandas as pd
from pyhive import hive


def _build_select(table: str, columns: Optional[list]) -> str:
    if columns:
        cols_sql = ", ".join(columns)
    else:
        cols_sql = "*"
    return f"SELECT {cols_sql} FROM {table}"


def load_hive_data(cfg: Dict) -> pd.DataFrame:
    host = cfg.get("host") or os.getenv("HIVE_HOST")
    port = int(cfg.get("port") or os.getenv("HIVE_PORT", 10000))
    username = cfg.get("user") or os.getenv("HIVE_USERNAME")
    database = cfg.get("database") or os.getenv("HIVE_DATABASE", "default")

    if not host:
        raise ValueError("Hive config missing 'host'.")

    conn = hive.Connection(
        host=host,
        port=port,
        username=username,
        database=database,
    )

    table = cfg.get("table")
    custom_query = cfg.get("custom_query")
    columns = cfg.get("columns")

    if custom_query:
        query = custom_query
    elif table:
        query = _build_select(table, columns)
    else:
        raise ValueError("Hive source requires either 'table' or 'custom_query'.")

    df = pd.read_sql(query, conn)
    conn.close()
    return df
