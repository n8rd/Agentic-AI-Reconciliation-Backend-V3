import os
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text


def build_postgres_url(cfg: Dict) -> str:
    user = cfg.get("user") or os.getenv("POSTGRES_USER")
    password = cfg.get("password") or os.getenv("POSTGRES_PASSWORD")
    host = cfg.get("host") or os.getenv("POSTGRES_HOST", "localhost")
    port = cfg.get("port") or os.getenv("POSTGRES_PORT", 5432)
    database = cfg.get("database") or os.getenv("POSTGRES_DB")

    if not all([user, password, host, database]):
        raise ValueError("Postgres config is incomplete (user/password/host/database).")

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def _build_select(table: str, columns: Optional[list]) -> str:
    if columns:
        cols_sql = ", ".join(columns)
    else:
        cols_sql = "*"
    return f"SELECT {cols_sql} FROM {table}"


def load_postgres_data(cfg: Dict) -> pd.DataFrame:
    url = build_postgres_url(cfg)
    engine = create_engine(url)

    table = cfg.get("table")
    custom_query = cfg.get("custom_query")
    columns = cfg.get("columns")  # this will already be a list after frontend normalization

    if custom_query:
        query = text(custom_query)
    elif table:
        query = text(_build_select(table, columns))
    else:
        raise ValueError("Postgres source requires either 'table' or 'custom_query'.")

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    return df
