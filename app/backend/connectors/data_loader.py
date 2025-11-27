# backend/data_loader.py
from typing import Dict

import pandas as pd

from backend.connectors.postgres_connector import load_postgres_data
from backend.connectors.hive_connector import load_hive_data
from backend.connectors.oracle_connector import load_oracle_data
from backend.connectors.file_connector import FileConnector
from backend.connectors.bigquery_connector import BigQueryConnector

file_connector = FileConnector()
bigquery_connector = BigQueryConnector(project_id=None)  # or your project id


def load_source_data(cfg: Dict) -> pd.DataFrame:
    """
    Dispatch to the right connector based on cfg["type"].

    cfg examples:
      {"type": "file", "path": "...", "format": "csv"}
      {"type": "postgres", "host": "...", "database": "...", "user": "...", ...}
      {"type": "hive", "host": "...", "database": "...", ...}
      {"type": "oracle", "host": "...", "service": "...", ...}
      {"type": "bigquery", "table": "project.dataset.table", ...}
    """
    if cfg is None:
        raise ValueError("Source config is None")

    src_type = (cfg.get("type") or "").lower()

    if src_type == "file":
        return file_connector.load(cfg)
    elif src_type == "postgres":
        return load_postgres_data(cfg)
    elif src_type == "hive":
        return load_hive_data(cfg)
    elif src_type == "oracle":
        return load_oracle_data(cfg)
    elif src_type == "bigquery":
        return bigquery_connector.load(cfg)
    else:
        raise ValueError(f"Unsupported source type: {src_type}")
