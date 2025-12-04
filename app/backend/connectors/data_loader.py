# backend/data_loader.py
from typing import Dict
import uuid
import pandas as pd

from backend.connectors.postgres_connector import load_postgres_data
from backend.connectors.hive_connector import load_hive_data
from backend.connectors.oracle_connector import load_oracle_data
from backend.connectors.file_connector import FileConnector
from backend.connectors.bigquery_connector import BigQueryConnector
from backend.config import settings

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

def materialize_to_bigquery(cfg: Dict, label: str) -> str:
    """
    Ensure the given source is available as a BigQuery table.
    Returns fully-qualified table id: project.dataset.table

    - If type=bigquery -> just returns cfg["table"] / ["table_fqn"]
    - If type=file / oracle / postgres / hive -> loads into recon_staging.<label>_<uuid>
    """
    src_type = (cfg.get("type") or "").lower()

    # Native BigQuery: nothing to load, just return the table id
    if src_type == "bigquery":
        table = cfg.get("table_fqn") or cfg.get("table")
        if not table:
            raise ValueError("BigQuery source missing 'table' or 'table_fqn'")
        return table

    # For all other types we go via DataFrame -> staging dataset
    df = load_source_data(cfg)

    dataset = settings.BQ_STAGING_DATASET  # e.g. "recon_staging"
    if not dataset:
        raise ValueError("BQ_STAGING_DATASET must be configured")

    table_name = f"{label}_{uuid.uuid4().hex[:8]}"

    # --------------------------------------------------------
    # NEW: ensure dataset exists before loading DF
    # --------------------------------------------------------
    bigquery_connector.ensure_dataset(dataset)

    # Upload DataFrame into auto-created staging table
    table_fqn = bigquery_connector.load_dataframe_to_table(
        df, dataset, table_name
    )

    cfg["table_fqn"] = table_fqn
    return table_fqn

