import pandas as pd
from typing import Optional
try:
    from google.cloud import bigquery
except Exception:
    bigquery = None


def _ensure_list(val):
    """
    Accept:
      - None           -> []
      - list           -> same list
      - comma string   -> ["col1", "col2"]
      - anything else  -> []
    """
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        return [c.strip() for c in val.split(",") if c.strip()]
    return []


class BigQueryConnector:
    def __init__(self, project_id: Optional[str] = None):
        self.project_id = project_id

    def _client(self):
        if bigquery is None:
            raise RuntimeError("google-cloud-bigquery not installed")
        return bigquery.Client(project=self.project_id)

    def load(self, cfg: dict) -> pd.DataFrame:
        """
        Load from an existing BigQuery table (no change from your current behavior).
        """
        client = self._client()

        table = cfg.get("table_fqn") or cfg.get("table")
        if not table:
            raise ValueError("BigQuery cfg missing 'table' or 'table_fqn'.")

        cols_cfg = cfg.get("columns")
        cols = _ensure_list(cols_cfg)

        if not cols:
            sql = f"SELECT * FROM `{table}`"
        else:
            sql = f"SELECT {', '.join(cols)} FROM `{table}`"

        df = client.query(sql).to_dataframe()
        return df

    def load_dataframe_to_table(self, df: pd.DataFrame, dataset: str, table: str) -> str:
        """
        Upload a pandas DataFrame into BigQuery as dataset.table.
        Returns fully-qualified table id.
        """
        client = self._client()
        table_id = f"{client.project}.{dataset}.{table}"

        job = client.load_table_from_dataframe(df, table_id)
        job.result()  # wait for load to finish

        return table_id