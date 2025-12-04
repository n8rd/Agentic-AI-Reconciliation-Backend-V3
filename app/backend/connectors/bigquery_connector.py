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
    """
    Unified BigQuery connector used by:
      - materialize_sources (table load)
      - node_sql / node_exec (recon SQL execution)
      - file upload loaders (load_dataframe_to_table)
    """

    def __init__(self, project_id: Optional[str] = None):
        """
        project_id:
          - None → default project from Cloud Run service account
          - string → explicit project
        """
        self.project_id = project_id
        self.client = None   # lazy init

    # -----------------------------------------------------
    # Lazy BigQuery client creation
    # -----------------------------------------------------
    def _client(self):
        if self.client is not None:
            return self.client

        if bigquery is None:
            raise RuntimeError("google-cloud-bigquery not installed")

        self.client = bigquery.Client(project=self.project_id)
        return self.client

    # -----------------------------------------------------
    # Core execution used by node_exec
    # -----------------------------------------------------
    def run_query(self, query: str) -> pd.DataFrame:
        client = self._client()
        job = client.query(query)
        return job.result().to_dataframe()

    # Backwards compatible alias
    def run_query_to_df(self, query: str) -> pd.DataFrame:
        return self.run_query(query)

    # -----------------------------------------------------
    # Load an existing BigQuery table into a DataFrame
    # -----------------------------------------------------
    def load(self, cfg: dict) -> pd.DataFrame:
        """
        Load selected columns from an existing BigQuery table.
        cfg = {
            "table_fqn": "project.dataset.table",
            "columns": ["colA", "colB"]
        }
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

        return client.query(sql).to_dataframe()

    # -----------------------------------------------------
    # Upload a DataFrame to BigQuery (used for file sources)
    # -----------------------------------------------------
    def load_dataframe_to_table(self, df: pd.DataFrame, dataset: str, table: str) -> str:
        """
        Upload a pandas DataFrame into BigQuery:
            RETURN: "project.dataset.table"
        """
        client = self._client()

        project = client.project
        table_id = f"{project}.{dataset}.{table}"

        load_job = client.load_table_from_dataframe(df, table_id)
        load_job.result()  # wait for load completion

        return table_id

    def ensure_dataset(self, dataset: str):
        """
        Create the dataset if it doesn't exist.
        """
        client = self._client()
        try:
            client.get_dataset(dataset)
        except Exception:
            logger.info("[BigQueryConnector] Creating dataset: %s", dataset)
            client.create_dataset(dataset)

    def ensure_table(self, dataset: str, table: str, schema=None):
        """
        Create table if missing. schema = list[bigquery.SchemaField]
        """
        client = self._client()
        table_id = f"{client.project}.{dataset}.{table}"

        try:
            client.get_table(table_id)
            return table_id
        except Exception:
            pass

        logger.info("[BigQueryConnector] Creating table: %s", table_id)
        table_obj = bigquery.Table(table_id, schema=schema)
        client.create_table(table_obj)
        return table_id

