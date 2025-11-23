import pandas as pd

try:
    from google.cloud import bigquery
except Exception:
    bigquery = None

class BigQueryConnector:
    def __init__(self, project_id: str | None = None):
        self.project_id = project_id

    def load(self, cfg: dict) -> pd.DataFrame:
        if bigquery is None:
            raise RuntimeError("google-cloud-bigquery not installed")
        client = bigquery.Client(project=self.project_id)
        table = cfg["table"]  # fully qualified `project.dataset.table` or `dataset.table`
        cols = cfg.get("columns", ["*"])
        sql = f"SELECT {', '.join(cols)} FROM `{table}`"
        df = client.query(sql).to_dataframe()
        return df
