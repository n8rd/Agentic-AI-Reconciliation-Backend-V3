import pandas as pd
from backend.connectors.oracle_connector import OracleConnector
from backend.connectors.postgres_connector import PostgresConnector
from backend.connectors.hive_connector import HiveConnector
from backend.connectors.file_connector import FileConnector
from backend.connectors.bigquery_connector import BigQueryConnector
from backend.config import settings

class DataLoader:
    def __init__(self):
        self.registry = {
            "oracle": OracleConnector(),
            "postgres": PostgresConnector(),
            "hive": HiveConnector(),
            "file": FileConnector(),
            "bigquery": BigQueryConnector(settings.google_project_id),
        }

    def load(self, source_cfg: dict) -> pd.DataFrame:
        src_type = source_cfg["type"]
        if src_type not in self.registry:
            raise ValueError(f"Unsupported source type: {src_type}")
        return self.registry[src_type].load(source_cfg)
