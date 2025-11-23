import pandas as pd
import fastavro
import io
import os

class FileConnector:
    def load(self, cfg: dict) -> pd.DataFrame:
        path = cfg["path"]

        if path.endswith(".csv"):
            return pd.read_csv(path)

        if path.endswith(".json"):
            return pd.read_json(path, lines=cfg.get("lines", False))

        if path.endswith(".parquet"):
            return pd.read_parquet(path)

        if path.endswith(".avro"):
            with open(path, "rb") as f:
                reader = fastavro.reader(f)
                return pd.DataFrame(list(reader))

        raise ValueError(f"Unsupported file type for {path}")
