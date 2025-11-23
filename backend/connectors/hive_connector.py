import pandas as pd

try:
    from pyhive import hive
except Exception:
    hive = None

class HiveConnector:
    def load(self, cfg: dict) -> pd.DataFrame:
        if hive is None:
            raise RuntimeError("pyhive not installed")
        conn = hive.Connection(
            host=cfg["host"],
            port=cfg.get("port", 10000),
            username=cfg.get("user"),
            database=cfg.get("database", "default"),
        )
        cols = cfg.get("columns", ["*"])
        sql = f"SELECT {', '.join(cols)} FROM {cfg['table']}"
        df = pd.read_sql(sql, conn)
        return df
