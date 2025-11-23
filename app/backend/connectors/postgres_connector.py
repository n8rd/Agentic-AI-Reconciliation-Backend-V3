import pandas as pd

try:
    import psycopg2
except Exception:
    psycopg2 = None

class PostgresConnector:
    def load(self, cfg: dict) -> pd.DataFrame:
        if psycopg2 is None:
            raise RuntimeError("psycopg2 not installed")
        conn = psycopg2.connect(
            dbname=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
            host=cfg["host"],
            port=cfg.get("port", 5432),
        )
        cols = cfg.get("columns", ["*"])
        sql = f"SELECT {', '.join(cols)} FROM {cfg['table']}"
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
