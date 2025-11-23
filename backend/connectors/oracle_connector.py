import pandas as pd

try:
    import cx_Oracle
except Exception:
    cx_Oracle = None

class OracleConnector:
    def load(self, cfg: dict) -> pd.DataFrame:
        if cx_Oracle is None:
            raise RuntimeError("cx_Oracle not installed")
        dsn = cx_Oracle.makedsn(cfg["host"], cfg["port"], service_name=cfg["service"])
        conn = cx_Oracle.connect(cfg["user"], cfg["password"], dsn)
        cols = cfg.get("columns", ["*"])
        sql = f"SELECT {', '.join(cols)} FROM {cfg['table']}"
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
