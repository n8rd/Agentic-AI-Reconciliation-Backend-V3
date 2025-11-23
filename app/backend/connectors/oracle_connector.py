import pandas as pd
import oracledb  # make sure `python-oracledb` is in requirements.txt


class OracleConnector:
    def load(self, cfg: dict) -> pd.DataFrame:
        """
        cfg keys expected:
          host, port, service, user, password, table
          optional: columns (list of column names)
        """

        # Thin mode connection – no Oracle client / Instant Client required
        conn = oracledb.connect(
            user=cfg["user"],
            password=cfg["password"],
            host=cfg["host"],
            port=cfg["port"],
            service_name=cfg["service"],
        )

        try:
            cols = cfg.get("columns", ["*"])
            col_expr = ", ".join(cols)
            sql = f"SELECT {col_expr} FROM {cfg['table']}"

            df = pd.read_sql(sql, conn)
        finally:
            conn.close()

        return df
