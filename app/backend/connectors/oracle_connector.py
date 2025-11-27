# backend/connectors/oracle_connector.py
import os
from typing import Dict, Optional, List

import oracledb
import pandas as pd


def _ensure_list(val) -> Optional[List[str]]:
  if val is None:
    return None
  if isinstance(val, list):
    return val
  if isinstance(val, str):
    return [c.strip() for c in val.split(",") if c.strip()]
  raise ValueError(f"Unsupported columns type: {type(val)}")


def _build_select(table: str, columns: Optional[List[str]]) -> str:
  if columns:
    cols_sql = ", ".join(columns)
  else:
    cols_sql = "*"
  return f"SELECT {cols_sql} FROM {table}"


def build_oracle_dsn(cfg: Dict) -> str:
  host = cfg.get("host") or os.getenv("ORACLE_HOST")
  port = cfg.get("port") or int(os.getenv("ORACLE_PORT", 1521))
  service = cfg.get("service") or os.getenv("ORACLE_SERVICE")

  if not host or not service:
    raise ValueError("Oracle config missing 'host' or 'service'.")

  # For thin mode
  return oracledb.makedsn(host, port, service_name=service)


def load_oracle_data(cfg: Dict) -> pd.DataFrame:
  """
  cfg keys (from UI + normalizeSource):
    - host, port, service
    - user, password
    - table (or custom_query)
    - columns: list[str] or comma-separated string
  """
  user = cfg.get("user") or os.getenv("ORACLE_USER")
  password = cfg.get("password") or os.getenv("ORACLE_PASSWORD")

  if not user or not password:
    raise ValueError("Oracle config missing 'user' or 'password'.")

  dsn = build_oracle_dsn(cfg)

  # Thin mode (no instant client required if DB supports it)
  conn = oracledb.connect(user=user, password=password, dsn=dsn)

  table = cfg.get("table")
  custom_query = cfg.get("custom_query")
  columns = _ensure_list(cfg.get("columns"))

  if custom_query:
    query = custom_query
  elif table:
    query = _build_select(table, columns)
  else:
    raise ValueError("Oracle source requires either 'table' or 'custom_query'.")

  df = pd.read_sql(query, conn)
  conn.close()
  return df
