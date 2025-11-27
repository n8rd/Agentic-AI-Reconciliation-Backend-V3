# backend/connectors/file_connector.py

import os
import pandas as pd
import fastavro


class FileConnector:
    def load(self, cfg: dict) -> pd.DataFrame:
        """
        cfg structure:
            {
              "type": "file",
              "path": "/tmp/recon_uploads/fileA.csv",
              "format": "csv" | "json" | "parquet" | "avro" | "xlsx" | None,
              "lines": True/False    # optional for JSON
            }

        If 'format' is provided explicitly (via UI upload), it takes precedence
        over file extension.
        """

        path = cfg["path"]
        explicit_format = (cfg.get("format") or "").lower()
        json_lines = cfg.get("lines", False)

        # 1. Determine format: explicit > file extension
        if explicit_format:
            fmt = explicit_format
        else:
            # infer from path extension
            _, ext = os.path.splitext(path)
            fmt = ext.replace(".", "").lower()

        # -------------------------------
        # 2. Supported formats
        # -------------------------------

        # CSV
        if fmt in ["csv"]:
            return pd.read_csv(path)

        # JSON
        if fmt in ["json"]:
            return pd.read_json(path, lines=json_lines)

        # Parquet
        if fmt in ["parquet", "pq"]:
            return pd.read_parquet(path)

        # Avro
        if fmt in ["avro"]:
            with open(path, "rb") as f:
                reader = fastavro.reader(f)
                return pd.DataFrame(list(reader))

        # Excel
        if fmt in ["xlsx", "xls", "excel"]:
            return pd.read_excel(path)

        # If unsupported
        raise ValueError(f"Unsupported file format '{fmt}' for path={path}")
