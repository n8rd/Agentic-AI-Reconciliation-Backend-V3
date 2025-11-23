import React from "react";

const SOURCE_TYPES = [
  { value: "file", label: "File (CSV/JSON/Parquet/Avro)" },
  { value: "bigquery", label: "BigQuery Table" },
  { value: "oracle", label: "Oracle" },
  { value: "postgres", label: "Postgres" },
  { value: "hive", label: "Hive" },
];

export default function SourceConfigForm({ label, value, onChange }) {
  const handleFieldChange = (field, fieldValue) => {
    onChange({ ...value, [field]: fieldValue });
  };

  const handleTypeChange = (e) => {
    const type = e.target.value;
    // Reset to a sane default for that type
    let base = { type };
    if (type === "file") {
      base = { type: "file", path: "" };
    } else if (type === "bigquery") {
      base = {
        type: "bigquery",
        table: "",
        table_fqn: "",
        numeric_cols: "",
        array_cols: "",
      };
    } else if (type === "oracle") {
      base = {
        type: "oracle",
        host: "",
        port: 1521,
        service: "",
        user: "",
        password: "",
        table: "",
        columns: "",
      };
    } else if (type === "postgres") {
      base = {
        type: "postgres",
        host: "",
        port: 5432,
        database: "",
        user: "",
        password: "",
        table: "",
        columns: "",
      };
    } else if (type === "hive") {
      base = {
        type: "hive",
        host: "",
        port: 10000,
        database: "default",
        user: "",
        table: "",
        columns: "",
      };
    }
    onChange(base);
  };

  const renderFieldsForType = () => {
    if (!value?.type) return null;
    const t = value.type;

    if (t === "file") {
      return (
        <>
          <label>File path (local / GCS / HDFS)</label>
          <input
            type="text"
            value={value.path || ""}
            onChange={(e) => handleFieldChange("path", e.target.value)}
            placeholder="e.g. gs://bucket/trades_a.parquet"
          />
        </>
      );
    }

    if (t === "bigquery") {
      return (
        <>
          <label>BigQuery table (dataset.table or project.dataset.table)</label>
          <input
            type="text"
            value={value.table || ""}
            onChange={(e) => {
              handleFieldChange("table", e.target.value);
              handleFieldChange("table_fqn", e.target.value);
            }}
            placeholder="project.dataset.table"
          />
          <label>Numeric columns (comma-separated)</label>
          <input
            type="text"
            value={value.numeric_cols || ""}
            onChange={(e) => handleFieldChange("numeric_cols", e.target.value)}
            placeholder="amount, notional, pnl"
          />
          <label>Array columns (comma-separated)</label>
          <input
            type="text"
            value={value.array_cols || ""}
            onChange={(e) => handleFieldChange("array_cols", e.target.value)}
            placeholder="tags, labels"
          />
        </>
      );
    }

    if (t === "oracle" || t === "postgres" || t === "hive") {
      return (
        <>
          <label>Host</label>
          <input
            type="text"
            value={value.host || ""}
            onChange={(e) => handleFieldChange("host", e.target.value)}
            placeholder="db.host.internal"
          />

          <label>Port</label>
          <input
            type="number"
            value={value.port || ""}
            onChange={(e) => handleFieldChange("port", Number(e.target.value))}
          />

          {t === "postgres" && (
            <>
              <label>Database</label>
              <input
                type="text"
                value={value.database || ""}
                onChange={(e) =>
                  handleFieldChange("database", e.target.value)
                }
              />
            </>
          )}

          {t === "oracle" && (
            <>
              <label>Service name</label>
              <input
                type="text"
                value={value.service || ""}
                onChange={(e) => handleFieldChange("service", e.target.value)}
              />
            </>
          )}

          {t === "hive" && (
            <>
              <label>Database</label>
              <input
                type="text"
                value={value.database || ""}
                onChange={(e) =>
                  handleFieldChange("database", e.target.value)
                }
              />
            </>
          )}

          <label>User</label>
          <input
            type="text"
            value={value.user || ""}
            onChange={(e) => handleFieldChange("user", e.target.value)}
          />

          <label>Password</label>
          <input
            type="password"
            value={value.password || ""}
            onChange={(e) => handleFieldChange("password", e.target.value)}
          />

          <label>Table</label>
          <input
            type="text"
            value={value.table || ""}
            onChange={(e) => handleFieldChange("table", e.target.value)}
            placeholder="TRADES or schema.TRADES"
          />

          <label>Columns (optional, comma-separated)</label>
          <input
            type="text"
            value={value.columns || ""}
            onChange={(e) => handleFieldChange("columns", e.target.value)}
            placeholder="trade_id, amount, trade_date"
          />
        </>
      );
    }

    return null;
  };

  return (
    <div style={{ border: "1px solid #ccc", padding: 12, borderRadius: 8, marginBottom: 16 }}>
      <h4>{label}</h4>
      <label>Source Type</label>
      <select value={value?.type || ""} onChange={handleTypeChange}>
        <option value="">Select source type</option>
        {SOURCE_TYPES.map((s) => (
          <option key={s.value} value={s.value}>
            {s.label}
          </option>
        ))}
      </select>

      <div style={{ marginTop: 8, display: "flex", flexDirection: "column", gap: 8 }}>
        {renderFieldsForType()}
      </div>
    </div>
  );
}
