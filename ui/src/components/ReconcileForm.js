import React, { useState } from "react";
import { callRecon } from "../api";
import SourceConfigForm from "./SourceConfigForm";

export default function ReconcileForm({ setResult }) {
  const [sourceA, setSourceA] = useState({ type: "" });
  const [sourceB, setSourceB] = useState({ type: "" });

  const [absThreshold, setAbsThreshold] = useState(0.01);
  const [relThreshold, setRelThreshold] = useState(0.001);

  const handleSubmit = async () => {
    if (!sourceA.type || !sourceB.type) {
      alert("Please select source type for both Dataset A and Dataset B");
      return;
    }

    // Normalize comma-separated fields into arrays where backend expects them
    const normalizeSource = (src) => {
      const copy = { ...src };

      if (copy.columns && typeof copy.columns === "string") {
        copy.columns = copy.columns
          .split(",")
          .map((c) => c.trim())
          .filter(Boolean);
      }

      if (copy.numeric_cols && typeof copy.numeric_cols === "string") {
        copy.numeric_cols = copy.numeric_cols
          .split(",")
          .map((c) => c.trim())
          .filter(Boolean);
      }

      if (copy.array_cols && typeof copy.array_cols === "string") {
        copy.array_cols = copy.array_cols
          .split(",")
          .map((c) => c.trim())
          .filter(Boolean);
      }

      return copy;
    };

    const payload = {
      dataset_a: normalizeSource(sourceA),
      dataset_b: normalizeSource(sourceB),
      thresholds: {
        abs: Number(absThreshold),
        rel: Number(relThreshold),
      },
      // Entities are optional; can be added later in the UI
      entities: [],
    };

    const res = await callRecon(payload);
    setResult(res);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16, marginBottom: 24 }}>
      <SourceConfigForm label="Dataset A" value={sourceA} onChange={setSourceA} />
      <SourceConfigForm label="Dataset B" value={sourceB} onChange={setSourceB} />

      <div style={{ display: "flex", gap: 16, alignItems: "center" }}>
        <div>
          <label>Absolute difference threshold</label>
          <input
            type="number"
            step="0.0001"
            value={absThreshold}
            onChange={(e) => setAbsThreshold(e.target.value)}
          />
        </div>
        <div>
          <label>Relative difference threshold</label>
          <input
            type="number"
            step="0.0001"
            value={relThreshold}
            onChange={(e) => setRelThreshold(e.target.value)}
          />
        </div>
      </div>

      <button onClick={handleSubmit}>Run Reconciliation</button>
    </div>
  );
}
