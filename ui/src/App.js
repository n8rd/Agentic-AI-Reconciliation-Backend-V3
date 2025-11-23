import React, { useState } from "react";
import ReconcileForm from "./components/ReconcileForm";
import ResultView from "./components/ResultView";

export default function App() {
  const [result, setResult] = useState(null);

  return (
    <div style={{ padding: 20 }}>
      <h2>Agentic AI Reconciliation v3</h2>
      <ReconcileForm setResult={setResult} />
      <ResultView result={result} />
    </div>
  );
}
