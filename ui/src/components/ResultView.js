import React from "react";

export default function ResultView({ result }) {
  if (!result) return null;
  return (
    <div>
      <h3>Reconciliation Result</h3>
      <pre>{JSON.stringify(result, null, 2)}</pre>
    </div>
  );
}
