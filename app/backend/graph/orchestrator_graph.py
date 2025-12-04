from pydantic import BaseModel, ValidationError
from typing import Optional, List, Dict, Any
from langgraph.graph import StateGraph, START, END

from backend.agents.schema_mapper import SchemaMapperAgent
from backend.agents.entity_resolver import EntityResolverAgent
from backend.agents.query_synthesizer import QuerySynthesizerAgent
from backend.agents.explanation_generator import ExplanationGeneratorAgent
from backend.config import settings
from backend.connectors.data_loader import load_source_data
from backend.connectors.bigquery_connector import bigquery, BigQueryConnector
from backend.connectors.data_loader import materialize_to_bigquery


import pandas as pd
from collections.abc import Mapping

import logging
logger = logging.getLogger(__name__)

class ColumnMapping(BaseModel):
    a_col: str
    b_col: str

class Approval(BaseModel):
    approved_matches: List[ColumnMapping]

class ReconState(BaseModel):
    # input configs
    dataset_a: Dict[str, Any] | None = None
    dataset_b: Dict[str, Any] | None = None
    thresholds: Dict[str, Any] = {"abs": 0.01, "rel": 0.001}
    entities: List[str] | None = None
    # "approval": { "approved_matches": [ { "a_col": "...", "b_col": "..." }, ... ] }
    approval: Optional[Approval] = None

    # internal / results
    df_a_sample: Any | None = None
    df_b_sample: Any | None = None
    schema_mapping: Dict[str, Any] | None = None
    entity_res: Dict[str, Any] | None = None
    sql: str | None = None
    bq_status: str | None = None
    explanation: str | None = None
    status: str | None = None

    entity_resolved: Dict[str, Any] | None = None

    # --- NEW: STORE RAW DATAFRAMES FOR JOIN-KEY VALIDATION ---
    data_a: Any | None = None
    data_b: Any | None = None

    # --- NEW: STORE COLUMN LISTS ---
    columns_a: List[str] | None = None
    columns_b: List[str] | None = None

    result_df: Any | None = None     # internal only
    result: Any | None = None        # JSON serializable output

sm = SchemaMapperAgent()
er = EntityResolverAgent()
qs = QuerySynthesizerAgent()
eg = ExplanationGeneratorAgent()

def materialize_sources(state: ReconState) -> ReconState:
    # Dataset A
    if state.dataset_a:
        fqn_a = materialize_to_bigquery(state.dataset_a, "a")
        state.dataset_a["table_fqn"] = fqn_a

    # Dataset B
    if state.dataset_b:
        fqn_b = materialize_to_bigquery(state.dataset_b, "b")
        state.dataset_b["table_fqn"] = fqn_b

    return state

def node_load(state: ReconState) -> ReconState:
    # --- Load Dataset A ---
    if state.dataset_a:
        df_a = load_source_data(state.dataset_a)
        state.data_a = df_a   # keep copy for error reporting
        state.columns_a = df_a.columns.tolist()  # <-- NEW
        # materialize into BigQuery after this, in materialize_sources()

    # --- Load Dataset B ---
    if state.dataset_b:
        df_b = load_source_data(state.dataset_b)
        state.data_b = df_b
        state.columns_b = df_b.columns.tolist()  # <-- NEW

    return state

def node_map(state: ReconState) -> ReconState:
    """
    Run schema mapping to propose column matches.

    Prefer full DataFrames from state (data_a / data_b or df_a / df_b).
    Fall back to df_a_sample / df_b_sample only if full data is missing.
    """

    # ---- pick df_a ----
    df_a = None
    if getattr(state, "data_a", None) is not None:
        df_a = state.data_a
    elif getattr(state, "df_a", None) is not None:
        df_a = state.df_a
    elif getattr(state, "df_a_sample", None) is not None:
        df_a = pd.DataFrame(state.df_a_sample)

    # ---- pick df_b ----
    df_b = None
    if getattr(state, "data_b", None) is not None:
        df_b = state.data_b
    elif getattr(state, "df_b", None) is not None:
        df_b = state.df_b
    elif getattr(state, "df_b_sample", None) is not None:
        df_b = pd.DataFrame(state.df_b_sample)

    # Defensive: if still missing, return empty mapping
    if df_a is None or df_b is None:
        logger.error(
            "node_map: missing data_a/data_b and df_a_sample/df_b_sample; "
            "cannot compute schema mapping."
        )
        state.schema_mapping = {
            "matches": [],
            "numeric_cols": [],
            "array_cols": [],
            "string_cols": [],
        }
        return state

    sm = SchemaMapperAgent()
    state.schema_mapping = sm.run({"df_a": df_a, "df_b": df_b})

    # Columns for UI
    state.columns_a = df_a.columns.tolist()
    state.columns_b = df_b.columns.tolist()

    return state


def node_approval(state: ReconState) -> ReconState:
    """
    HITL checkpoint:
    - First call (/reconcile): no approval yet -> mark PENDING_APPROVAL.
    - Second call (/reconcile/approve): approval present -> filter mappings and mark APPROVED.
    """
    if state.approval is None:
        # No approval from client yet: stop after this node
        state.status = "PENDING_APPROVAL"
        return state

    # We have an Approval object with approved_matches.
    # Filter schema_mapping.matches to only keep approved pairs.
    try:
        approved_pairs = {
            (m.a_col, m.b_col) for m in state.approval.approved_matches
        }
        if state.schema_mapping and "matches" in state.schema_mapping:
            original_matches = state.schema_mapping["matches"]
            state.schema_mapping["matches"] = [
                m
                for m in original_matches
                if (m.get("a_col"), m.get("b_col")) in approved_pairs
            ]
    except Exception as e:
        logger.error("Error applying approval to schema_mapping: %s", e)

    state.status = "APPROVED"
    return state

def decide_after_approval(state: ReconState) -> str:
    """
    Decide what happens after the approval node.

    - When called from /reconcile:
      status will typically be "PENDING_APPROVAL" (or None),
      so we route to "await" and end the graph → UI shows mapping for user approval.

    - When called from /reconcile/approve:
      routes will set status="APPROVED" and include an approval payload,
      so we route to "entity_resolve" to continue the pipeline.
    """

    status = getattr(state, "status", None)

    if status == "APPROVED":
        return "entity_resolve"

    # Default (PENDING_APPROVAL or anything else) → stop at "await"
    return "await"


def node_await(state: ReconState) -> ReconState:
    # Pause here; client inspects mapping and calls again with approval set
    return state

def node_entity_resolve(state: ReconState) -> ReconState:
    """
    Run entity resolution after schema mapping.
    Uses full dataframes plus mapping + thresholds + any existing entities.
    """

    payload = {
        "data_a": state.data_a,
        "data_b": state.data_b,
        "schema_mapping": state.schema_mapping,
        # extra context in case EntityResolverAgent uses it
        "thresholds": getattr(state, "thresholds", None),
        "entities": getattr(state, "entities", []) or [],
        "dataset_a": getattr(state, "dataset_a", None),
        "dataset_b": getattr(state, "dataset_b", None),
    }

    result = er.run(payload)

    # If the agent returns explicit entities, persist them on state
    if isinstance(result, dict) and "entities" in result:
        state.entities = result["entities"]
        state.entity_resolved = result  # keep whole payload if useful
    else:
        state.entity_resolved = result

    # Re-capture columns in case entity resolver changed anything
    if state.data_a is not None:
        state.columns_a = state.data_a.columns.tolist()
    if state.data_b is not None:
        state.columns_b = state.data_b.columns.tolist()

    return state



def node_sql(state: ReconState) -> ReconState:
    """
    Node: SQL Synthesis
    Use QuerySynthesizerAgent to produce the reconciliation SQL.
    Does NOT execute SQL (that's node_exec).
    """

    logger.info("[node_sql] Starting SQL synthesis")

    schema_mapping = state.schema_mapping or {}
    numeric_cols = schema_mapping.get("numeric_cols", [])
    array_cols = schema_mapping.get("array_cols", [])

    table_a = state.dataset_a.get("table_fqn")
    table_b = state.dataset_b.get("table_fqn")

    qs_payload = {
        "schema_mapping": schema_mapping,
        "thresholds": state.thresholds,
        "table_a": table_a,
        "table_b": table_b,
        "numeric_cols": numeric_cols,
        "array_cols": array_cols,
        "columns_a": getattr(state, "columns_a", []),
        "columns_b": getattr(state, "columns_b", []),
        "entities": getattr(state, "entities", []) or [],
        "approval": getattr(state, "approval", None),
    }

    logger.info("[node_sql] Invoking QuerySynthesizerAgent with payload keys: %s",
                list(qs_payload.keys()))

    try:
        result = qs.run(qs_payload)
    except Exception as e:
        logger.error("[node_sql] QuerySynthesizerAgent failed: %s", e, exc_info=True)
        raise

    sql = None
    if isinstance(result, dict):
        sql = result.get("sql")

    if not sql:
        raise RuntimeError("[node_sql] Synthesized SQL is empty!")

    logger.info("[node_sql] SQL synthesis complete (first 200 chars):\n%s",
                sql[:200])

    state.sql = sql
    return state

def decide_after_sql(state: ReconState) -> str:
    """
    Decide what happens after SQL synthesis.

    Typical behaviour:
    - Normal mode: run "exec" to execute the SQL, then "explain".
    - Dry-run mode (if you ever support it): skip exec and go straight to "explain".

    For now we assume normal mode unless a specific flag is set.
    """

    # Example optional flag on state: dry_run: bool = False
    dry_run = getattr(state, "dry_run", False)

    if dry_run:
        return "explain"

    return "exec"


def node_exec(state: ReconState) -> ReconState:
    """
    Node: Execute SQL on BigQuery.
    Stores DataFrame in state.result_df.
    """

    sql = state.sql
    if not sql:
        logger.error("[node_exec] No SQL provided in state.sql")
        state.result_df = None
        return state

    logger.info("[node_exec] Running SQL on BigQuery...")

    from backend.connectors.bigquery_connector import BigQueryConnector
    from backend.config import settings

    logger.info("[node_exec] Project: %s", settings.google_project_id)
    logger.info("[node_exec] SQL length: %s chars", len(sql))

    bq = BigQueryConnector(project_id=settings.google_project_id)

    try:
        df = bq.run_query(sql)
        logger.info("[node_exec] BQ query completed. Rows fetched: %s", len(df))
        logger.info("[node_exec] Columns: %s", list(df.columns))
        logger.info("[node_exec] Sample row: %s", df.head(1).to_dict(orient="records"))

    except Exception as e:
        logger.error("[node_exec] Error executing BigQuery SQL: %s",
                     e, exc_info=True)
        state.result_df = None
        return state

    state.result_df = df
    return state

def node_explain(state: ReconState) -> ReconState:
    """
    Generate a natural language explanation of the reconciliation results
    using ExplanationGeneratorAgent.
    """

    df = getattr(state, "result_df", None)

    # If there are no results, keep it simple
    if df is None or df.empty:
        state.explanation = (
            "No reconciliation differences were found, or the query returned no rows."
        )
        return state

    # Convert a small sample of results to JSON-safe records for the LLM
    sample_records = df.head(20).to_dict(orient="records")

    payload = {
        "schema_mapping": state.schema_mapping,
        "thresholds": state.thresholds,
        "entities": getattr(state, "entities", []) or [],
        "results": sample_records,
    }

    try:
        eg_result = eg.run(payload)
    except Exception as e:
        logger.error("node_explain: ExplanationGeneratorAgent error: %s", e, exc_info=True)
        state.explanation = "Reconciliation completed, but explanation generation failed."
        return state

    # Expecting something like {"explanation": "..."}
    if isinstance(eg_result, dict):
        state.explanation = (
            eg_result.get("explanation")
            or eg_result.get("summary")
            or "Reconciliation completed."
        )
    else:
        state.explanation = str(eg_result)

    return state


def build_graph():
    g = StateGraph(ReconState)

    # Nodes
    g.add_node("load", node_load)
    g.add_node("materialize_sources", materialize_sources)  # stays
    g.add_node("map", node_map)
    g.add_node("approval_node", node_approval)
    g.add_node("await", node_await)               # used for PENDING_APPROVAL stop
    g.add_node("entity_resolve", node_entity_resolve)
    g.add_node("sql_node", node_sql)              # just builds SQL via qs
    g.add_node("exec", node_exec)                 # runs SQL on BQ, sets result_df
    g.add_node("explain", node_explain)

    # Edges
    g.add_edge(START, "load")

    # load -> materialize_sources -> map
    g.add_edge("load", "materialize_sources")
    g.add_edge("materialize_sources", "map")

    # map -> approval
    g.add_edge("map", "approval_node")

    # After approval:
    # - First call (/reconcile): status=PENDING_APPROVAL → go to "await" → END (front-end shows mapping)
    # - Second call (/reconcile/approve): status=APPROVED → go to "entity_resolve"
    g.add_conditional_edges(
        "approval_node",
        decide_after_approval,
        {
            "entity_resolve": "entity_resolve",
            "await": "await",
        },
    )

    g.add_edge("await", END)

    # After entity resolution, always go to SQL synthesis node
    g.add_edge("entity_resolve", "sql_node")

    # After SQL synthesis:
    # - Normal flow: run exec first, then explain
    # - Optional: if you ever support a "dry run" mode, you can jump straight to explain
    g.add_conditional_edges(
        "sql_node",
        decide_after_sql,
        {
            "exec": "exec",
            "explain": "explain",
        },
    )

    g.add_edge("exec", "explain")
    g.add_edge("explain", END)

    return g.compile()


graph = build_graph()

def run_graph(payload: dict) -> dict:
    logger.info("RUN_GRAPH_VERSION: 2025-12-04-REV3")

    try:
        state = ReconState(**payload)
    except ValidationError as e:
        logger.error("ReconState validation error: %s", e.json())
        raise

    # Run the graph →
    final = graph.invoke(state)

    # ---------------------------------------------------------
    # REMOVE ALL DATAFRAMES (any possible location)
    # ---------------------------------------------------------

    # Remove full DataFrames
    for attr in ["data_a", "data_b", "df_a_sample", "df_b_sample",
                 "result_df", "entity_resolved"]:
        if hasattr(final, attr):
            val = getattr(final, attr)
            if isinstance(val, pd.DataFrame):
                setattr(final, attr, None)
            elif isinstance(val, dict):
                # If dict contains DFs (entity_resolved often does)
                for k, v in list(val.items()):
                    if isinstance(v, pd.DataFrame):
                        val[k] = None
                setattr(final, attr, val)

    # Convert result_df → result list
    if hasattr(final, "result_df") and isinstance(final.result_df, pd.DataFrame):
        try:
            final.result = final.result_df.to_dict(orient="records")
        except Exception as e:
            logger.error("[run_graph] DF → JSON conversion failed: %s", e)
            final.result = []
        final.result_df = None

    # ---------------------------------------------------------
    # Handle Pydantic vs AddableValuesDict
    # ---------------------------------------------------------

    if hasattr(final, "dict"):
        result = final.dict()
    else:
        result = dict(final)

    # Absolute safety: RE-SCAN for any leftover DataFrames (deep)
    def df_sanitizer(obj):
        if isinstance(obj, pd.DataFrame):
            return None
        if isinstance(obj, list):
            return [df_sanitizer(x) for x in obj]
        if isinstance(obj, dict):
            return {k: df_sanitizer(v) for k, v in obj.items()}
        return obj

    result = df_sanitizer(result)

    return result

