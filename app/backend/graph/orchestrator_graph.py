from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver

from backend.agents.schema_mapper import SchemaMapperAgent
from backend.agents.entity_resolver import EntityResolverAgent
from backend.agents.query_synthesizer import QuerySynthesizerAgent
from backend.agents.explanation_generator import ExplanationGeneratorAgent
from backend.config import settings
from backend.data_loader import load_source_data
from backend.connectors.bigquery_connector import bigquery, BigQueryConnector

class ReconState(BaseModel):
    # input configs
    dataset_a: Dict[str, Any] | None = None
    dataset_b: Dict[str, Any] | None = None
    thresholds: Dict[str, Any] = {"abs": 0.01, "rel": 0.001}
    entities: List[str] | None = None
    approval: Optional[str] = None

    # internal / results
    df_a_sample: Any | None = None
    df_b_sample: Any | None = None
    schema_mapping: Dict[str, Any] | None = None
    entity_res: Dict[str, Any] | None = None
    sql: str | None = None
    bq_status: str | None = None
    explanation: str | None = None
    status: str | None = None

sm = SchemaMapperAgent()
er = EntityResolverAgent()
qs = QuerySynthesizerAgent()
eg = ExplanationGeneratorAgent()

def node_load(state: ReconState) -> ReconState:
    # Use unified loader: supports file/postgres/hive/oracle/bigquery
    df_a = load_source_data(state.dataset_a)
    df_b = load_source_data(state.dataset_b)

    # Keep only a sample in state to keep payload light
    state.df_a_sample = df_a.head(50).to_dict(orient="list")
    state.df_b_sample = df_b.head(50).to_dict(orient="list")
    return state

def node_map(state: ReconState) -> ReconState:
    import pandas as pd
    df_a = pd.DataFrame(state.df_a_sample)
    df_b = pd.DataFrame(state.df_b_sample)
    state.schema_mapping = sm.run({"df_a": df_a, "df_b": df_b})
    state.status = "PENDING_APPROVAL"
    return state

def node_approval(state: ReconState) -> ReconState:
    if state.approval == "approve":
        state.status = "APPROVED"
    elif state.approval == "revise":
        state.status = "REVISE_REQUESTED"
    return state

def decide_after_approval(state: ReconState):
    if state.status == "APPROVED":
        return "entity_resolve"
    if state.status == "REVISE_REQUESTED":
        return "map"
    return "await"

def node_await(state: ReconState) -> ReconState:
    # Pause here; client inspects mapping and calls again with approval set
    return state

def node_entity_resolve(state: ReconState) -> ReconState:
    ents = state.entities or []
    state.entity_res = er.run({"entities": ents})
    return state

def node_sql(state: ReconState) -> ReconState:
    table_a = state.dataset_a.get("table_fqn", "project.dataset.table_a")
    table_b = state.dataset_b.get("table_fqn", "project.dataset.table_b")
    # numeric/array cols assumed to be provided or can be inferred
    numeric_cols = state.dataset_a.get("numeric_cols", [])
    array_cols = state.dataset_a.get("array_cols", [])
    out = qs.run({
        "schema_mapping": state.schema_mapping,
        "thresholds": state.thresholds,
        "table_a": table_a,
        "table_b": table_b,
        "numeric_cols": numeric_cols,
        "array_cols": array_cols,
    })
    state.sql = out["sql"]
    return state

def decide_after_sql(state: ReconState):
    if state.sql and "SELECT" in state.sql.upper():
        return "exec"
    return "explain"

def node_exec(state: ReconState) -> ReconState:
    if bigquery is None or not settings.google_project_id:
        state.bq_status = "SKIPPED_NO_BQ"
        return state
    client = bigquery.Client(project=settings.google_project_id)
    job = client.query(state.sql)
    res = job.result()
    state.bq_status = f"{res.total_rows} rows processed"
    return state

def node_explain(state: ReconState) -> ReconState:
    out = eg.run({"sql": state.sql, "bq_status": state.bq_status})
    state.explanation = out["explanation"]
    state.status = state.status or "DONE"
    return state

def build_graph():
    g = StateGraph(ReconState)
    g.add_node("load", node_load)
    g.add_node("map", node_map)
    g.add_node("approval_node", node_approval)
    g.add_node("await", node_await)
    g.add_node("entity_resolve", node_entity_resolve)
    g.add_node("sql_node", node_sql)
    g.add_node("exec", node_exec)
    g.add_node("explain", node_explain)

    g.add_edge(START, "load")
    g.add_edge("load", "map")
    g.add_edge("map", "approval_node")
    g.add_conditional_edges("approval_node", decide_after_approval,
                            {"entity_resolve": "entity_resolve", "map": "map", "await": "await"})
    g.add_edge("await", END)
    g.add_edge("entity_resolve", "sql_node")
    g.add_conditional_edges("sql_node", decide_after_sql,
                            {"exec": "exec", "explain": "explain"})
    g.add_edge("exec", "explain")
    g.add_edge("explain", END)

    #return g.compile(checkpointer=MemorySaver())
    return g.compile()

graph = build_graph()

def run_graph(payload: dict) -> dict:
    state = ReconState(**payload)
    final = graph.invoke(state)
    return final.dict()
