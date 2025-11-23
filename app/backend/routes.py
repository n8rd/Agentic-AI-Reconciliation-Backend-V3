from fastapi import APIRouter
from backend.graph.orchestrator_graph import run_graph

router = APIRouter()

@router.post("/reconcile")
def reconcile(payload: dict):
    return run_graph(payload)
