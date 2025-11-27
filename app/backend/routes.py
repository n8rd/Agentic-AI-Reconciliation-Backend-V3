# backend/routes.py
import json
import os
from typing import Optional

from fastapi import APIRouter, Form, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse

from backend.graph.orchestrator_graph import run_graph

router = APIRouter()

UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/tmp/recon_uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


@router.post("/reconcile")
async def reconcile(
    dataset_a: str = Form(...),
    dataset_b: str = Form(...),
    thresholds: str = Form(...),
    entities: str = Form("[]"),
    fileA: Optional[UploadFile] = File(None),
    fileB: Optional[UploadFile] = File(None),
):
    """
    Supports both:
    - Pure config (no files)
    - Config + uploaded files for dataset A/B
    """
    try:
        src_a = json.loads(dataset_a)
        src_b = json.loads(dataset_b)
        thresholds_obj = json.loads(thresholds)
        entities_obj = json.loads(entities)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON in one of the fields: {e}")

    # If files are uploaded, save and override the source configs
    if fileA is not None:
        filename_a = f"dataset_a_{fileA.filename}"
        path_a = os.path.join(UPLOAD_DIR, filename_a)
        with open(path_a, "wb") as f:
            f.write(await fileA.read())

        src_a["type"] = "file"
        src_a["path"] = path_a
        # infer format from extension
        if "." in fileA.filename:
            src_a["format"] = fileA.filename.rsplit(".", 1)[1].lower()

    if fileB is not None:
        filename_b = f"dataset_b_{fileB.filename}"
        path_b = os.path.join(UPLOAD_DIR, filename_b)
        with open(path_b, "wb") as f:
            f.write(await fileB.read())

        src_b["type"] = "file"
        src_b["path"] = path_b
        if "." in fileB.filename:
            src_b["format"] = fileB.filename.rsplit(".", 1)[1].lower()

    payload = {
        "dataset_a": src_a,
        "dataset_b": src_b,
        "thresholds": thresholds_obj,
        "entities": entities_obj,
    }

    result = run_graph(payload)
    # result is assumed to already be JSON serialisable
    return JSONResponse(content=result)
