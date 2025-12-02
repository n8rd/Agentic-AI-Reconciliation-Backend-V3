# backend/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.routes import router  # whatever you already have

app = FastAPI(title="Agentic AI Reconciliation v3")

# Allow the UI origin + localhost for dev
origins = [
    "https://agentic-ai-reconciliation-ui-v3-947423379682.us-central1.run.app",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,         # no ["*"] when allow_credentials=True
    allow_credentials=True,                # if you need cookies/Authorization
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],  # or ["*"]
    allow_headers=["*"],                   # or list specific ones
    expose_headers=["*"],                  # optional, if UI reads response headers
    max_age=600,                           # optional: cache preflight
)


app.include_router(router, prefix="/api")

@app.get("/api/health")
def health():
    return {"status": "ok"}
