# backend/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.routes import router  # whatever you already have

app = FastAPI(title="Agentic AI Reconciliation v3")

# Allow the UI origin + localhost for dev
origins = [
    "https://agentic-ai-reconciliation-ui-v3-947423379682.us-central1.run.app",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,          # or ["*"] temporarily if you want to allow all
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")

@app.get("/health")
def health():
    return {"status": "ok"}
