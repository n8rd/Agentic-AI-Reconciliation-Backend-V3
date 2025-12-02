from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    recon_model_provider: str = "gemini"  # openai | gemini | mock

    # OpenAI
    openai_api_key: Optional[str] = None
    openai_base_url: Optional[str] = None
    openai_model: str = "gpt-4o-mini"

    # Gemini via Google AI Studio
    google_api_key: Optional[str] = None

    # Gemini via Vertex AI
    google_project_id: Optional[str] = None
    vertex_location: str = "us-central1"
    gemini_model: str = "gemini-1.5-pro"

    class Config:
        env_file = ".env"


settings = Settings()
