from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    model_provider: str = "openai"  # openai | gemini | mock
    openai_api_key: Optional[str] = None
    openai_base_url: Optional[str] = None
    openai_model: str = "gpt-4o-mini"

    google_project_id: Optional[str] = None
    vertex_location: str = "us-central1"
    gemini_model: str = "gemini-1.5-pro"

    class Config:
        env_file = ".env"

settings = Settings()
