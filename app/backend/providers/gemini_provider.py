from .llm_provider import LLMProvider
from backend.config import settings


class GeminiLLM(LLMProvider):
    _client = None
    _mode = None  # "vertex" or "api"

    @classmethod
    def init_client(cls):
        """
        Auto-select client:
        - Vertex AI if GOOGLE_PROJECT_ID is set
        - Google AI Studio if GOOGLE_API_KEY is set
        """
        # Prefer Vertex mode
        if settings.google_project_id:
            try:
                import vertexai
                from vertexai.generative_models import GenerativeModel
            except ImportError as e:
                raise RuntimeError(
                    "GeminiLLM: missing dependency 'google-cloud-aiplatform'."
                ) from e

            vertexai.init(
                project=settings.google_project_id,
                location=settings.vertex_location,
            )
            cls._client = GenerativeModel(settings.gemini_model)
            cls._mode = "vertex"
            return

        # Fallback → Google AI Studio API
        if settings.google_api_key:
            try:
                import google.generativeai as genai
            except ImportError as e:
                raise RuntimeError(
                    "GeminiLLM: missing dependency 'google-generativeai'."
                ) from e

            genai.configure(api_key=settings.google_api_key)
            cls._client = genai.GenerativeModel(settings.gemini_model)
            cls._mode = "api"
            return

        # No valid configuration → default to Mock provider
        cls._client = None
        cls._mode = None

    def chat(self, prompt: str) -> str:
        """
        Mirror OpenAILLM behaviour:
        - If client missing → fallback to MockLLM
        - Inject system instruction automatically
        - Return raw string output (no markdown)
        """

        # Init client if needed
        if GeminiLLM._client is None:
            GeminiLLM.init_client()

        # If still no client → mock fallback
        if GeminiLLM._client is None:
            from .mock_provider import MockLLM
            return MockLLM().chat(prompt)

        # Build system + user message (Gemini supports "messages" but via different API)
        system_instruction = "Be precise, return JSON when asked, no markdown."
        full_prompt = f"{system_instruction}\n\nUser: {prompt}"

        # ---- Vertex AI mode ----
        if GeminiLLM._mode == "vertex":
            response = GeminiLLM._client.generate_content(full_prompt)
            if hasattr(response, "text"):
                return response.text

            # fallback if vertex response is structured differently
            try:
                cand = response.candidates[0]
                part = cand.content.parts[0]
                return part.text
            except Exception:
                raise RuntimeError(
                    "GeminiLLM (Vertex): unexpected response structure."
                )

        # ---- Google AI Studio API mode ----
        if GeminiLLM._mode == "api":
            response = GeminiLLM._client.generate_content(full_prompt)
            return response.text

        raise RuntimeError("GeminiLLM: invalid internal mode state.")
