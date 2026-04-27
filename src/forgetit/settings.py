import os
from pathlib import Path
from dotenv import load_dotenv

# repo root = .../forget-it
REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = REPO_ROOT / ".env"

# Load it deterministically
loaded = load_dotenv(dotenv_path=ENV_PATH, override=False)

LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1")
LLM_MODEL = os.getenv("LLM_MODEL")
LLM_API_KEY = os.getenv("LLM_API_KEY")

# Letta / MemGPT — api_key is optional for local instances
LETTA_BASE_URL = os.getenv("LETTA_BASE_URL", "http://localhost:8283")
LETTA_API_KEY = os.getenv("LETTA_API_KEY")
# Max characters per chunk sent to Letta for embedding.
# Letta does NOT re-chunk API-provided text before embedding, so this value
# must keep even dense technical text (code, SQL) within the model's token limit.
# nomic-embed-text via Ollama defaults to 2048 token context; code can hit
# ~1 token/char, so 512 chars is a safe ceiling.
LETTA_MAX_CHUNK_CHARS = int(os.getenv("LETTA_MAX_CHUNK_CHARS", "512"))

if not loaded:
    raise RuntimeError(f".env not loaded from {ENV_PATH} (file missing or not readable).")

if not LLM_API_KEY:
    raise RuntimeError(f"LLM_API_KEY missing (expected in {ENV_PATH} or exported env var).")