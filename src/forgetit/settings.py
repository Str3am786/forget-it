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

if not loaded:
    raise RuntimeError(f".env not loaded from {ENV_PATH} (file missing or not readable).")

if not LLM_API_KEY:
    raise RuntimeError(f"LLM_API_KEY missing (expected in {ENV_PATH} or exported env var).")