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

# Embeddings — a separate endpoint from the chat LLM (OpenRouter is chat-only).
# Defaults target the nomic-embed-text model already used by Letta, served over
# Ollama's OpenAI-compatible /v1/embeddings.  Override any of these in .env.
EMBED_BASE_URL = os.getenv("EMBED_BASE_URL", "http://localhost:11434/v1")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")
# Embedding servers usually ignore the key (Ollama) — fall back to the LLM key.
EMBED_API_KEY = os.getenv("EMBED_API_KEY", LLM_API_KEY or "sk-noauth")
EMBED_BATCH = int(os.getenv("EMBED_BATCH", "128"))
# Hard cap on per-item input length (characters) before embedding. Memory items
# (esp. whole-session chunks) can exceed the embedding model's context window;
# the server then 400s ("input length exceeds the context length"). We truncate
# to this many characters first. ~4 chars/token, so 8000 ≈ 2000 tokens — safe for
# a 2048-ctx model. Raise it for an 8k-ctx model; the embedder also shrinks
# adaptively if the server still reports a context-length error.
EMBED_MAX_INPUT_CHARS = int(os.getenv("EMBED_MAX_INPUT_CHARS", "8000"))
# L2-normalise vectors so dot product == cosine. Off only for debugging.
EMBED_NORMALIZE = os.getenv("EMBED_NORMALIZE", "1") not in ("0", "false", "False")
# Persisted content-addressable cache so corpus passes / reruns don't re-embed.
# Empty string disables caching.
EMBED_CACHE_PATH = os.getenv("EMBED_CACHE_PATH", "artifacts/embeddings.sqlite")

# Byte-cost model (global switch). When False (default), the retention budget
# counts only stored CONTENT = text bytes, treating the embedding as a side
# index (like BM25 postings) — this matches the no-embedding runs already
# characterised. When True, the vector counts as part of a record's stored cost.
# The flag is the single source of truth: every byte consumer resolves cost via
# forgetit.core.cost.record_cost, so footprint / used_bytes / compression all
# agree. See forgetit.core.cost.
COUNT_VECTOR_BYTES = os.getenv("COUNT_VECTOR_BYTES", "0") not in ("0", "false", "False", "")

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