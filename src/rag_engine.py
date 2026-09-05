"""
rag_engine.py — Shared RAG logic for hialt-recall.

Extracted from query.py so both the CLI and the Streamlit UI
can import the same pipeline without duplication.

Datastore: Postgres + pgvector (migrated 2026-09 from MongoDB — brute-force
numpy cosine similarity replaced by a server-side pgvector HNSW index).
Column names written by rag_consumer.py / read here:
  source_file, headers, text, project_dir, project_name, tags, embedding
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from dotenv import load_dotenv
from dataclasses import dataclass
from typing import List, Optional, TypedDict

import psycopg2
import requests
from pgvector import Vector
from pgvector.psycopg2 import register_vector

log = logging.getLogger("hialt.rag_engine")

# Postgres identifiers (table names) come from environment variables the user
# controls, but are still interpolated into SQL via f-strings below (psycopg2
# can't parameterize identifiers) — validate them so a typo'd .env value fails
# loudly at startup instead of producing confusing SQL syntax errors.
_VALID_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, label: str) -> str:
    if not _VALID_IDENTIFIER_RE.match(name):
        raise RuntimeError(
            f"Invalid {label} '{name}' — must be a plain SQL identifier "
            f"(letters, digits, underscore, not starting with a digit)."
        )
    return name


# ---------------------------------------------------------------------------
# Settings  (mirrors the dataclass in query.py / ingest.py exactly)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Settings:
    postgres_uri: str
    postgres_table: str
    postgres_failed_table: str
    embedding_dim: int
    ollama_base_url: str
    embedding_model: str
    ollama_llm_model: str
    groq_api_key: Optional[str]
    groq_model: str
    llm_temperature: float
    data_repos_dir: Path  # Added so ingest.py can find your projects


def load_settings(env_path: Optional[str] = None) -> Settings:
    """
    Load settings from .env then environment variables.
    Looks for .env in the parent directory (root) since this file lives in src/.
    Raises RuntimeError if POSTGRES_URI is missing.
    """
    if env_path:
        dotenv_path = env_path
    else:
        this_dir = Path(__file__).resolve().parent
        root_dir = this_dir.parent
        dotenv_path = root_dir / ".env"
        if not dotenv_path.exists():
            dotenv_path = this_dir / ".env"

    load_dotenv(dotenv_path=dotenv_path, override=True)

    postgres_uri = os.environ.get("POSTGRES_URI", "").strip()
    if not postgres_uri:
        raise RuntimeError(f"Missing POSTGRES_URI in .env or environment. Checked: {dotenv_path}")

    # Dynamically locate data/repos directory based on environment
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        data_repos_dir = Path("/data/repos").resolve()
    else:
        this_dir = Path(__file__).resolve().parent
        data_repos_dir = (this_dir.parent / "data" / "repos").resolve()

    return Settings(
        postgres_uri=postgres_uri,
        postgres_table=_validate_identifier(
            os.environ.get("POSTGRES_TABLE", "chunks").strip(), "POSTGRES_TABLE"
        ),
        postgres_failed_table=_validate_identifier(
            os.environ.get("POSTGRES_FAILED_TABLE", "chunks_failed").strip(), "POSTGRES_FAILED_TABLE"
        ),
        embedding_dim=int(os.environ.get("EMBEDDING_DIM", "1024").strip()),
        ollama_base_url=os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434").strip(),
        embedding_model=os.environ.get("OLLAMA_EMBEDDING_MODEL", "mxbai-embed-large").strip(),
        ollama_llm_model=os.environ.get("OLLAMA_LLM_MODEL", "llama3").strip(),
        groq_api_key=os.environ.get("GROQ_API_KEY") or None,
        groq_model=os.environ.get("GROQ_MODEL", "openai/gpt-oss-120b").strip(),
        llm_temperature=float(os.environ.get("LLM_TEMPERATURE", "0.2").strip()),
        data_repos_dir=data_repos_dir,  # Passed back here
    )


# ---------------------------------------------------------------------------
# Postgres connection + schema
# ---------------------------------------------------------------------------

def get_connection(settings: Settings):
    """
    Open a psycopg2 connection with pgvector's type adapters registered, so
    a Vector() can be bound to a `vector` column/parameter and a `vector`
    result column casts back to a Python object.

    register_vector() looks the `vector` type up in the database and raises
    psycopg2.ProgrammingError if the extension isn't installed — so this is
    deliberately NOT used by ensure_schema(), which is the code path that
    installs it. See _connect_raw().

    Note that registration only adapts pgvector's own Vector (and numpy
    ndarray); a plain list of floats binds as a Postgres ARRAY[...] literal,
    which no `vector` operator accepts. Always wrap embeddings in Vector().
    """
    conn = _connect_raw(settings)
    register_vector(conn)
    return conn


def _connect_raw(settings: Settings):
    """Open a connection without pgvector registration (bootstrap path)."""
    return psycopg2.connect(settings.postgres_uri)


def ensure_schema(settings: Settings) -> None:
    """
    Create the pgvector extension, the chunks table + its indexes, and the
    dead-letter table if they don't already exist yet. Idempotent — safe to
    call on every startup, same philosophy as the old Mongo _ensure_indexes().

    Called by rag_consumer.py at startup and by ingest.py's --reset path.

    Uses _connect_raw() rather than get_connection(): the `vector` type may
    not exist yet on a fresh database, and registering it is exactly what
    would fail before the CREATE EXTENSION below has had a chance to run.

    CREATE EXTENSION IF NOT EXISTS is a no-op when the extension is already
    present, which is the normal case here — the platform provisions it
    declaratively (the Database resource in hialt-platform requests it),
    because pgvector is not a trusted extension and the owning role cannot
    install it itself.
    """
    conn = _connect_raw(settings)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {settings.postgres_table} (
                    doc_id       TEXT PRIMARY KEY,
                    project_dir  TEXT NOT NULL,
                    project_name TEXT NOT NULL,
                    tags         TEXT[] NOT NULL DEFAULT '{{}}',
                    source_file  TEXT NOT NULL,
                    chunk_index  INT NOT NULL,
                    headers      TEXT,
                    text         TEXT NOT NULL,
                    embedding    vector({settings.embedding_dim}),
                    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
                );
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{settings.postgres_table}_project_dir
                    ON {settings.postgres_table} (project_dir);
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{settings.postgres_table}_project_name
                    ON {settings.postgres_table} (project_name);
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{settings.postgres_table}_tags
                    ON {settings.postgres_table} USING GIN (tags);
            """)
            # HNSW + cosine distance ops — matches the old numpy cosine-similarity
            # ranking exactly (score = 1 - cosine_distance, see fetch_and_rank).
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{settings.postgres_table}_embedding_hnsw
                    ON {settings.postgres_table} USING hnsw (embedding vector_cosine_ops);
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {settings.postgres_failed_table} (
                    doc_id       TEXT PRIMARY KEY,
                    project_dir  TEXT,
                    project_name TEXT,
                    tags         TEXT[],
                    source_file  TEXT,
                    chunk_index  INT,
                    headers      TEXT,
                    text         TEXT,
                    error        TEXT,
                    failed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
                );
            """)
    finally:
        conn.close()
    log.info(
        "Postgres schema verified (table=%s, failed_table=%s, embedding_dim=%d).",
        settings.postgres_table, settings.postgres_failed_table, settings.embedding_dim,
    )


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

class Chunk(TypedDict):
    text: str
    source_file: str
    headers: str
    score: float
    # {"project": <manifest display name>, "tags": [...]}
    metadata: dict


@dataclass
class RAGResult:
    question: str
    answer: str
    chunks: List[Chunk]
    llm_used: str        # "groq" | "ollama" | "none"
    error: Optional[str] # None on clean success; fallback message or error string


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------

def embed_query(settings: Settings, text: str) -> List[float]:
    """Embed text via Ollama. Matches _embed() in rag_consumer.py."""
    url = f"{settings.ollama_base_url.rstrip('/')}/api/embeddings"
    resp = requests.post(
        url,
        json={"model": settings.embedding_model, "prompt": text},
        timeout=120,
    )
    resp.raise_for_status()
    return [float(x) for x in resp.json().get("embedding", [])]


# ---------------------------------------------------------------------------
# Retrieval
# ---------------------------------------------------------------------------

def fetch_and_rank(
    settings: Settings,
    query_vec: List[float],
    top_k: int,
    project_filter: Optional[str] = None,
    tag_filter: Optional[str] = None,
) -> List[Chunk]:
    """
    Retrieve the top-k chunks ranked by cosine similarity, computed and
    ordered server-side via pgvector's HNSW index (`embedding <=> query`),
    with optional project/tag filters pushed into the WHERE clause.

    Parameters
    ──────────
    settings
        Shared pipeline configuration (Postgres URI, table, embedding dim).
    query_vec
        Embedding of the user's query; must match settings.embedding_dim
        (produced by ``embed_query()``).
    top_k
        Maximum number of ranked Chunk results to return.
    project_filter : optional
        If provided, matches EITHER project_dir (the raw directory name
        under data/repos, e.g. "kubeconfigs" — what ingest.py's --project
        flag uses) OR project_name (the manifest's display name, e.g.
        "KubeConfigs") — these can legitimately differ.
    tag_filter : optional
        If provided, restricts to rows whose tags array contains this tag.

    Returns
    ───────
    List[Chunk] — ranked highest-score-first, length ≤ top_k.
    Empty list when no candidates match the filter or the table is empty.
    """
    where_clauses: List[str] = []
    # Vector(), not a plain list — psycopg2 renders a list as ARRAY[...],
    # which the `<=>` operator has no overload for.
    params: dict = {"query_vec": Vector(query_vec), "top_k": top_k}

    if project_filter is not None:
        where_clauses.append("(project_dir = %(project_filter)s OR project_name = %(project_filter)s)")
        params["project_filter"] = project_filter

    if tag_filter is not None:
        where_clauses.append("%(tag_filter)s = ANY(tags)")
        params["tag_filter"] = tag_filter

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    conn = get_connection(settings)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT source_file, headers, text, project_name, tags,
                       1 - (embedding <=> %(query_vec)s) AS score
                FROM {settings.postgres_table}
                {where_sql}
                ORDER BY embedding <=> %(query_vec)s
                LIMIT %(top_k)s;
                """,
                params,
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    return [
        Chunk(
            text=text,
            source_file=source_file,
            headers=headers or "",
            score=float(score),
            metadata={"project": project_name, "tags": list(tags) if tags else []},
        )
        for source_file, headers, text, project_name, tags, score in rows
    ]


# ---------------------------------------------------------------------------
# Prompt assembly
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "Use only the provided context to answer. "
    "If the answer is not in the context, say you don't know. "
    "Always cite source_file and headers."
)


def _build_user_prompt(question: str, chunks: List[Chunk]) -> str:
    parts = [
        f"[{i+1}] {c['source_file']} ({c['headers']}):\n{c['text']}"
        for i, c in enumerate(chunks)
    ]
    return f"Context:\n{chr(10).join(parts)}\n\nQuestion: {question}"


# ---------------------------------------------------------------------------
# LLM backends  (exact ports of _groq_chat / _ollama_chat from query.py)
# ---------------------------------------------------------------------------

def _groq_chat(settings: Settings, user_prompt: str) -> str:
    if not settings.groq_api_key:
        raise RuntimeError("No GROQ_API_KEY configured")
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {settings.groq_api_key}"}
    payload = {
        "model": settings.groq_model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": settings.llm_temperature,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=180)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


def _ollama_chat(settings: Settings, user_prompt: str) -> str:
    url = f"{settings.ollama_base_url.rstrip('/')}/api/chat"
    payload = {
        "model": settings.ollama_llm_model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "options": {"temperature": settings.llm_temperature},
        "stream": False,
    }
    resp = requests.post(url, json=payload, timeout=180)
    resp.raise_for_status()
    return resp.json().get("message", {}).get("content", "").strip()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_query(
    question: str,
    top_k: int = 3,
    force_ollama: bool = False,
    settings: Optional[Settings] = None,
    project_filter: Optional[str] = None,
    tag_filter: Optional[str] = None,
) -> RAGResult:
    """
    Full RAG pipeline: embed → retrieve → generate.

    Args:
        question:        Natural-language question.
        top_k:           Number of chunks to retrieve (default 3).
        force_ollama:    Skip Groq, use Ollama only (mirrors --no-groq flag).
        settings:        Pre-loaded Settings; loads from .env if None.
        project_filter:  If set, restrict retrieval to this project name.
        tag_filter:      If set, restrict retrieval to docs with this tag.

    Returns:
        RAGResult — always returns, never raises.
        On success: answer + chunks + llm_used, error=None.
        On fallback: answer + chunks + llm_used="ollama", error=<fallback note>.
        On failure: answer="", error=<message>.
    """
    if settings is None:
        try:
            settings = load_settings()
        except RuntimeError as e:
            return RAGResult(
                question=question, answer="", chunks=[], llm_used="none", error=str(e)
            )

    # Step 1 — embed
    try:
        q_vec = embed_query(settings, question)
    except Exception as e:
        return RAGResult(
            question=question, answer="", chunks=[], llm_used="none",
            error=f"Embedding failed: {e}",
        )

    # Step 2 — retrieve (with optional metadata filters)
    try:
        chunks = fetch_and_rank(
            settings,
            q_vec,
            top_k,
            project_filter=project_filter,
            tag_filter=tag_filter,
        )
    except Exception as e:
        return RAGResult(
            question=question, answer="", chunks=[], llm_used="none",
            error=f"Postgres retrieval failed: {e}",
        )

    if not chunks:
        return RAGResult(
            question=question,
            answer="No relevant context found. Make sure you have run `ingest.py` first.",
            chunks=[],
            llm_used="none",
            error=None,
        )

    user_prompt = _build_user_prompt(question, chunks)

    # Step 3 — generate (Groq → Ollama fallback)
    groq_error_msg: str = ""

    if not force_ollama:
        try:
            answer = _groq_chat(settings, user_prompt)
            return RAGResult(
                question=question, answer=answer, chunks=chunks,
                llm_used="groq", error=None,
            )
        except Exception as e:
            groq_error_msg = str(e)
    else:
        groq_error_msg = "skipped via --no-groq"

    try:
        answer = _ollama_chat(settings, user_prompt)
        return RAGResult(
            question=question, answer=answer, chunks=chunks,
            llm_used="ollama",
            error=f"Groq unavailable ({groq_error_msg}), answered via local Ollama.",
        )
    except Exception as ollama_err:
        return RAGResult(
            question=question, answer="", chunks=chunks, llm_used="none",
            error=f"Both LLMs failed.\nGroq: {groq_error_msg}\nOllama: {ollama_err}",
        )
