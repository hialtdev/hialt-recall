"""
rag_engine.py — Shared RAG logic for hialt-recall.

Extracted from query.py so both the CLI and the Streamlit UI
can import the same pipeline without duplication.

Field names match MongoDB documents written by ingest.py:
  source_file, headers, text, <embedding_field>
"""

from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, TypedDict

import numpy as np
import requests
from dotenv import load_dotenv
from pymongo import MongoClient


# ---------------------------------------------------------------------------
# Settings  (mirrors the dataclass in query.py / ingest.py exactly)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Settings:
    mongo_uri: str
    mongo_default_db: str
    mongo_collection: str
    ollama_base_url: str
    embedding_model: str
    embedding_field: str
    ollama_llm_model: str
    groq_api_key: Optional[str]
    groq_model: str
    llm_temperature: float


def load_settings(env_path: Optional[str] = None) -> Settings:
    """
    Load settings from .env then environment variables.
    Looks for .env in the parent directory (root) since this file lives in src/.
    Raises RuntimeError if MONGO_URI is missing.
    """
    if env_path:
        load_dotenv(dotenv_path=env_path, override=True)
    else:
        # This file is at ~/hialt-recall/src/rag_engine.py
        this_dir = Path(__file__).resolve().parent
        # Climb up one directory to the repository root (~/hialt-recall/.env)
        root_dir = this_dir.parent
        
        dotenv_path = root_dir / ".env"
        # Fallback to current directory just in case
        if not dotenv_path.exists():
            dotenv_path = this_dir / ".env"
            
        load_dotenv(dotenv_path=dotenv_path, override=True)

    mongo_uri = os.environ.get("MONGO_URI", "").strip()
    if not mongo_uri:
        # Providing explicit feedback to the Streamlit UI if it fails
        raise RuntimeError(
            f"Missing MONGO_URI in .env or environment.\n"
            f"Attempted to read from: {dotenv_path.resolve()}"
        )

    return Settings(
        mongo_uri=mongo_uri,
        mongo_default_db=os.environ.get("MONGO_DB_NAME", "local_rag").strip(),
        mongo_collection=os.environ.get("MONGO_COLLECTION", "rag_chunks").strip(),
        ollama_base_url=os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434").strip(),
        embedding_model=os.environ.get("OLLAMA_EMBEDDING_MODEL", "mxbai-embed-large").strip(),
        embedding_field=os.environ.get("MONGO_EMBEDDING_FIELD", "embedding").strip(),
        ollama_llm_model=os.environ.get("OLLAMA_LLM_MODEL", "llama3").strip(),
        groq_api_key=os.environ.get("GROQ_API_KEY") or None,
        groq_model=os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile").strip(),
        llm_temperature=float(os.environ.get("LLM_TEMPERATURE", "0.2").strip()),
    )


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

class Chunk(TypedDict):
    text: str
    source_file: str
    headers: str
    score: float


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
    """Embed text via Ollama. Matches _ollama_embeddings() in query.py."""
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

def _cosine_top_k(
    query_vec: List[float],
    docs: List[dict],
    embedding_field: str,
    top_k: int,
) -> List[Chunk]:
    """Exact port of _cosine_top_k() from query.py."""
    q = np.array(query_vec, dtype=np.float32)
    q_norm = np.linalg.norm(q)
    if q_norm == 0:
        return []
    q = q / q_norm

    scored: List[Chunk] = []
    for doc in docs:
        raw_emb = doc.get(embedding_field)
        if not raw_emb:
            continue
        v = np.array(raw_emb, dtype=np.float32)
        v_norm = np.linalg.norm(v)
        if v_norm == 0:
            continue
        score = float(np.dot(q, v / v_norm))
        scored.append(
            Chunk(
                text=doc.get("text", ""),
                source_file=doc.get("source_file", ""),
                headers=doc.get("headers", ""),
                score=score,
            )
        )

    scored.sort(key=lambda c: c["score"], reverse=True)
    return scored[:top_k]


def fetch_and_rank(settings: Settings, query_vec: List[float], top_k: int) -> List[Chunk]:
    """Fetch all docs from Mongo and return top-k by cosine similarity."""
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    db = client[settings.mongo_default_db]
    collection = db[settings.mongo_collection]
    projection = {
        "_id": 0,
        "text": 1,
        "source_file": 1,
        "headers": 1,
        settings.embedding_field: 1,
    }
    all_docs = list(collection.find({}, projection))
    return _cosine_top_k(query_vec, all_docs, settings.embedding_field, top_k)


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
) -> RAGResult:
    """
    Full RAG pipeline: embed → retrieve → generate.

    Args:
        question:     Natural-language question.
        top_k:        Number of chunks to retrieve (default 3).
        force_ollama: Skip Groq, use Ollama only (mirrors --no-groq flag).
        settings:     Pre-loaded Settings; loads from .env if None.

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

    # Step 2 — retrieve
    try:
        chunks = fetch_and_rank(settings, q_vec, top_k)
    except Exception as e:
        return RAGResult(
            question=question, answer="", chunks=[], llm_used="none",
            error=f"MongoDB retrieval failed: {e}",
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