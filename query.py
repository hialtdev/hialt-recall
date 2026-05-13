import argparse
import os
from dataclasses import dataclass
from typing import List, Optional, TypedDict

import numpy as np
import requests
from dotenv import load_dotenv
from pymongo import MongoClient

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

class Chunk(TypedDict):
    text: str
    source_file: str
    headers: str
    score: float

def _load_settings() -> Settings:
    this_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(dotenv_path=os.path.join(this_dir, ".env"), override=False)

    mongo_uri = os.environ.get("MONGO_URI", "").strip()
    if not mongo_uri:
        raise SystemExit("Missing MONGO_URI in .env")

    return Settings(
        mongo_uri=mongo_uri,
        mongo_default_db=os.environ.get("MONGO_DB_NAME", "local_rag").strip(),
        mongo_collection=os.environ.get("MONGO_COLLECTION", "rag_chunks").strip(),
        ollama_base_url=os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434").strip(),
        embedding_model=os.environ.get("OLLAMA_EMBEDDING_MODEL", "mxbai-embed-large").strip(),
        embedding_field=os.environ.get("MONGO_EMBEDDING_FIELD", "embedding").strip(),
        ollama_llm_model=os.environ.get("OLLAMA_LLM_MODEL", "llama3").strip(),
        groq_api_key=os.environ.get("GROQ_API_KEY") or None,
        groq_model=os.environ.get("GROQ_MODEL", "llama3-70b-8192").strip(),
        llm_temperature=float(os.environ.get("LLM_TEMPERATURE", "0.2").strip()),
    )

def _ollama_embeddings(settings: Settings, text: str) -> List[float]:
    url = f"{settings.ollama_base_url.rstrip('/')}/api/embeddings"
    resp = requests.post(url, json={"model": settings.embedding_model, "prompt": text}, timeout=120)
    resp.raise_for_status()
    emb = resp.json().get("embedding")
    return [float(x) for x in emb]

def _cosine_top_k(query_vec: List[float], docs: List[dict], embedding_field: str, top_k: int) -> List[Chunk]:
    q = np.array(query_vec, dtype=np.float32)
    q_norm = np.linalg.norm(q)
    if q_norm == 0: return []
    q = q / q_norm

    scored: List[Chunk] = []
    for doc in docs:
        raw_emb = doc.get(embedding_field)
        if not raw_emb: continue
        v = np.array(raw_emb, dtype=np.float32)
        v_norm = np.linalg.norm(v)
        if v_norm == 0: continue
        score = float(np.dot(q, v / v_norm))
        scored.append(Chunk(
            text=doc.get("text", ""),
            source_file=doc.get("source_file", ""),
            headers=doc.get("headers", ""),
            score=score
        ))

    scored.sort(key=lambda c: c["score"], reverse=True)
    return scored[:top_k]

def _ollama_chat(settings: Settings, system_prompt: str, user_prompt: str) -> str:
    url = f"{settings.ollama_base_url.rstrip('/')}/api/chat"
    payload = {
        "model": settings.ollama_llm_model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "options": {"temperature": settings.llm_temperature},
        "stream": False,
    }
    resp = requests.post(url, json=payload, timeout=180)
    resp.raise_for_status()
    return resp.json().get("message", {}).get("content", "").strip()

def _groq_chat(settings: Settings, system_prompt: str, user_prompt: str) -> str:
    if not settings.groq_api_key: raise RuntimeError("No Groq API Key")
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {settings.groq_api_key}"}
    payload = {
        "model": settings.groq_model,
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
        "temperature": settings.llm_temperature,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=180)
    return resp.json().get("choices", [{}])[0].get("message", {}).get("content", "").strip()

def _build_system_prompt() -> str:
    return "Use only the provided context to answer. If unsure, say you don't know. Cite source_file and headers."

def main() -> None:
    parser = argparse.ArgumentParser(description="Query the local MongoDB RAG store.")
    parser.add_argument("query", type=str, help="User question.")
    parser.add_argument("--top-k", type=int, default=3)
    parser.add_argument("--no-groq", action="store_true")
    args = parser.parse_args()

    settings = _load_settings()
    client = MongoClient(settings.mongo_uri)
    db = client[settings.mongo_default_db]
    collection = db[settings.mongo_collection]

    print(f"🔍 Vectorizing query...")
    q_emb = _ollama_embeddings(settings, args.query)

    print("📡 Fetching docs...")
    projection = {"_id": 0, "text": 1, "source_file": 1, "headers": 1, settings.embedding_field: 1}
    all_docs = list(collection.find({}, projection))

    if not all_docs:
        print("❌ No documents found. Run ingest.py first.")
        return

    print(f"🧮 Calculating similarity...")
    results = _cosine_top_k(q_emb, all_docs, settings.embedding_field, args.top_k)

    context_parts = [f"[{i+1}] {r['source_file']} ({r['headers']}):\n{r['text']}" for i, r in enumerate(results)]
    user_prompt = f"Context:\n{chr(10).join(context_parts)}\n\nQuestion: {args.query}"

    print("🚀 Generating answer via Groq (Turbo)...")
    try:
        # Groq is now the primary engine
        answer = _groq_chat(settings, _build_system_prompt(), user_prompt)
    except Exception as e:
        if args.no_groq:
            print(f"❌ Groq failed and --no-groq is set: {e}")
            return
        
        print(f"⚠️ Groq failed ({e}). Falling back to local Ollama (CPU will spike)...")
        # Ollama is now the backup
        answer = _ollama_chat(settings, _build_system_prompt(), user_prompt)

    print(f"\n--- ANSWER ---\n{answer}")

if __name__ == "__main__":
    main()