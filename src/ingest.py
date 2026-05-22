import argparse
import hashlib
import re
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import json
from confluent_kafka import Producer
from dotenv import load_dotenv
from langchain_core.documents import Document
from langchain_text_splitters import MarkdownHeaderTextSplitter
from pymongo import MongoClient
from tqdm import tqdm

from rag_engine import load_settings, Settings

# Load environment variables early for local development fallback
load_dotenv()

HEADER_LEVELS = [
    ("#", "h1"),
    ("##", "h2"),
    ("###", "h3"),
]

_ESC_PREFIX = "__RAG_ESC_HASH_"
_ESC_SUFFIX = "__"

def _read_text_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""

def _is_markdown(path: Path) -> bool:
    return path.suffix.lower() in {".md", ".markdown", ".mdx"}

def _is_code(path: Path) -> bool:
    return path.suffix.lower() in {".py", ".rs", ".js", ".ts", ".java", ".go", ".toml", ".yaml"}

EXCLUDED_DIRS = {
    "node_modules", ".git", "target", "build", "dist",
    ".next", "__pycache__", ".venv", "venv", "coverage",
    ".storage", ".cloud", "tts", "backups", "share", "custom_components",
    "hialt-recall/data"
}

def _iter_source_files(repos_dir: Path, max_files: Optional[int] = None) -> Iterator[Tuple[str, Path]]:
    if not repos_dir.exists():
        print(f"Warning: {repos_dir} not found. Creating it...")
        repos_dir.mkdir(parents=True, exist_ok=True)
        return

    count = 0
    for project_dir in sorted(repos_dir.iterdir()):
        if not project_dir.is_dir(): continue
        for path in sorted(project_dir.rglob("*")):
            if any(part in EXCLUDED_DIRS for part in path.parts):
                continue
            if path.is_file() and not path.name.startswith(".") and (_is_markdown(path) or _is_code(path)):
                yield project_dir.name, path
                count += 1
                if max_files and count >= max_files: return

def _escape_header_hashes_in_fenced_code(markdown: str) -> str:
    fence_re = re.compile(r"^\s*(```+|~~~+)")
    header_re = re.compile(r"^(\s{0,3})(#{1,6})(\s+)")
    in_fence = False
    fence_delim = None
    out_lines = []
    for line in markdown.splitlines(keepends=True):
        if not in_fence:
            m = fence_re.match(line)
            if m:
                in_fence = True
                fence_delim = m.group(1)
            out_lines.append(line)
            continue
        stripped = line.lstrip()
        if fence_delim and stripped.startswith(fence_delim):
            in_fence = False
            out_lines.append(line)
            continue
        m = header_re.match(line)
        if m:
            indent, hash_run, ws = m.group(1), m.group(2), m.group(3)
            out_lines.append(f"{indent}{_ESC_PREFIX}{len(hash_run)}{_ESC_SUFFIX}{ws}")
        else:
            out_lines.append(line)
    return "".join(out_lines)

def _unescape_header_hash_tokens_in_fenced_code(text: str) -> str:
    return text.replace(_ESC_PREFIX, "#" * 1).replace(_ESC_SUFFIX, "")

def _chunk_markdown(text: str) -> List[Tuple[str, str]]:
    escaped = _escape_header_hashes_in_fenced_code(text)
    splitter = MarkdownHeaderTextSplitter(headers_to_split_on=HEADER_LEVELS, strip_headers=False)
    docs = splitter.split_text(escaped)
    
    raw_chunks = []
    for doc in docs:
        parts = [doc.metadata.get(k, "") for _, k in HEADER_LEVELS if doc.metadata.get(k)]
        headers = " > ".join(parts) if parts else "Top"
        raw_chunks.append((doc.page_content.strip(), headers))

    capped = []
    max_chars = 400
    for chunk_text, headers in raw_chunks:
        if len(chunk_text) <= max_chars:
            capped.append((chunk_text, headers))
        else:
            for i in range(0, len(chunk_text), max_chars):
                sub = chunk_text[i:i + max_chars].strip()
                if sub:
                    capped.append((sub, headers))
    return capped

def _chunk_code(text: str) -> List[Tuple[str, str]]:
    cleaned = text.strip()
    if not cleaned:
        return []
    max_chars = 400
    chunks = []
    for i in range(0, len(cleaned), max_chars):
        chunk = cleaned[i:i + max_chars].strip()
        if chunk:
            chunks.append((chunk, "Code"))
    return chunks

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()

def _make_doc_id(project: str, source_file_rel: str, chunk_index: int, headers: str, text: str) -> str:
    key = f"{project}|{source_file_rel}|{chunk_index}|{headers}|{_sha256_hex(text)}"
    return _sha256_hex(key)

def kafka_delivery_report(err, msg):
    if err is not None:
        print(f"Kafka Delivery Failure: {err}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest local repos via Kafka.")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--max-files", type=int, default=None)
    args = parser.parse_args()

    settings = load_settings()

    if args.reset:
        client = MongoClient(settings.mongo_uri)
        db = client[settings.mongo_default_db]
        collection = db[settings.mongo_collection]
        collection.drop()
        print("MongoDB collection dropped for reset.")
        client.close()

    # Dynamic bootstrap target for port-forward vs in-cluster deployment
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    print(f"Kafka Producer initialized targeting broker: {bootstrap_servers}. Scanning dependencies...")

    files_iter = _iter_source_files(settings.data_repos_dir, args.max_files)
    
    for project, file_path in tqdm(files_iter, desc="Publishing to Kafka"):
        rel_path = file_path.relative_to(settings.data_repos_dir).as_posix()
        raw_text = _read_text_file(file_path)

        chunks = _chunk_markdown(raw_text) if _is_markdown(file_path) else _chunk_code(raw_text)

        for idx, (content, h_path) in enumerate(chunks):
            doc_id = _make_doc_id(project, rel_path, idx, h_path, content)
            
            payload = {
                "doc_id": doc_id,
                "project": project,
                "source_file": rel_path,
                "chunk_index": idx,  # Explicitly included for clear upsert handling
                "headers": h_path,
                "text": content
            }

            producer.produce(
                topic=os.environ.get('KAFKA_TOPIC', 'hialt-recall-chunks'),
                key=doc_id.encode('utf-8'),
                value=json.dumps(payload).encode('utf-8'),
                callback=kafka_delivery_report
            )
            producer.poll(0)

    print("\nFlushing remaining messages to Kafka broker...")
    producer.flush()
    print("Pipeline production complete! All files published to Kafka.")

if __name__ == "__main__":
    main()