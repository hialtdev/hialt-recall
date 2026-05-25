import argparse
import hashlib
import logging
import re
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import json
import yaml
from confluent_kafka import Producer
from dotenv import load_dotenv
from langchain_core.documents import Document
from langchain_text_splitters import MarkdownHeaderTextSplitter
from pymongo import MongoClient
from tqdm import tqdm

from rag_engine import load_settings, Settings

# Load environment variables early for local development fallback
load_dotenv()

log = logging.getLogger("hialt.ingest")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

MANIFEST_FILENAME = "hialt-knowledge.yaml"

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


# ---------------------------------------------------------------------------
# Global exclusion defaults — applied when no manifest is present AND as an
# always-on safety net even when a manifest exists (manifest can only ADD to
# these, not remove them, so vendor noise never leaks in accidentally).
# ---------------------------------------------------------------------------

EXCLUDED_DIRS: frozenset[str] = frozenset({
    "node_modules", ".git", "target", "build", "dist",
    ".next", "__pycache__", ".venv", "venv", "coverage",
    ".storage", ".cloud", "tts", "backups", "share", "custom_components",
    "hialt-recall/data",
    # Additional safe-defaults not in the original set:
    "vendor", "third_party", ".terraform", ".idea", ".vscode",
    ".mypy_cache", ".ruff_cache", ".pytest_cache", ".tox",
})


# ---------------------------------------------------------------------------
# Manifest dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IngestionManifest:
    """Parsed, validated representation of hialt-knowledge.yaml."""

    project_name: str
    project_tags: list[str] = field(default_factory=list)
    enabled: bool = True
    # Normalised (no leading/trailing slash) relative paths to prune
    exclude_paths: frozenset[str] = field(default_factory=frozenset)
    # Empty == allow all extensions that pass _is_markdown/_is_code checks
    include_extensions: frozenset[str] = field(default_factory=frozenset)

    @classmethod
    def from_dict(cls, data: dict, fallback_name: str) -> "IngestionManifest":
        project  = data.get("project",   {}) or {}
        ingestion = data.get("ingestion", {}) or {}

        raw_excludes: list[str] = ingestion.get("exclude_paths", [])       or []
        raw_exts:     list[str] = ingestion.get("include_extensions", [])  or []

        return cls(
            project_name=str(project.get("name", fallback_name)).strip(),
            project_tags=[str(t).strip() for t in (project.get("tags") or [])],
            enabled=bool(ingestion.get("enabled", True)),
            exclude_paths=frozenset(
                _normalise_path(p) for p in raw_excludes if p
            ),
            include_extensions=frozenset(
                (e if e.startswith(".") else f".{e}").lower()
                for e in raw_exts if e
            ),
        )

    @classmethod
    def default(cls, project_name: str) -> "IngestionManifest":
        return cls(project_name=project_name)


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _normalise_path(raw: str) -> str:
    """Strip leading/trailing slashes and whitespace for safe prefix matching."""
    return raw.strip().strip("/").strip("\\")



# ---------------------------------------------------------------------------
# Manifest loading
# ---------------------------------------------------------------------------

def _load_manifest(project_root: Path) -> IngestionManifest:
    """
    Parse hialt-knowledge.yaml at the root of *project_root* if it exists.
    Falls back to IngestionManifest.default() on any error so that one bad
    manifest never blocks the rest of the pipeline.
    """
    manifest_path = project_root / MANIFEST_FILENAME
    fallback_name = project_root.name

    if not manifest_path.is_file():
        log.debug("No manifest in %s — using defaults.", project_root.name)
        return IngestionManifest.default(fallback_name)

    try:
        with manifest_path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
        if not isinstance(raw, dict):
            raise ValueError("Manifest root must be a YAML mapping.")
        manifest = IngestionManifest.from_dict(raw, fallback_name)
        log.info(
            "Manifest loaded: project='%s' tags=%s enabled=%s "
            "exclude_paths=%d include_extensions=%d",
            manifest.project_name, manifest.project_tags, manifest.enabled,
            len(manifest.exclude_paths), len(manifest.include_extensions),
        )
        return manifest
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Bad manifest at %s: %s — falling back to defaults.", manifest_path, exc
        )
        return IngestionManifest.default(fallback_name)


# ---------------------------------------------------------------------------
# Directory exclusion check
# ---------------------------------------------------------------------------

def _is_excluded_dir(dir_name: str, rel_dir: str, manifest: IngestionManifest) -> bool:
    """
    Return True if a directory should be pruned from the os.walk descent.

    Checks (in priority order):
      1. Global EXCLUDED_DIRS — leaf name match (always applied).
      2. Manifest exclude_paths — supports both leaf names ("vendor") and
         relative sub-paths ("docs/generated") via prefix matching.
    """
    # 1. Global safety net — leaf name only
    if dir_name in EXCLUDED_DIRS:
        return True

    # 2. Manifest-specified paths
    candidate = _normalise_path(os.path.join(rel_dir, dir_name))
    for excluded in manifest.exclude_paths:
        if candidate == excluded or candidate.startswith(excluded + "/"):
            return True

    return False


# ---------------------------------------------------------------------------
# Extension filter
# ---------------------------------------------------------------------------

def _is_allowed_file(path: Path, manifest: IngestionManifest) -> bool:
    """
    Return True when a file should be ingested.

    If the manifest specifies include_extensions, ONLY those extensions are
    allowed (full allow-list mode).  Otherwise, fall back to the original
    _is_markdown / _is_code predicate so existing behaviour is unchanged.
    """
    if manifest.include_extensions:
        return path.suffix.lower() in manifest.include_extensions
    # Original behaviour: markdown or recognised code extensions
    return _is_markdown(path) or _is_code(path)


# ---------------------------------------------------------------------------
# Manifest-driven directory walker (replaces _iter_source_files)
# ---------------------------------------------------------------------------

def _iter_source_files(
    repos_dir: Path,
    max_files: Optional[int] = None,
) -> Iterator[Tuple[str, Path, IngestionManifest]]:
    """
    Iterate valid source files under *repos_dir*, one project at a time.

    Yields ``(project_dir_name, absolute_file_path, manifest)`` so that
    callers have full access to the parsed manifest (project name, tags, …)
    when building Kafka payloads.

    Key behavioural guarantees:
    - Each top-level subdirectory of *repos_dir* is treated as a project.
    - hialt-knowledge.yaml is loaded (or defaulted) once per project.
    - ``ingestion.enabled: false`` skips the entire project tree.
    - ``dirs[:]`` is mutated in-place inside os.walk so excluded subtrees
      (e.g. node_modules with 40 000 files) are never stat()'d at all.
    - The original ``project_dir.name`` is always yielded as the identifier
      so that ``_make_doc_id`` hashing remains stable across runs even if
      the manifest renames the project for display purposes.
    """
    if not repos_dir.exists():
        log.warning("%s not found — creating it.", repos_dir)
        repos_dir.mkdir(parents=True, exist_ok=True)
        return

    count = 0
    repos_dir_str = str(repos_dir)

    for project_dir in sorted(repos_dir.iterdir()):
        if not project_dir.is_dir():
            continue

        manifest = _load_manifest(project_dir)

        if not manifest.enabled:
            log.info("Project '%s' disabled via manifest — skipping.", manifest.project_name)
            continue

        project_root_str = str(project_dir)

        for dirpath, dirs, files in os.walk(project_root_str, topdown=True):
            # Relative path of the directory currently being walked
            rel_dir = os.path.relpath(dirpath, project_root_str)
            rel_dir = "" if rel_dir == "." else rel_dir

            # ── Prune dirs in-place ────────────────────────────────────────
            # Mutating dirs[:] with topdown=True stops os.walk from ever
            # entering excluded subtrees — critical for vendor/node_modules.
            dirs[:] = sorted(
                d for d in dirs
                if not d.startswith(".")          # skip hidden dirs always
                and not _is_excluded_dir(d, rel_dir, manifest)
            )

            for filename in sorted(files):
                if filename.startswith("."):
                    continue

                file_path = Path(dirpath) / filename

                if not _is_allowed_file(file_path, manifest):
                    continue

                yield project_dir.name, file_path, manifest
                count += 1
                if max_files and count >= max_files:
                    return


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
    parser.add_argument(
        "--project",
        default=None,
        help="Only ingest this project directory name (optional filter).",
    )
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

    # _iter_source_files now yields (project_dir_name, path, manifest)
    for project, file_path, manifest in tqdm(files_iter, desc="Publishing to Kafka"):
        # Optional single-project filter (mirrors --project CLI arg)
        if args.project and project != args.project:
            continue

        rel_path = file_path.relative_to(settings.data_repos_dir).as_posix()
        raw_text = _read_text_file(file_path)

        chunks = _chunk_markdown(raw_text) if _is_markdown(file_path) else _chunk_code(raw_text)

        for idx, (content, h_path) in enumerate(chunks):
            doc_id = _make_doc_id(project, rel_path, idx, h_path, content)

            payload = {
                "doc_id": doc_id,
                "project": project,                 # kept at top level for backwards compat
                "source_file": rel_path,
                "chunk_index": idx,
                "headers": h_path,
                "text": content,
                # ── Rich metadata block ────────────────────────────────────
                # Nested under "metadata" so the retrieval layer can query:
                #   {"metadata.project": ...}  and  {"metadata.tags": ...}
                # Top-level "project" field is preserved so existing consumers
                # don't break during the migration window.
                "metadata": {
                    "project": manifest.project_name,
                    "tags":    manifest.project_tags,
                },
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