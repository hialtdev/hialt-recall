# hialt-recall

A local-first RAG (Retrieval-Augmented Generation) pipeline for the hialt.dev home lab. Ask natural language questions about your own codebases and get cited answers grounded in your actual source files.

**No special MongoDB plugins or `mongot` required.** Embeddings are stored as plain arrays; similarity search is done entirely in-process with numpy cosine similarity.

---

## How it works

1. `ingest.py` walks `./data/repos/<project>/**`, chunks Markdown files by headers and code files by character boundaries, embeds each chunk with Ollama (`mxbai-embed-large`), and upserts chunks + raw embedding arrays into a plain MongoDB collection.
2. `query.py` embeds the user query, fetches all stored embeddings from MongoDB, ranks them with numpy cosine similarity, and feeds the top-k chunks to Groq (fast, cloud LLM) with Ollama as a local fallback.

---

## 1. Directory layout

```
hialt-recall/
├── data/
│   └── repos/              # Symlinks to your local project directories
│       ├── foxwatch -> /home/user/RustroverProjects/foxwatch
│       ├── bitbybit-service -> /home/user/IdeaProjects/bitbybit-service
│       └── bitbybit-frontend -> /home/user/WebstormProjects/bitbybit-react-ts-portfolio
├── ingest.py
├── query.py
├── requirements.txt
├── .env
└── .env.example
```

The directory name under `data/repos/` becomes the `project` metadata field on every chunk — so query results cite which project the answer came from.

**Use symlinks, not copies.** This keeps your source of truth in one place:

```bash
cd data/repos
ln -s /path/to/your/project project-name
```

---

## 2. Prerequisites

- Python 3.10+
- Ollama running locally at `http://localhost:11434`

```bash
ollama pull mxbai-embed-large   # embedding model
ollama pull llama3              # local LLM fallback
```

- MongoDB accessible on `localhost:27017` via k3s port-forward:

```bash
# Note: use your actual k3s service name
kubectl port-forward svc/mongodb-service 27017:27017
```

This terminal must stay open while ingesting or querying. Run ingest and query in a second terminal.

- (Recommended) A free [Groq API key](https://console.groq.com) for fast query responses.

---

## 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env` — the critical variables:

```env
# MongoDB with authentication (required for k3s-hosted MongoDB)
MONGO_URI=mongodb://username:password@localhost:27017/local_rag?authSource=admin

# Ollama (local embedding + LLM fallback)
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_EMBEDDING_MODEL=mxbai-embed-large
OLLAMA_LLM_MODEL=llama3

# Groq (primary LLM — fast, free tier, open source models)
GROQ_API_KEY=gsk_your_key_here
GROQ_MODEL=openai/gpt-oss-120b

# MongoDB collection
MONGO_DB_NAME=local_rag
MONGO_COLLECTION=rag_chunks
MONGO_EMBEDDING_FIELD=embedding

LLM_TEMPERATURE=0.2
```

No special MongoDB index setup is needed — embeddings are stored as plain arrays.

---

## 4. Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 5. Ingest documents

```bash
source .venv/bin/activate
python3 ingest.py
```

The ingester automatically excludes junk directories: `node_modules`, `.git`, `target`, `build`, `dist`, `__pycache__`, `.venv`, `coverage`.

File types ingested:

| Type | Extensions |
|------|-----------|
| Markdown | `.md`, `.markdown`, `.mdx` |
| Code | `.py`, `.rs`, `.js`, `.ts`, `.java`, `.go`, `.toml`, `.yaml` |

**Ingestion is idempotent** — chunks whose content hasn't changed are skipped on reruns. If you add a new project symlink or update files, just re-run `python3 ingest.py` without any flags.

### Options

| Flag | Description |
|------|-------------|
| `--reset` | ⚠️ Drop the entire collection and re-ingest from scratch |
| `--max-files N` | Cap total files processed (useful for testing) |

### ⚠️ Warning: --reset is destructive

`--reset` drops your entire MongoDB collection. All embeddings are permanently deleted and must be re-generated from scratch.

On an HP EliteDesk i5, a full ingest of 3 projects (~750 chunks) takes approximately **10 minutes**. Plan accordingly before using this flag.

**When you need `--reset`:**
- You changed the chunking strategy
- You want to remove a project that was ingested with the wrong exclusions (e.g. `node_modules` was swept up)
- You want a completely clean slate

**When you do NOT need `--reset`:**
- Adding a new project — add the symlink and re-run `python3 ingest.py`
- Updating files in an existing project — changed chunks are re-ingested, unchanged ones are skipped
- Resuming an interrupted ingest — just re-run without `--reset`

---

## 6. Query

```bash
python3 query.py "What does the telemetry module do?"
```

Groq is the primary LLM (fast, ~0.3 seconds). Ollama is the local fallback if Groq is unavailable or not configured.

### Options

| Flag | Description |
|------|-------------|
| `--top-k N` | Number of chunks to retrieve (default 3). Use 5-10 for complex cross-project questions |
| `--no-groq` | Disable Groq, force local Ollama only |

The model is instructed to:
- Answer **only** from the retrieved context
- Say "I don't know" if the answer isn't in the context (hallucination guard)
- Cite `source_file` and section `headers` in every response

### Example queries

```bash
# Single project, specific fact
python3 query.py "What MQTT topic does foxwatch subscribe to?"

# Cross-project architectural question
python3 query.py "What are the common patterns I use for error handling across my Rust and Java projects?" --top-k 10

# Hallucination guard test — should say "I don't know"
python3 query.py "Does foxwatch support gRPC?"
```

---

## 7. Validation checklist

After a fresh ingest, run these to confirm everything is working:

```bash
# Check chunk count by project
python3 -c "
from dotenv import load_dotenv
from pymongo import MongoClient
import os
load_dotenv('.env')
client = MongoClient(os.environ['MONGO_URI'])
db = client['local_rag']
pipeline = [
    {'\$group': {'_id': '\$project', 'count': {'\$sum': 1}}},
    {'\$sort': {'count': -1}}
]
for doc in db.rag_chunks.aggregate(pipeline):
    print(doc)
"
```

If any project has an unexpectedly large chunk count (tens of thousands), it likely swept up `node_modules` or a build directory. Use `--reset` and check your symlink targets.

---

## Notes on scaling

The current retriever fetches all documents and ranks in-process with numpy. This is fine for home lab repo collections (hundreds to low thousands of chunks). If you accumulate tens of thousands of legitimate chunks, consider adding a MongoDB index on `project` to pre-filter before fetching.

---

## Author

Robert Glasser — [hialt.dev](https://hialt.dev)