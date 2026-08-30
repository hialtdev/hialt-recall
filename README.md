# hialt-recall

A local-first RAG (Retrieval-Augmented Generation) pipeline for the hialt.dev home lab. Ask natural language questions about your own codebases and get cited answers grounded in your actual source files.

**No special MongoDB plugins or `mongot` required.** Embeddings are stored as plain arrays; similarity search is done entirely in-process with numpy cosine similarity.

---

## How it works

The pipeline is a two-stage producer/consumer split around Kafka, plus a retrieval layer shared by the CLI and the web UI:

1. `src/ingest.py` walks `./data/repos/<project>/**`, chunks Markdown files by headers and code files by character boundaries, and **publishes** each chunk (plus project/tag metadata) as a message to a Kafka topic. It does not touch Mongo or Ollama directly.
2. `src/rag_consumer.py` consumes that Kafka topic, embeds each chunk with Ollama (`mxbai-embed-large`), and upserts chunks + raw embedding arrays into a plain MongoDB collection. It must be running (locally or in the cluster) for ingested chunks to actually become queryable — see [§6](#6-run-the-consumer).
3. `src/query.py` (CLI) / `src/app.py` (Streamlit UI) embed the user query, fetch stored embeddings from MongoDB, rank them with numpy cosine similarity, and feed the top-k chunks to Groq (fast, cloud LLM) with Ollama as a local fallback. Shared logic lives in `src/rag_engine.py`.

---

## 1. Directory layout

```
hialt-recall/
├── data/
│   └── repos/              # Symlinks to your local project directories
│       ├── foxwatch -> /home/user/RustroverProjects/foxwatch
│       ├── bitbybit-service -> /home/user/IdeaProjects/bitbybit-service
│       └── bitbybit-frontend -> /home/user/WebstormProjects/bitbybit-react-ts-portfolio
├── src/
│   ├── ingest.py          # Kafka producer — walks + chunks source files
│   ├── rag_consumer.py    # Kafka consumer — embeds + writes to MongoDB
│   ├── rag_engine.py      # Shared retrieval/generation logic
│   ├── query.py           # CLI entrypoint
│   └── app.py             # Streamlit UI entrypoint
├── k8s/                    # Deployment, ConfigMap, Kafka topic, Ollama, ingress
├── Dockerfile
├── deploy.sh                # Build + push to GHCR, restart k8s deployments
├── .github/workflows/       # CI/CD: build/push image, deploy on push to main
├── requirements.txt
├── .env
└── .env.example
```

Each project directory under `data/repos/` may also contain an optional `hialt-knowledge.yaml` manifest — see [§5](#5-ingest-documents) for what it controls.

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

- A Kafka broker accessible on `localhost:9092`. In the hialt.dev cluster this is a Strimzi-managed broker; port-forward it the same way:

```bash
# Note: use your actual Strimzi cluster's bootstrap service/namespace
kubectl port-forward -n kafka svc/bitbybit-kafka-kafka-bootstrap 9092:9092
```

Both port-forwards must stay open while ingesting or querying. Run ingest, the consumer, and query in separate terminals.

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

# Kafka — sits between ingest.py (producer) and rag_consumer.py (consumer)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=hialt-recall-chunks
KAFKA_CONSUMER_GROUP=hialt-recall-workers
```

No special MongoDB index setup is needed for the embeddings themselves — they're stored as plain arrays. `rag_consumer.py` does create a couple of lookup indexes (`doc_id`, `metadata.project`, `metadata.tags`) automatically on first startup.

---

## 4. Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`confluent-kafka` needs the `librdkafka` C library. Prebuilt wheels cover most platforms, but if the install fails compiling from source, install `librdkafka-dev` (Debian/Ubuntu) or `librdkafka` (Homebrew) first.

---

## 5. Ingest documents

`ingest.py` only *publishes* chunks to Kafka — it does not embed or write to Mongo itself. The [consumer](#6-run-the-consumer) must be running for anything you ingest here to actually show up in queries.

```bash
source .venv/bin/activate
python3 src/ingest.py
```

The ingester automatically excludes junk directories: `node_modules`, `.git`, `target`, `build`, `dist`, `__pycache__`, `.venv`, `coverage`, and a handful of other vendor/IDE/cache dirs (see `EXCLUDED_DIRS` in `src/ingest.py`) — these are always pruned, even if a manifest tries to un-exclude them.

File types ingested by default:

| Type | Extensions |
|------|-----------|
| Markdown | `.md`, `.markdown`, `.mdx` |
| Code | `.py`, `.rs`, `.js`, `.ts`, `.java`, `.go`, `.toml`, `.yaml` |

**Ingestion is idempotent** — chunks whose content hasn't changed are skipped on reruns. If you add a new project symlink or update files, just re-run `python3 src/ingest.py` without any flags.

### Per-project manifest — `hialt-knowledge.yaml`

Drop an optional `hialt-knowledge.yaml` at the root of any project under `data/repos/<project>/` to override ingestion for that project only:

```yaml
project:
  name: foxwatch          # display name shown in citations/filters (defaults to the dir name)
  tags: [rust, embedded]  # filterable in the UI and via --tag

ingestion:
  enabled: true            # set false to skip this project entirely
  exclude_paths:            # extra relative paths to prune, on top of the global defaults
    - docs/generated
  include_extensions:       # if set, ONLY these extensions are ingested (overrides the markdown/code default)
    - .rs
    - .md
```

A missing or invalid manifest falls back to sane defaults (project name = directory name, no extra excludes, standard markdown/code extensions) — one bad manifest never blocks ingestion of the rest of your repos.

### Options

| Flag | Description |
|------|-------------|
| `--reset` | ⚠️ Drop the entire collection and re-ingest from scratch |
| `--max-files N` | Cap total files processed (useful for testing) |
| `--project NAME` | Only ingest the project directory matching this name |

### ⚠️ Warning: --reset is destructive

`--reset` drops your entire MongoDB collection. All embeddings are permanently deleted and must be re-generated from scratch.

On an HP EliteDesk i5, a full ingest of 3 projects (~750 chunks) takes approximately **10 minutes** end-to-end (publish + consume + embed). Plan accordingly before using this flag.

**When you need `--reset`:**
- You changed the chunking strategy
- You want to remove a project that was ingested with the wrong exclusions (e.g. `node_modules` was swept up)
- You want a completely clean slate

**When you do NOT need `--reset`:**
- Adding a new project — add the symlink and re-run `python3 src/ingest.py`
- Updating files in an existing project — changed chunks are re-ingested, unchanged ones are skipped
- Resuming an interrupted ingest — just re-run without `--reset`

---

## 6. Run the consumer

`rag_consumer.py` is the other half of the pipeline: it reads chunk messages off the Kafka topic, embeds each one via Ollama, and upserts the result into MongoDB. It's a long-running process — start it before (or while) you run `ingest.py`, and leave it running:

```bash
source .venv/bin/activate
python3 src/rag_consumer.py
```

It commits Kafka offsets only after a successful MongoDB write, so it's safe to `Ctrl+C` and restart — any in-flight or un-embedded messages will simply be reprocessed. In the cluster this runs as the `hialt-recall-consumer` deployment (see `k8s/deployment.yaml`).

---

## 7. Query

```bash
python3 src/query.py "What does the telemetry module do?"
```

Groq is the primary LLM (fast, ~0.3 seconds). Ollama is the local fallback if Groq is unavailable or not configured.

### Options

| Flag | Description |
|------|-------------|
| `--top-k N` | Number of chunks to retrieve (default 3). Use 5-10 for complex cross-project questions |
| `--no-groq` | Disable Groq, force local Ollama only |
| `--project NAME` | Restrict retrieval to chunks whose `metadata.project` matches this name |
| `--tag TAG` | Restrict retrieval to chunks whose `metadata.tags` contains this tag |

The model is instructed to:
- Answer **only** from the retrieved context
- Say "I don't know" if the answer isn't in the context (hallucination guard)
- Cite `source_file` and section `headers` in every response

Prefer a UI? `streamlit run src/app.py` exposes the same pipeline — including the project/tag filters — with a browsable citation view and query history.

### Example queries

```bash
# Single project, specific fact
python3 src/query.py "What MQTT topic does foxwatch subscribe to?"

# Cross-project architectural question
python3 src/query.py "What are the common patterns I use for error handling across my Rust and Java projects?" --top-k 10

# Scoped to one project via its manifest tag
python3 src/query.py "How is auth handled?" --tag rust

# Hallucination guard test — should say "I don't know"
python3 src/query.py "Does foxwatch support gRPC?"
```

---

## 8. Validation checklist

After a fresh ingest, run these to confirm everything is working:

```bash
# Check chunk count by project
python3 -c "
from dotenv import load_dotenv
from pymongo import MongoClient
import os
load_dotenv('.env')
client = MongoClient(os.environ['MONGO_URI'])
db = client[os.environ.get('MONGO_DB_NAME', 'local_rag')]
coll = db[os.environ.get('MONGO_COLLECTION', 'rag_chunks')]
pipeline = [
    {'\$group': {'_id': '\$metadata.project', 'count': {'\$sum': 1}}},
    {'\$sort': {'count': -1}}
]
for doc in coll.aggregate(pipeline):
    print(doc)
"
```

If any project has an unexpectedly large chunk count (tens of thousands), it likely swept up `node_modules` or a build directory. Use `--reset` and check your symlink targets.

---

## Notes on scaling

The current retriever fetches all matching documents and ranks them in-process with numpy — it does not use a vector index. This is fine for home lab repo collections (hundreds to low thousands of chunks), especially combined with the `--project`/`--tag` filters (backed by the `metadata.project`/`metadata.tags` indexes `rag_consumer.py` creates automatically) to narrow the candidate set before ranking. If you accumulate tens of thousands of legitimate chunks and retrieval latency becomes noticeable, that's the point to look at a real ANN index (MongoDB Atlas Vector Search, or an embedded engine — see `ARCHITECTURE_BACKLOG_RUST_INTEGRATION.md` for one direction being considered).

---

## Author

Robert Glasser — [hialt.dev](https://hialt.dev)