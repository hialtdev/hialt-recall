# hialt-recall

A local-first RAG (Retrieval-Augmented Generation) pipeline for the hialt.dev home lab. Ask natural language questions about your own codebases and get cited answers grounded in your actual source files.

**Storage is PostgreSQL + pgvector**, on the shared `postgres-main` instance the platform runs in the `database` namespace. Embeddings live in a `vector(1024)` column and similarity search runs server-side against an HNSW index, so retrieval no longer scans every stored chunk. Migrated from MongoDB in September 2026.

---

## How it works

The pipeline is a two-stage producer/consumer split around Kafka, plus a retrieval layer shared by the CLI and the web UI:

1. `src/ingest.py` walks `./data/repos/<project>/**`, chunks Markdown files by headers and code files by character boundaries, and **publishes** each chunk (plus project/tag metadata) as a message to a Kafka topic. It does not touch Postgres or Ollama directly.
2. `src/rag_consumer.py` consumes that Kafka topic, embeds each chunk with Ollama (`mxbai-embed-large`), and upserts chunks + embeddings into the `chunks` table. It must be running (locally or in the cluster) for ingested chunks to actually become queryable — see [§6](#6-run-the-consumer).
3. `src/query.py` (CLI) / `src/app.py` (Streamlit UI) embed the user query, ask Postgres for the nearest chunks by cosine distance, and feed the top-k to Groq (fast, cloud LLM) with Ollama as a local fallback. Shared logic lives in `src/rag_engine.py`.

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
│   ├── rag_consumer.py    # Kafka consumer — embeds + writes to Postgres
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

- PostgreSQL accessible on `localhost:5432` via k3s port-forward. This is the shared `postgres-main` instance managed by CloudNativePG; `-rw` is the read/write primary service:

```bash
kubectl port-forward -n database svc/postgres-main-rw 5432:5432
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
# Postgres. hialt-recall has its own role and database on the shared
# instance — never point this at another service's credentials.
POSTGRES_URI=postgresql://hialt_recall:password@localhost:5432/hialt_recall

# Ollama (local embedding + LLM fallback)
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_EMBEDDING_MODEL=mxbai-embed-large
OLLAMA_LLM_MODEL=llama3

# Groq (primary LLM — fast, free tier, open source models)
GROQ_API_KEY=gsk_your_key_here
GROQ_MODEL=openai/gpt-oss-120b

# Postgres tables. EMBEDDING_DIM must match what OLLAMA_EMBEDDING_MODEL
# actually produces — a pgvector column has a fixed width, set when the
# table is created. mxbai-embed-large produces 1024 dimensions.
EMBEDDING_DIM=1024
POSTGRES_TABLE=chunks
POSTGRES_FAILED_TABLE=chunks_failed

LLM_TEMPERATURE=0.2

# Kafka — sits between ingest.py (producer) and rag_consumer.py (consumer)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=hialt-recall-chunks
KAFKA_CONSUMER_GROUP=hialt-recall-workers
```

No manual schema setup is needed. `rag_engine.ensure_schema()` runs at consumer startup and creates the `chunks` and `chunks_failed` tables, the `project_dir`/`project_name`/`tags` lookup indexes, and the HNSW index on `embedding`, all with `IF NOT EXISTS`.

The one thing it cannot create is the `vector` extension itself: pgvector is not a trusted extension, so the owning role is not allowed to install it. The platform provisions it declaratively instead, through the `extensions` block on the `Database` resource in `hialt-platform`. Against a database where the extension is missing, every connection fails with `vector type not found in the database`.

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

`ingest.py` only *publishes* chunks to Kafka — it does not embed or write to Postgres itself. The [consumer](#6-run-the-consumer) must be running for anything you ingest here to actually show up in queries.

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
  exclude_paths:            # relative files/directories to skip, on top of the global defaults
    - docs/generated
    - config/development-values.yaml
  include_extensions:       # if set, ONLY these extensions are ingested (overrides the markdown/code default)
    - .rs
    - .md
```

A missing or invalid manifest falls back to sane defaults (project name = directory name, no extra excludes, standard markdown/code extensions) — one bad manifest never blocks ingestion of the rest of your repos.

### Options

| Flag | Description |
|------|-------------|
| `--reset` | ⚠️ Truncate the chunks table and re-ingest from scratch |
| `--max-files N` | Cap total files processed (useful for testing) |
| `--project NAME` | Only ingest the project directory matching this name |

### ⚠️ Warning: --reset is destructive

`--reset` truncates the `chunks` table. All embeddings are permanently deleted and must be re-generated from scratch. It leaves `chunks_failed` alone, so dead-lettered rows survive a reset.

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

`rag_consumer.py` is the other half of the pipeline: it reads chunk messages off the Kafka topic, embeds each one via Ollama, and upserts the result into Postgres. It's a long-running process — start it before (or while) you run `ingest.py`, and leave it running:

```bash
source .venv/bin/activate
python3 src/rag_consumer.py
```

It commits Kafka offsets only after a successful Postgres write, so it's safe to `Ctrl+C` and restart — any in-flight or un-embedded messages will simply be reprocessed. In the cluster this runs as the `hialt-recall-consumer` deployment (see `k8s/deployment.yaml`).

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
psql "$POSTGRES_URI" -c \
  "SELECT project_name, count(*) FROM chunks GROUP BY project_name ORDER BY count DESC;"

# Anything that failed to embed or write ends up here
psql "$POSTGRES_URI" -c \
  "SELECT source_file, error, failed_at FROM chunks_failed ORDER BY failed_at DESC LIMIT 20;"
```

If any project has an unexpectedly large chunk count (tens of thousands), it likely swept up `node_modules` or a build directory. Use `--reset` and check your symlink targets.

---

## Notes on scaling

Retrieval now runs server-side against a pgvector HNSW index using cosine distance, so ranking cost no longer grows linearly with the size of the collection. That was the main motivation for leaving MongoDB, where every candidate had to be pulled into the client and scored with numpy.

Two things to know about the index. HNSW is approximate, so a low-recall result set is tuned with `hnsw.ef_search` rather than by adding candidates. And `--project`/`--tag` filters apply as a `WHERE` clause on top of the index scan, which can under-fill the result set when a filter is highly selective. If that shows up in practice, the usual fix is a partial index per project or raising `ef_search` for filtered queries.

For alternative retrieval directions being considered, see `ARCHITECTURE_BACKLOG_RUST_INTEGRATION.md`.

---

## Author

Robert Glasser — [hialt.dev](https://hialt.dev)
