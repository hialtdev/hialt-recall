# Local-First RAG (MongoDB + Ollama + numpy cosine)

A minimal local-first RAG pipeline designed for a home lab / k3s environment.

**No special MongoDB plugins or `mongot` required.** Embeddings are stored as plain arrays; similarity search is done entirely in-process with numpy cosine similarity.

## How it works

1. `ingest.py` walks `./data/repos/<project>/**`, chunks Markdown by headers, embeds with Ollama (`mxbai-embed-large`), and upserts chunks + raw embedding arrays into a plain MongoDB collection.
2. `query.py` embeds the user query, fetches all stored embeddings from MongoDB, ranks them with numpy cosine similarity, and feeds the top-k chunks to an Ollama LLM (fallback: Groq).

## 1. Directory layout

Place repos under:

```
data/repos/<project_name>/
```

`<project_name>` becomes the `project` metadata field. Example:

```bash
mkdir -p data/repos/foxwatch
# clone or copy repo files into data/repos/foxwatch/
```

## 2. Prerequisites

- Python 3.10+
- Ollama running locally at `http://localhost:11434`
  ```bash
  ollama pull mxbai-embed-large
  ollama pull llama3
  ```
- MongoDB accessible on `localhost:27017` — either:
  - **k3s / Kubernetes:** `kubectl port-forward svc/mongodb 27017:27017 -n <namespace>`
  - **Docker:** `docker run -p 27017:27017 mongo:7`

## 3. Configure environment

Copy the template and edit if needed (defaults work out of the box):

```bash
cp .env.example .env
```

The critical variable is `MONGO_URI`. The default is:

```
mongodb://localhost:27017/local_rag
```

No special MongoDB index setup is needed — embeddings are stored as plain arrays.

## 4. Install dependencies

```bash
pip3 install -r requirements.txt
```

## 5. Ingest documents

```bash
python3 ingest.py
```

Options:

| Flag | Description |
|------|-------------|
| `--reset` | Drop the collection and re-ingest from scratch |
| `--max-files N` | Cap total files processed (useful for testing) |
| `--max-bytes-per-file N` | Skip files larger than N bytes (default 2 MB) |

Ingestion is idempotent — chunks whose content hasn't changed are skipped on reruns.

## 6. Query

```bash
python3 query.py "What does the telemetry module do?"
```

Options:

| Flag | Description |
|------|-------------|
| `--top-k N` | Number of chunks to retrieve (default 3) |
| `--no-groq` | Disable Groq fallback if Ollama is unavailable |

The model is instructed to:
- Use **only** the retrieved context to answer
- Say it doesn't know if the answer isn't in the context
- Cite `source_file` and section `headers` in the response

## Notes on scaling

The current retriever fetches all documents and ranks in-process. This is fine for typical home lab repo collections (thousands of chunks). If you accumulate tens of thousands of chunks, consider adding a simple MongoDB index on `project` to pre-filter before fetching.
