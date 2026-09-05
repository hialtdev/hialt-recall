# rag_consumer.py
"""
Kafka consumer for hialt-recall.

Reads raw chunk payloads from the ingest topic, embeds them via Ollama,
and upserts the result into Postgres (via pgvector).

Manual offset commit ensures at-least-once delivery for the common case:
the Kafka offset is only advanced after a verified Postgres write.

Kafka offsets are a single monotonic pointer per partition, not a per-message
ack — committing message N+1 also implicitly "consumes" any earlier message
whose commit you skipped. That means silently leaving a failed message
uncommitted and moving on (relying on "the next restart" to retry it) does
NOT work in general: as soon as any later message in the same partition
commits successfully, the earlier failure is gone for good, restart or not.

So a failed message is retried a bounded number of times in-process first
(no need to re-fetch it from Kafka — we already have it in hand). If it still
fails, it's recorded in the dead-letter table and the offset is committed
anyway, so one bad chunk can't wedge the partition forever. Check that table
periodically for anything that needs manual reprocessing.
"""

import json
import logging
import os
import time
from typing import Optional

import psycopg2
import requests
from confluent_kafka import Consumer, KafkaError
from pgvector import Vector
from dotenv import load_dotenv

from rag_engine import load_settings, get_connection, ensure_schema, Settings

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("hialt.consumer")

# How long to wait between in-process retry attempts (seconds); multiplied
# by the attempt number for a touch of backoff.
RETRY_BACKOFF = 2

# How many times to retry a single message in-process before dead-lettering it.
MAX_ATTEMPTS = 3


def _embed(ollama_base_url: str, model: str, text: str) -> list[float]:
    """Call Ollama /api/embeddings and return the vector."""
    resp = requests.post(
        f"{ollama_base_url}/api/embeddings",
        json={"model": model, "prompt": text},
        timeout=120,
    )
    resp.raise_for_status()
    return [float(x) for x in resp.json()["embedding"]]


def _upsert_chunk(conn, settings: Settings, payload: dict, embedding: list[float]) -> None:
    """
    Insert or update one chunk row, keyed by doc_id (the stable SHA-256 key
    from ingest.py). Equivalent to the old Mongo update_one(upsert=True).
    """
    metadata = payload.get("metadata") or {}
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {settings.postgres_table}
                (doc_id, project_dir, project_name, tags, source_file,
                 chunk_index, headers, text, embedding, updated_at)
            VALUES (%(doc_id)s, %(project_dir)s, %(project_name)s, %(tags)s,
                    %(source_file)s, %(chunk_index)s, %(headers)s, %(text)s,
                    %(embedding)s, now())
            ON CONFLICT (doc_id) DO UPDATE SET
                project_dir  = EXCLUDED.project_dir,
                project_name = EXCLUDED.project_name,
                tags         = EXCLUDED.tags,
                source_file  = EXCLUDED.source_file,
                chunk_index  = EXCLUDED.chunk_index,
                headers      = EXCLUDED.headers,
                text         = EXCLUDED.text,
                embedding    = EXCLUDED.embedding,
                updated_at   = now();
            """,
            {
                "doc_id": payload["doc_id"],
                "project_dir": payload.get("project", ""),
                "project_name": metadata.get("project") or payload.get("project", ""),
                "tags": metadata.get("tags") or [],
                "source_file": payload.get("source_file", ""),
                "chunk_index": payload.get("chunk_index", 0),
                "headers": payload.get("headers", ""),
                "text": payload.get("text", ""),
                # Vector(), not a plain list — psycopg2 renders a list as
                # ARRAY[...], which is not assignable to a `vector` column.
                "embedding": Vector(embedding),
            },
        )


def _record_failure(conn, settings: Settings, payload: dict, error: str) -> None:
    """Write a dead-letter row for a chunk that exhausted MAX_ATTEMPTS."""
    metadata = payload.get("metadata") or {}
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {settings.postgres_failed_table}
                (doc_id, project_dir, project_name, tags, source_file,
                 chunk_index, headers, text, error, failed_at)
            VALUES (%(doc_id)s, %(project_dir)s, %(project_name)s, %(tags)s,
                    %(source_file)s, %(chunk_index)s, %(headers)s, %(text)s,
                    %(error)s, now())
            ON CONFLICT (doc_id) DO UPDATE SET
                error     = EXCLUDED.error,
                failed_at = EXCLUDED.failed_at;
            """,
            {
                "doc_id": payload.get("doc_id"),
                "project_dir": payload.get("project", ""),
                "project_name": metadata.get("project") or "",
                "tags": metadata.get("tags") or [],
                "source_file": payload.get("source_file", ""),
                "chunk_index": payload.get("chunk_index", 0),
                "headers": payload.get("headers", ""),
                "text": payload.get("text", ""),
                "error": error,
            },
        )


def run_consumer() -> None:
    settings = load_settings()
    ensure_schema(settings)

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer_group    = os.environ.get("KAFKA_CONSUMER_GROUP",    "hialt-recall-workers")

    # ── Topic must match ingest.py's default exactly ───────────────────────
    kafka_topic = os.environ.get("KAFKA_TOPIC", "hialt-recall-chunks")

    consumer = Consumer({
        "bootstrap.servers":  bootstrap_servers,
        "group.id":           consumer_group,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,   # only commit after verified DB write
    })
    consumer.subscribe([kafka_topic])

    # Single long-lived connection for the life of the consumer, same shape
    # as the old long-lived MongoClient. autocommit=True so each statement
    # is its own transaction — a failed insert doesn't poison a later one,
    # which matters for the per-message retry loop below.
    conn = get_connection(settings, autocommit=True)

    # Use settings.embedding_model so OLLAMA_EMBEDDING_MODEL env var controls
    # both ingest-time and query-time embedding — they must match.
    ollama_base_url = settings.ollama_base_url.rstrip("/")
    embedding_model = settings.embedding_model

    log.info("=== Hialt-Recall Kafka Consumer Online ===")
    log.info("Broker: %s  |  Topic: %s  |  Group: %s", bootstrap_servers, kafka_topic, consumer_group)
    log.info("Embedding model: %s via %s", embedding_model, ollama_base_url)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                break

            # ── Decode payload ─────────────────────────────────────────────
            try:
                payload: dict = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                log.error("Unparseable message — skipping. Error: %s", exc)
                consumer.commit(msg, asynchronous=False)
                continue

            doc_id      = payload.get("doc_id")
            project     = payload.get("project")
            source_file = payload.get("source_file")
            chunk_index = payload.get("chunk_index", 0)
            content     = payload.get("text", "").strip()

            file_ident = f"{project} → {source_file} [chunk {chunk_index}]"

            if not content:
                log.warning("Empty text payload — skipping %s", file_ident)
                consumer.commit(msg, asynchronous=False)
                continue

            if not doc_id:
                log.warning("Missing doc_id — skipping %s", file_ident)
                consumer.commit(msg, asynchronous=False)
                continue

            log.info("Processing: %s", file_ident)

            # ── Embed + upsert, with bounded in-process retry ──────────────
            # We already hold the full message in memory, so retrying just
            # re-runs the embed/write below — no need to touch Kafka until
            # we're ready to commit (success) or dead-letter (exhausted).
            last_exc: Optional[Exception] = None

            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    # 1. Embed via Ollama using the settings-driven model name
                    embedding = _embed(ollama_base_url, embedding_model, content)

                    # 2. Upsert on doc_id — the stable SHA-256 key from ingest.py.
                    #    ON CONFLICT handles file renames correctly (a renamed
                    #    file gets a new doc_id, leaving the old row to age out
                    #    or be pruned by a separate job).
                    _upsert_chunk(conn, settings, payload, embedding)

                    # 3. Commit Kafka offset only after the DB write is confirmed
                    consumer.commit(msg, asynchronous=False)
                    log.info("✓ Upserted %s (attempt %d/%d)", doc_id, attempt, MAX_ATTEMPTS)
                    break

                except requests.RequestException as exc:
                    last_exc = exc
                    log.warning("Attempt %d/%d: Ollama embedding failed for %s: %s",
                                attempt, MAX_ATTEMPTS, file_ident, exc)

                except psycopg2.Error as exc:
                    last_exc = exc
                    log.warning("Attempt %d/%d: Postgres write failed for %s: %s",
                                attempt, MAX_ATTEMPTS, file_ident, exc)

                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    log.warning("Attempt %d/%d: Unexpected error processing %s: %s",
                                attempt, MAX_ATTEMPTS, file_ident, exc, exc_info=True)

                if attempt < MAX_ATTEMPTS:
                    time.sleep(RETRY_BACKOFF * attempt)

            else:
                # Loop completed without `break` — every attempt failed.
                # Record it for manual follow-up, then commit forward so this
                # one bad chunk doesn't block everything behind it forever.
                log.error(
                    "Giving up on %s after %d attempts — moving to dead-letter. Last error: %s",
                    file_ident, MAX_ATTEMPTS, last_exc,
                )
                try:
                    _record_failure(conn, settings, payload, str(last_exc))
                except psycopg2.Error:
                    log.error(
                        "Also failed to write dead-letter record for %s — this chunk is being lost.",
                        file_ident, exc_info=True,
                    )
                consumer.commit(msg, asynchronous=False)

    except KeyboardInterrupt:
        log.info("Gracefully shutting down RAG worker daemon…")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run_consumer()
