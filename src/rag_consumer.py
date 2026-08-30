# rag_consumer.py
"""
Kafka consumer for hialt-recall.

Reads raw chunk payloads from the ingest topic, embeds them via Ollama,
and upserts the result (including the metadata block) into MongoDB.

Manual offset commit ensures at-least-once delivery for the common case:
the Kafka offset is only advanced after a verified MongoDB write.

Kafka offsets are a single monotonic pointer per partition, not a per-message
ack — committing message N+1 also implicitly "consumes" any earlier message
whose commit you skipped. That means silently leaving a failed message
uncommitted and moving on (relying on "the next restart" to retry it) does
NOT work in general: as soon as any later message in the same partition
commits successfully, the earlier failure is gone for good, restart or not.

So a failed message is retried a bounded number of times in-process first
(no need to re-fetch it from Kafka — we already have it in hand). If it still
fails, it's recorded in a `<collection>_failed` dead-letter collection and
the offset is committed anyway, so one bad chunk can't wedge the partition
forever. Check that collection periodically for anything that needs manual
reprocessing.
"""

import json
import logging
import os
import sys
import time
from typing import Optional

import requests
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

from rag_engine import load_settings

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


def _ensure_indexes(coll) -> None:
    """
    Create indexes the first time the consumer starts.

    - Unique index on doc_id  → correct upsert key, prevents duplicates,
                                 O(1) lookup instead of full collection scan.
    - Index on metadata.project → makes project_filter queries efficient.
    - Index on metadata.tags    → makes tag_filter $in queries efficient.

    create_index() is idempotent — safe to call on every startup.
    """
    coll.create_index([("doc_id", ASCENDING)], unique=True, name="idx_doc_id")
    coll.create_index([("metadata.project", ASCENDING)], name="idx_metadata_project")
    coll.create_index([("metadata.tags",    ASCENDING)], name="idx_metadata_tags")
    log.info("MongoDB indexes verified.")


def _embed(ollama_base_url: str, model: str, text: str) -> list[float]:
    """Call Ollama /api/embeddings and return the vector."""
    resp = requests.post(
        f"{ollama_base_url}/api/embeddings",
        json={"model": model, "prompt": text},
        timeout=120,
    )
    resp.raise_for_status()
    return [float(x) for x in resp.json()["embedding"]]


def run_consumer() -> None:
    settings = load_settings()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer_group    = os.environ.get("KAFKA_CONSUMER_GROUP",    "hialt-recall-workers")

    # ── Topic must match ingest.py's default exactly ───────────────────────
    # ingest.py defaults to "hialt-recall-chunks"; the old consumer defaulted
    # to "rag-raw-chunks".  Both now read from the same env var with the same
    # fallback so they stay in sync without manual co-ordination.
    kafka_topic = os.environ.get("KAFKA_TOPIC", "hialt-recall-chunks")

    consumer = Consumer({
        "bootstrap.servers":  bootstrap_servers,
        "group.id":           consumer_group,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,   # only commit after verified DB write
    })
    consumer.subscribe([kafka_topic])

    mongo_client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    db   = mongo_client[settings.mongo_default_db]
    coll = db[settings.mongo_collection]
    dead_letter_coll = db[f"{settings.mongo_collection}_failed"]

    _ensure_indexes(coll)
    dead_letter_coll.create_index([("doc_id", ASCENDING)], unique=True, name="idx_doc_id")

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

                    # 2. Build the document to write.
                    #    payload already contains the nested "metadata" block written
                    #    by ingest.py:  {"metadata": {"project": ..., "tags": [...]}}
                    #    We add the embedding and write everything atomically.
                    doc = {**payload, settings.embedding_field: embedding}

                    # 3. Upsert on doc_id — the stable SHA-256 key from ingest.py.
                    #    This is O(1) via the unique index and handles file renames
                    #    correctly (a renamed file gets a new doc_id, leaving the
                    #    old document to age out or be pruned by a separate job).
                    coll.update_one(
                        {"doc_id": doc_id},
                        {"$set": doc},
                        upsert=True,
                    )

                    # 4. Commit Kafka offset only after the DB write is confirmed
                    consumer.commit(msg, asynchronous=False)
                    log.info("✓ Upserted %s (attempt %d/%d)", doc_id, attempt, MAX_ATTEMPTS)
                    break

                except requests.RequestException as exc:
                    last_exc = exc
                    log.warning("Attempt %d/%d: Ollama embedding failed for %s: %s",
                                attempt, MAX_ATTEMPTS, file_ident, exc)

                except PyMongoError as exc:
                    last_exc = exc
                    log.warning("Attempt %d/%d: MongoDB write failed for %s: %s",
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
                    dead_letter_coll.update_one(
                        {"doc_id": doc_id},
                        {"$set": {**payload, "error": str(last_exc), "failed_at": time.time()}},
                        upsert=True,
                    )
                except PyMongoError:
                    log.error(
                        "Also failed to write dead-letter record for %s — this chunk is being lost.",
                        file_ident, exc_info=True,
                    )
                consumer.commit(msg, asynchronous=False)

    except KeyboardInterrupt:
        log.info("Gracefully shutting down RAG worker daemon…")
    finally:
        consumer.close()
        mongo_client.close()


if __name__ == "__main__":
    run_consumer()