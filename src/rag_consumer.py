# rag_consumer.py
"""
Kafka consumer for hialt-recall.

Reads raw chunk payloads from the ingest topic, embeds them via Ollama,
and upserts the result (including the metadata block) into MongoDB.

Manual offset commit ensures at-least-once delivery: the Kafka offset is
only advanced after a verified MongoDB write, so a crash mid-flight leaves
the message available for retry on the next startup.
"""

import json
import logging
import os
import sys
import time

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

# How long to wait before retrying a failed embed/write (seconds)
RETRY_BACKOFF = 2


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

    _ensure_indexes(coll)

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

            # ── Embed + upsert (with simple retry) ────────────────────────
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
                log.info("✓ Upserted %s", doc_id)

            except requests.HTTPError as exc:
                log.error("Ollama embedding failed for %s: %s — will retry on restart.", file_ident, exc)
                time.sleep(RETRY_BACKOFF)
                # Do NOT commit — message stays in Kafka for retry

            except PyMongoError as exc:
                log.error("MongoDB write failed for %s: %s — will retry on restart.", file_ident, exc)
                time.sleep(RETRY_BACKOFF)
                # Do NOT commit

            except Exception as exc:  # noqa: BLE001
                log.error("Unexpected error processing %s: %s", file_ident, exc, exc_info=True)
                time.sleep(RETRY_BACKOFF)
                # Do NOT commit

    except KeyboardInterrupt:
        log.info("Gracefully shutting down RAG worker daemon…")
    finally:
        consumer.close()
        mongo_client.close()


if __name__ == "__main__":
    run_consumer()