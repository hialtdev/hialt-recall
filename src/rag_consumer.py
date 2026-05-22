# src/workers/rag_consumer.py
import json
import os
import sys
import requests
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from dotenv import load_dotenv
from rag_engine import load_settings

load_dotenv()

def run_consumer():
    settings = load_settings()
    
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    consumer_group = os.environ.get('KAFKA_CONSUMER_GROUP', 'hialt-recall-workers')
    kafka_topic = os.environ.get('KAFKA_TOPIC', 'rag-raw-chunks')

    # Configure Kafka Consumer with Manual Offsets for Resiliency
    consumer = Consumer({
        'bootstrap.servers':  bootstrap_servers,
        'group.id':           consumer_group,
        'auto.offset.reset':  'earliest',
        'enable.auto.commit': False  # Only commit when safely in MongoDB
    })
    consumer.subscribe([kafka_topic])

    # Dynamic MongoDB initialization via shared settings
    mongo_client = MongoClient(settings.mongo_uri)
    db = mongo_client[settings.mongo_default_db]
    coll = db[settings.mongo_collection]

    # Dynamic Ollama configuration
    ollama_base_url = os.environ.get('OLLAMA_BASE_URL', 'http://localhost:11434').rstrip('/')

    print("=== Hialt-Recall Kafka Consumer Online ===")
    print(f"Targeting Kafka Broker: {bootstrap_servers}")
    print(f"Awaiting code chunks from topic: {kafka_topic}...\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka Network Error: {msg.error()}", file=sys.stderr)
                    break

            payload = json.loads(msg.value().decode('utf-8'))
            
            # Realigned Schema Mapping to match ingest.py output structure
            doc_id = payload.get('doc_id')
            project = payload.get('project')
            source_file = payload.get('source_file')
            chunk_index = payload.get('chunk_index', 0)
            content = payload.get('text')

            file_ident = f"{project} -> {source_file} [Chunk {chunk_index}]"
            print(f"Processing: {file_ident}")

            if not content:
                print(f"Skipping empty payload content for doc_id: {doc_id}", file=sys.stderr)
                consumer.commit(msg, asynchronous=False)
                continue

            try:
                # 1. Fetch embedding vector from dynamic Ollama service route
                ollama_response = requests.post(
                    f"{ollama_base_url}/api/embeddings",
                    json={"model": "mxbai-embed-large", "prompt": content}
                )
                ollama_response.raise_for_status()
                
                # 2. Append vector array to payload
                payload['embedding'] = ollama_response.json()['embedding']
                
                # 3. Upsert straight into MongoDB using our unique schema elements
                coll.update_one(
                    {"project": project, "source_file": source_file, "chunk_index": chunk_index},
                    {"$set": payload},
                    upsert=True
                )
                
                # 4. Commit offset to Kafka ONLY after verified DB write
                consumer.commit(msg, asynchronous=False)
                print(f"Successfully ingested chunk {doc_id} to MongoDB.")
                
            except Exception as inner_err:
                print(f"CRITICAL: Failed to process chunk {file_ident}. Error: {inner_err}", file=sys.stderr)
                # Offset is NOT committed; message stays safe in Kafka queue for retry

    except KeyboardInterrupt:
        print("\nGracefully shutting down RAG worker daemon...")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()