"""
query.py — CLI for hialt-recall.

All RAG logic lives in rag_engine.py.
This file is a thin argument-parsing wrapper, preserving
the exact same CLI interface as before:

    python query.py "What MQTT topic does foxwatch subscribe to?"
    python query.py "Cross-project error handling patterns" --top-k 10
    python query.py "Does foxwatch support gRPC?" --no-groq
"""

import argparse
import sys
import textwrap

from src.rag_engine import load_settings, run_query


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Query the local MongoDB RAG store.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
            Examples:
              python query.py "What MQTT topic does foxwatch subscribe to?"
              python query.py "Common error-handling patterns" --top-k 10
              python query.py "Does foxwatch support gRPC?" --no-groq
        """),
    )
    parser.add_argument("query", type=str, help="Natural-language question.")
    parser.add_argument("--top-k", type=int, default=3,
                        help="Number of chunks to retrieve (default: 3).")
    parser.add_argument("--no-groq", action="store_true",
                        help="Disable Groq, force local Ollama only.")
    parser.add_argument("--project", default=None,
                        help="Restrict retrieval to this project name.")
    parser.add_argument("--tag", default=None,
                        help="Restrict retrieval to chunks with this tag.")
    args = parser.parse_args()

    try:
        settings = load_settings()
    except RuntimeError as e:
        print(f"❌ Config error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\n🔍 Vectorizing query...")
    print(f"📡 Fetching docs...")
    print(f"🧮 Calculating similarity...")

    if args.no_groq:
        print("🦙 Generating answer via local Ollama...")
    else:
        print("🚀 Generating answer via Groq (Turbo)...")

    result = run_query(
        question=args.query,
        top_k=args.top_k,
        force_ollama=args.no_groq,
        settings=settings,
        project_filter=args.project,
        tag_filter=args.tag,
    )

    # Surface fallback / error notes
    if result.error:
        # Fallback is a warning, not a fatal error
        if result.answer:
            print(f"\n⚠️  {result.error}", file=sys.stderr)
        else:
            print(f"\n❌ {result.error}", file=sys.stderr)
            sys.exit(1)

    print(f"\n--- ANSWER ---\n{result.answer}")

    if result.chunks:
        print(f"\n--- SOURCES ({result.llm_used}) ---")
        for i, c in enumerate(result.chunks, 1):
            print(f"  [{i}] {c['source_file']}  ({c['headers']})  score={c['score']:.3f}")


if __name__ == "__main__":
    main()