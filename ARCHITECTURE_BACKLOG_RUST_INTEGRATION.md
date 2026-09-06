# Architecture Backlog: Rust Integration Strategy for hialt-recall

**Document Identifier:** ARCH-BACKLOG-004  
**Date:** August 21, 2026  
**Status:** Proposed / Draft  
**Target Architecture:** `hialt-recall` RAG Pipeline & Ingestion Subsystems  

---

## Executive Summary

`hialt-recall` currently relies on a Python-based core for codebase scanning, document parsing, chunking, and embedding pipeline orchestration. While this architecture provides high flexibility, scaling the knowledge ingestion across large code repositories, extensive documentation trees, and high-frequency event logs highlights specific limitations in Python's single-threaded I/O, memory overhead, and chunking latency.

This architecture backlog outlines a phased roadmap to introduce high-performance, compiled **Rust components** into `hialt-recall`. The goal is to offload resource-intensive workloads (file walking, AST parsing, high-throughput chunking, and async local event orchestration) to native binaries or PyO3 extension modules without sacrificing the rapid UI prototyping capabilities of Streamlit.

---

## Technical Rationale & Objectives

* **Zero-Cost Abstractions & SIMD Acceleration:** Accelerate vector operations, local hashing, and text tokenization.
* **Low Idle Memory Footprint:** Reduce background memory consumption for continuous file watchers and background knowledge daemons from hundreds of MBs (Python runtime) to single-digit MBs (`tokio` async tasks).
* **Compile-Time Type Safety:** Enforce strict memory layouts, vector schemas, and event payload definitions at compile time.
* **Concurrency Efficiency:** Utilize Rust's `rayon` for data parallelism during full repository re-indexes and `tokio` for non-blocking stream processing.

---

## Backlog Items & Engineering Epics

### Epic 1: High-Speed Native Codebase Ingestion & Chunking Engine (`hialt-chunker`)

#### [ARCH-RUST-101] Fast Directory Walker & File Analyzer
* **Priority:** High
* **Component:** `hialt-chunker` (Rust Binary / PyO3 Module)
* **Description:** Implement a multi-threaded file system scanner using `ignore` and `walkdir` crates to traverse codebase structures while strictly honoring `.gitignore` rules, binary file exclusions, and configurable size thresholds.
* **Acceptance Criteria:**
  * Traverses 50,000+ files in under 200ms.
  * Outputs standardized file metadata JSON/Arrow streams.
  * Computes BLAKE3/SHA-256 hashes per file to enable delta-only indexing.

#### [ARCH-RUST-102] Tree-Sitter AST & Semantic Code Splitting
* **Priority:** High
* **Component:** `hialt-chunker`
* **Description:** Replace arbitrary character-count text splitters with semantic AST-aware parsing using `tree-sitter` Rust bindings. Support AST chunking for Python, Java, Rust, TypeScript, and Go.
* **Acceptance Criteria:**
  * Chunks code cleanly along logical function, class, and method boundaries.
  * Emits rich chunk metadata (enclosing parent context, line ranges, symbol signatures).
  * Exposes Python bindings via PyO3 (`maturin` build toolchain) for drop-in use in existing Python ingestion scripts.

---

### Epic 2: Embedded Vector Indexing & Local Storage Optimizations

#### [ARCH-RUST-201] Embedded Vector DB Integration (LanceDB / Qdrant Segment)
* **Priority:** Medium
* **Component:** Storage Layer Integration
* **Description:** Evaluate transitioning or supplementing local vector storage with embedded Rust-native vector engines (e.g., LanceDB over Apache Arrow or embedded Qdrant core).
* **Acceptance Criteria:**
  * Sub-millisecond local vector ANN lookup times for top-$k$ queries.
  * Zero external database process dependency for standalone desktop operation.
  * Direct inter-process memory sharing or memory-mapped file access.

#### [ARCH-RUST-202] Native Tokenizer & Local Ollama Embedder Client
* **Priority:** Medium
* **Component:** Embedding Subsystem
* **Description:** Implement a Rust native client using `tokenizers` and `reqwest` to interface with local Ollama embedding endpoints directly, bypassing Python HTTP serialization overhead.
* **Acceptance Criteria:**
  * Concurrent batching of chunk text streams to local Ollama API (`nomic-embed-text`, `bge-large-en`).
  * Backpressure handling and automatic retries on hardware/GPU saturation.

---

### Epic 3: Autonomous Background Knowledge Engine & Event Daemon

#### [ARCH-RUST-301] Asynchronous Directory & Event Watcher (`hialt-watchd`)
* **Priority:** Medium
* **Component:** Native Background Daemon
* **Description:** Build a lightweight daemon using `tokio` and `notify` to monitor registered codebase directories for file mutational events (`Create`, `Modify`, `Delete`).
* **Acceptance Criteria:**
  * Memory footprint strictly under 15 MB RAM while running in background.
  * Triggers delta re-chunking and vector payload re-indexing only on modified files via SHA-256 hash validation.

#### [ARCH-RUST-302] Native LLM Agent Orchestration with `rig-core`
* **Priority:** Low (Phase 3 Exploration)
* **Component:** RAG Service Layer
* **Description:** Implement a lightweight Rust service using `rig-core` to handle background RAG synthesis, document summarization, and query execution pipelines without firing up a full Python interpreter.
* **Acceptance Criteria:**
  * Instant startup times (< 50ms) for CLI queries.
  * Exposes lightweight local gRPC/UNIX domain socket endpoints for Streamlit UI consuming.

---

## Implementation Roadmap & Architectural Phasing

```
+-------------------------------------------------------------------+
| Phase 1: Hybrid Integration (Immediate Impact)                     |
|  - Build `hialt-chunker` with PyO3/maturin                        |
|  - Replace Python file scanner & character splitter in hialt-recall|
+-------------------------------------------------------------------+
                                 |
                                 v
+-------------------------------------------------------------------+
| Phase 2: Embedded Storage & Fast Sync                             |
|  - Integrate LanceDB/Arrow memory structures                       |
|  - Deploy `hialt-watchd` background file system monitor in Rust   |
+-------------------------------------------------------------------+
                                 |
                                 v
+-------------------------------------------------------------------+
| Phase 3: Native Micro-Agent Engine                                |
|  - Port background RAG pipeline to `rig-core` + `tokio`          |
|  - Python Streamlit UI retains visualization, delegates to Rust   |
+-------------------------------------------------------------------+
```

---

## Tooling & Dependency Stack

* **Language Standard:** Rust Edition 2021 (MSRV 1.78+)
* **Build / Python Binding:** `maturin`, `pyo3`
* **Async Runtime:** `tokio`
* **Parallel Processing:** `rayon`
* **Parsing / AST:** `tree-sitter`, `ignore`, `walkdir`
* **Agent Framework:** `rig-core`
* **Vector Engine Candidates:** `lancedb`, `qdrant-client`

---

*End of Architecture Backlog*
