# Stack Onboarding Status

This file is the shared, git-tracked coordination doc for the effort to bring
every hialtdev project into hialt-recall as a "wiki replacement" for the
whole stack. It's meant to be readable by any agent working on this — Claude,
GPT, Codex, or a human — without needing access to any one tool's private
session memory. Whoever picks up work in a repo below should update their
section before handing off.

## The strategy, in short

hialt-recall is a local RAG pipeline (Kafka → Ollama embeddings → MongoDB →
Groq/Ollama generation). The goal is to ingest every hialtdev project's
docs/config/runbooks so cross-project questions get real, cited answers
instead of living only in someone's head. Pattern per project: branch first
→ write/fix a `hialt-knowledge.yaml` manifest → symlink into
`hialt-recall/data/repos/<name>` → `python3 src/ingest.py --project <name>`
→ verify with a real query.

## Repo workflow — GitHub is truth, Gitea is where work happens

**Every repo in this effort, not just hialt-recall:** GitHub is the
permanent source of truth — it only ever receives finished, reviewed work.
All in-progress work (any new branch, any commit before it's ready to merge)
happens against the project's **Gitea** remote instead.

Convention: remote name `oci`, SSH alias `gitea-oci` (already configured on
the dev machine), path `gitea-oci:hialtdev/<repo>.git`. If a repo doesn't
have an `oci` remote yet:

```
# create an empty repo in the Gitea UI first (owner hialtdev, name matching
# GitHub — push-to-create is OFF on this instance, so this step is required)
git remote add oci gitea-oci:hialtdev/<repo>.git
git push oci --all
git push oci --tags
```

Branch-before-changes still applies, just on Gitea now: create the branch,
push it to `oci`, set upstream to `oci/<branch>`, then start editing. When a
repo's work is done and validated, push that branch to `origin` (GitHub) and
merge to `main` there.

## Ingestion safety nets — read before writing a manifest

`hialt-recall/src/ingest.py` has two **manifest-proof** hard gates that no
`hialt-knowledge.yaml` can override — they exist because a real secret
(a plaintext Seq admin password in KubeConfigs) got ingested and echoed back
by the LLM mid-session:

1. `_looks_like_secret()` — filename heuristic (`*secret*`/`*credentials*`
   as whole tokens, `.env*`, key/cert extensions, `id_rsa` etc.).
2. `_load_gitignored_paths()` — anything the project's own `.gitignore`
   would exclude is never ingested (via `git status --ignored`, not
   hand-parsed).

Neither is a content scanner. A real secret hardcoded inline in an
otherwise-innocuous file (e.g. typed directly into a deployment manifest
instead of pulled via `secretKeyRef`) won't be caught by either — manually
scan for that before trusting a new project's ingest, and add explicit
`exclude_paths` in the manifest for anything sensitive you find, even if a
net would also catch it (belt and suspenders, and it documents *why*).

## Repo status

### hialt-recall (the hub itself) — owner: Claude
Branch `agent-integration` on `oci`. CLI import bug, Kafka data-loss retry
bug, wrong Streamlit health-check path, GROQ_MODEL config drift, and both
ingestion safety nets above are fixed in working tree; confirm what's
actually committed vs. still local before starting new work here.

### KubeConfigs — owner: Claude
Pilot project, validated end-to-end (ingest + real cited query). Branch
`recall-onboarding`, manifest written with `exclude_paths` for the real
secret file. Being migrated onto the Gitea workflow above now.

### bitbybit-service — owner: **open — suggested: Codex**
Branch `recall-onboarding` exists on GitHub `origin` (base `dev`), not yet
on Gitea. No file changes made yet. Known findings, ready to act on:
- `README.md` is broken — an earlier AI session pasted a Python
  string-wrapper (`readme_content = """# BitByBit ...`) directly into the
  `.md` file; the code fence is never closed. Underlying content is good,
  just needs re-saving as plain markdown.
- `docs/bitbybit-cluster-runbook.md` and `docs/architecture/codebase-assessment.md`
  are already wiki-quality — ready to ingest as-is once a manifest exists.
- No `hialt-knowledge.yaml` yet. Needs one; also needs `.txt`/`.json`
  extensions allow-listed for dev notes / Postman collection (same issue
  KubeConfigs had with `bash commands.txt`).
- Stray file `${ENV:APP_LOG_PATH_ENV}` (literal filename) at repo root —
  looks like an unsubstituted Logback env var. Unrelated to recall ingestion,
  just flagged for whoever eventually touches logging config.

### bitbybit-react-ts-portfolio (ingested as `bitbybit-frontend`) — owner: **open — suggested: GPT**
Branch `recall-onboarding` exists on GitHub `origin` (base `dev`), not yet
on Gitea. No file changes made yet. Known findings, ready to act on:
- `README.md` is unmodified Vite boilerplate — no real project content.
- Real content lives in loose files instead: `BitByBitProjecDevNotes.txt`
  (a "React & Tomcat Fundamentals" / CORS-proxy writeup despite the name)
  and a file literally named `Tomcat Development Guide` with **no
  extension** — currently invisible to any extension-based ingestion; needs
  a rename (e.g. `.md`) before it can be picked up at all.
- No `hialt-knowledge.yaml` yet.

## Cluster capacity note

Single-node k3s on the same machine running hialt-recall's own services —
CPU requests were at ~92% of the node's 4 cores as of the last check. Fine
for ingestion (cheap, bounded batch job), but avoid adding new *standing*
services without checking headroom first.
