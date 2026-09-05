# hialt-recall — unattended handoff: finish + validate the Postgres migration

You are running locally with full permissions and no human watching in
real time. Work through this whole file end to end without stopping to
ask questions. A short list below identifies the few things that
genuinely cannot be done without a human (mostly: proving you're a
person to a third-party website) — for those, and for anything with a
real chance of breaking a shared/production service, the rule is **log
it in the final report and move on to the next task**, never block
waiting for an answer that isn't coming.

Work from `~/hialt-recall`. Start with `git status` and `git diff --stat`
so you know exactly what's already sitting there uncommitted before you
change anything else.

## The handful of things you cannot fully automate — read this first

1. **Revoking the leaked GitHub PAT** (Task A below) requires a human on
   github.com — there is no API path to delete/regenerate a personal
   access token without either that token itself or an interactive
   browser/device-code login. You CAN and SHOULD remove it from git
   config (fully automatable, do it). You CANNOT revoke the token value
   itself. Log this clearly as a required human follow-up in the final
   report — do not attempt a `gh auth login` device flow and sit waiting
   for someone to type a code that may never come.
2. **Swapping the shared Postgres deployment's container image** (if
   pgvector turns out to be missing — Task B, step 2) affects every
   other service that pod serves, not just this one. Don't do this
   unilaterally. Log it and move on; the rest of the migration's file
   changes are still valid and worth committing even if live validation
   can't complete yet.
3. **Any destructive action** (PVC deletes, `git push --force`, dropping
   a database another service might depend on) — never do these
   unattended. If a task seems to require one, skip that specific task,
   log why, and continue with everything else.
4. **A missing credential you can't reconstruct** (e.g. `GHCR_TOKEN` not
   set — Task C) — don't invent one or prompt-and-wait. Log it, skip the
   step that needs it, continue.

For everything else — passwords, table names, non-destructive schema
choices — just make a reasonable choice yourself (e.g. generate a
password with `openssl rand -base64 24`) and proceed. Don't stop to ask
about anything reversible.

---

## Git context

Two remotes: `oci` (self-hosted Gitea, `gitea-oci:hialtdev/hialt-recall.git`)
is the working remote for in-progress work; `origin` (GitHub) only ever
receives finished, validated work. **Do not push to `origin` until the
Postgres migration is confirmed working end-to-end** (Task B is fully
validated). Current branch should be `agent-integration`, tracking
`oci/agent-integration`.

There are likely two batches of uncommitted changes already sitting
together: an earlier round of bug fixes (rag_engine.py UnboundLocalError
fix, rag_consumer.py Kafka retry/dead-letter design, k8s/deployment.yaml
health-probe path fix, ingest.py secret-filename + gitignore ingestion
safety nets) AND a new Postgres migration layered on top of those same
files (MongoDB → Postgres + pgvector). Read the diffs so you understand
what's there before committing.

---

## Task A: remove the exposed GitHub PAT from git config

`hialt-recall`'s `origin` remote has a GitHub personal access token
embedded directly in the URL, sitting in plaintext in `.git/config`
(flagged independently in two separate sessions now — this is
confirmed, not a maybe).

Automatable now: switch `origin` to the same SSH pattern every other
hialtdev repo on this machine already uses (check `~/.ssh/config` for
the `github-hialtdev` host alias — KubeConfigs and bitbybit-service both
push through it already, so the key is already set up and working):

```
git remote set-url origin github-hialtdev:hialtdev/hialt-recall.git
git remote -v   # confirm no token is visible anywhere now
git fetch origin  # confirm the new URL actually works before relying on it
```

NOT automatable: the token value itself is still valid on GitHub's
servers until a human revokes/regenerates it there (Settings → Developer
settings → Personal access tokens). Removing it from git config stops
*this machine* from using it in that URL, but doesn't invalidate it —
note this distinction explicitly in the final report.

---

## Task B: validate the Postgres + pgvector migration (primary work)

Changed files already on disk: `src/rag_engine.py`, `src/rag_consumer.py`,
`src/ingest.py`, `src/query.py`, `src/app.py`, `requirements.txt`,
`.env.example`, `k8s/configmap.yaml`, `k8s/deployment.yaml`. Read
`rag_engine.py` first — `Settings`, `get_connection()`, and
`ensure_schema()` are the best map of the whole design.

Schema (created automatically by `ensure_schema()`, idempotent —
`CREATE ... IF NOT EXISTS`):
- `chunks`: doc_id (PK), project_dir, project_name, tags TEXT[],
  source_file, chunk_index, headers, text, embedding vector(1024),
  updated_at. Indexes on project_dir, project_name, GIN(tags), and an
  HNSW index on embedding (cosine ops).
- `chunks_failed`: dead-letter table, same shape minus embedding, plus
  error/failed_at.
- `project_dir` (raw folder name, e.g. "kubeconfigs") and `project_name`
  (manifest display name, e.g. "KubeConfigs") are deliberately separate
  columns — they can differ, and query-time filtering checks both. Don't
  collapse them into one.

Steps:

1. `kubectl get pods -A | grep -i postgres` and
   `kubectl get svc -A | grep -i postgres` to find the shared Postgres
   pod/service and its namespace.

2. Check pgvector is available:
   ```
   kubectl exec -it <pod> -n <ns> -- psql -U postgres -c \
     "SELECT * FROM pg_available_extensions WHERE name = 'vector';"
   ```
   Empty result = the image doesn't include it. **This is the
   "swapping the shared image" exception from the top of this file** —
   log it clearly (name the pod/deployment, note that
   `pgvector/pgvector` images exist as a drop-in base if the human wants
   to go that route) and skip to Task D (docs) rather than getting stuck
   here — there's still value in the rest of the work even if live
   validation has to wait.

3. If pgvector IS available, create hialt-recall's own database + role
   (isolation from whatever else lands on that shared instance — do not
   reuse a shared superuser or another service's credentials):
   ```sql
   CREATE ROLE hialt_recall WITH LOGIN PASSWORD '<openssl rand -base64 24>';
   CREATE DATABASE hialt_recall OWNER hialt_recall;
   ```

4. Update the `hialt-recall-secrets` k8s Secret — the manifests now read
   a `postgres-uri` key (was `mongodb-uri`). Check the existing secret
   first so you don't clobber `groq-api-key`:
   ```
   kubectl get secret hialt-recall-secrets -n hialt-recall -o yaml
   kubectl create secret generic hialt-recall-secrets -n hialt-recall \
     --from-literal=postgres-uri="postgresql://hialt_recall:<password>@<postgres-service>.<namespace>.svc.cluster.local:5432/hialt_recall" \
     --from-literal=groq-api-key="<preserve the existing value>" \
     --dry-run=client -o yaml | kubectl apply -f -
   ```

5. Update local `.env` (port-forward to the postgres service for local
   testing) with `POSTGRES_URI`, `EMBEDDING_DIM=1024`,
   `POSTGRES_TABLE=chunks`, `POSTGRES_FAILED_TABLE=chunks_failed` — see
   `.env.example` for the exact shape.

6. `pip install -r requirements.txt` in the project's venv (adds
   `psycopg2-binary`/`pgvector`; `pymongo`/`numpy` are no longer needed
   but harmless if still installed).

7. Ingest and validate locally:
   ```
   python3 src/ingest.py --project kubeconfigs
   python3 src/rag_consumer.py   # run long enough to drain the topic, then Ctrl+C
   python3 src/query.py "how do I fix the Seq admin password if the pod gets recreated?"
   ```
   Confirm `ensure_schema()` ran without error (check the consumer's
   startup log line) and that the query returns a real, cited answer —
   not just "no error thrown."

8. If local validation succeeds and you want this live in the cluster:
   check whether `GHCR_TOKEN` is set in the environment
   (`echo $GHCR_TOKEN`). If it's set, `./deploy.sh` (build, push to GHCR,
   restart both deployments, wait for rollout, print pod status) handles
   the rest. If it's NOT set, this is the "missing credential" exception
   — log it and skip the live deploy; the code changes are still valid
   to commit.

9. Once local validation (step 7) succeeds: `git add` the relevant files,
   commit, `git push oci agent-integration`. Do NOT push to `origin` yet.

---

## Task C: (folded into Task B, step 8 — deploy.sh handles build/push/restart)

---

## Task D: update docs to match

`README.md` and `ONBOARDING_STATUS.md` (repo root) both still document
the old Mongo setup (`MONGO_URI` etc.) — update the datastore/ingestion
description in both to match Postgres. `ONBOARDING_STATUS.md` is also
read by two other AI agents (GPT and Codex) working on separate repos
as part of a larger multi-agent effort — only touch the
ingestion/datastore parts of that file, leave their per-repo assignments
and audit-findings sections alone.

Do NOT touch `bitbybit-service` or `bitbybit-react-ts-portfolio` at all
— those are assigned to other agents.

---

## Final step: write a report

Create `RUN_REPORT.md` at the repo root summarizing: what you completed,
what you skipped and why (with enough detail that a human can pick up
exactly where you left off), and an explicit short list of anything that
still needs a human — this should at minimum include "revoke the old
GitHub PAT on github.com" from Task A, plus anything else you had to
skip per the exceptions list at the top of this file.
