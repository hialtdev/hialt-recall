# RUN_REPORT — Postgres migration handoff, 2026-09-05

Run against `HANDOFF_POSTGRES_MIGRATION.md` on branch `agent-integration`.

## Environment mismatch, read this first

The handoff assumes an unattended agent with full permissions. This run had a
permission classifier in front of the shell. Every **cluster-mutating** command
and the **git remote change** were denied, with no way to complete them from
here. Read-only `kubectl`, local file edits, and local Python all worked.

Nothing in the cluster was changed. The live deployment is still on MongoDB and
is still working; the migration exists only as uncommitted files on disk.

## What was completed

**Verified the target instance.** The shared Postgres is `postgres-main` in the
`database` namespace, a CloudNativePG cluster on
`ghcr.io/cloudnative-pg/postgresql:15.19-...-system-bookworm`, healthy, one
instance, created 2026-09-04. Read/write service is `postgres-main-rw`.

**pgvector is available: version 0.8.6.** The handoff's "if pgvector turns out
to be missing" branch does not apply, and no image swap is needed. This is the
one blocking question the handoff flagged as possibly fatal, and the answer is
good news.

**Found and fixed two defects that would have broken the migration at runtime.**
Both are confirmed from the installed library source, not guessed. Neither would
have shown up in a syntax check or an import test, and both would have surfaced
on the first real ingest or query.

1. *Embeddings were bound as plain Python lists.* `pgvector`'s psycopg2 adapter
   registers an adapter only for its own `Vector` type and for numpy arrays. A
   plain list is rendered by psycopg2 as a Postgres `ARRAY[...]` literal, which
   is not assignable to a `vector` column and which the `<=>` operator has no
   overload for. Every upsert and every query would have failed. Fixed in
   `src/rag_engine.py` and `src/rag_consumer.py` by wrapping both the query
   vector and the stored embedding in `Vector()`.

2. *`ensure_schema()` could never bootstrap a fresh database.* It opened its
   connection through `get_connection()`, which calls `register_vector()`, which
   raises `psycopg2.ProgrammingError: vector type not found in the database`
   when the extension is absent. So the function that runs `CREATE EXTENSION`
   could not connect until after the extension already existed. Fixed by giving
   the bootstrap path its own `_connect_raw()` that skips registration.

   The deeper point: pgvector is **not a trusted extension**, so the owning role
   is not permitted to install it regardless. It has to be provisioned out of
   band, which is what the manifest change below does.

**Prepared the platform-side provisioning** in `~/projects/hialt-platform`,
following the existing declarative pattern rather than the raw SQL the handoff
suggested, because `apps/postgres/` is the source of truth for this instance and
imperative `CREATE ROLE` would have drifted from it. Uncommitted, not applied:

- `apps/postgres/roles.yaml` — a `DatabaseRole` for `hialt_recall`,
  `connectionLimit: 10`, password from a `hialt-recall-db-role` Secret.
- `apps/postgres/databases.yaml` — a `Database` for `hialt_recall` owned by that
  role, declaring the `vector` extension so the operator creates it as
  superuser.
- `apps/postgres/README.md` — documents the third role and secret.

`kubectl apply --dry-run=server` accepts all three; only the real apply was
denied.

**Updated the docs** in this repo, per Task D. `README.md` now describes
Postgres and pgvector throughout: the intro, the pipeline description, the
prerequisites port-forward, the `.env` block, the schema section, the `--reset`
warning, the validation checklist (now `psql`, including a `chunks_failed`
query), and the scaling notes (now about HNSW recall and filtered-query
under-fill rather than numpy). `ONBOARDING_STATUS.md` had only its
datastore sentence changed; the other agents' per-repo sections were left
untouched.

**Local checks that did pass.** `requirements.txt` installs cleanly into
`.venv`; no `pymongo` or `numpy` references remain anywhere in `src/`; all five
modules compile and import; the existing suite is 4 passed.

## What was skipped, and why

**Task A, removing the leaked GitHub PAT — not done.** `git remote set-url` was
denied by the classifier. The token is still sitting in plaintext in
`.git/config` on the `origin` URL. The SSH host alias it should point at
(`github-hialtdev`) does exist in `~/.ssh/config` and is already used by other
repos, so the change is one command once permitted. This is the more urgent of
the two halves; the handoff already notes that revoking the token itself needs a
human on github.com regardless.

**Task B steps 3 through 9 — not done.** All of them need cluster writes or
depend on something that does:

- The `hialt-recall-db-role` Secret could not be created. A password was
  generated during the run and has been deleted rather than left lying in a
  temp file; generate a fresh one when you do this for real.
- The role and database therefore do not exist. `hialt_recall` is absent from
  `pg_roles`, and the databases on the instance are still `app`, `mattermost`,
  and `orbitfeed` only.
- The `hialt-recall-secrets` Secret in the `hialt-recall` namespace still has
  keys `groq-api-key` and `mongodb-uri`. The updated `k8s/deployment.yaml`
  reads `postgres-uri`, so **deploying the current manifests before adding that
  key would fail to start both pods.**
- No local `.env` change was made, deliberately. The current `.env` still has
  working `MONGO_URI` values, and rewriting it with placeholder Postgres values
  would have broken a working local setup in exchange for nothing.
- Local validation (ingest, consumer, query) could not run without a database.

**Nothing was committed.** The handoff gates the commit on step 7 local
validation succeeding, and it could not run. All changes are in the working tree
on `agent-integration`.

**GHCR deploy not attempted.** Moot while validation is incomplete.

## Needs a human

1. **Revoke the GitHub PAT** on github.com, Settings → Developer settings →
   Personal access tokens. It is live until you do. Then repoint the remote:
   ```
   git -C ~/hialt-recall remote set-url origin github-hialtdev:hialtdev/hialt-recall.git
   git -C ~/hialt-recall fetch origin
   ```

2. **Grant cluster write access to the agent, or run the provisioning yourself.**
   In order:
   ```
   PW=$(openssl rand -hex 24)   # hex, so it needs no URI escaping
   kubectl create secret generic hialt-recall-db-role -n database \
     --type=kubernetes.io/basic-auth \
     --from-literal=username=hialt_recall --from-literal=password="$PW"
   # store $PW in Bitwarden — per SECRETS.md, Bitwarden is the source of truth

   kubectl apply -f ~/projects/hialt-platform/apps/postgres/roles.yaml
   kubectl apply -f ~/projects/hialt-platform/apps/postgres/databases.yaml

   kubectl create secret generic hialt-recall-secrets -n hialt-recall \
     --from-literal=postgres-uri="postgresql://hialt_recall:$PW@postgres-main-rw.database.svc.cluster.local:5432/hialt_recall" \
     --from-literal=groq-api-key="<preserve the existing value>" \
     --dry-run=client -o yaml | kubectl apply -f -
   ```
   Then commit the `hialt-platform` change, which is currently uncommitted in
   that repo.

3. **Run the validation the handoff asks for**, with a port-forward to
   `svc/postgres-main-rw` in the `database` namespace and a local `.env`
   carrying `POSTGRES_URI`, `EMBEDDING_DIM=1024`, `POSTGRES_TABLE=chunks`,
   `POSTGRES_FAILED_TABLE=chunks_failed`. Confirm the consumer logs
   `Postgres schema verified` and that a real query returns a cited answer.
   Only then commit and push to `oci`.

4. **Decide what happens to the MongoDB data.** The migration re-embeds from
   source rather than copying vectors across, so the first ingest after cutover
   regenerates everything. The old `mongodb` deployment and its `local_rag`
   database in the `default` namespace are untouched and still running. Take a
   dump before retiring them, matching what `apps/postgres/README.md` records
   for the Mattermost and OrbitFeed cutovers.

## One thing worth a second look

The `chunks` table has no unique constraint tying a chunk to its source file
beyond `doc_id`, and `--reset` truncates `chunks` but not `chunks_failed`. That
is defensible, and the README now says so, but it means a dead-lettered row can
outlive the content it came from. Worth a prune step if `chunks_failed` ever
grows.
