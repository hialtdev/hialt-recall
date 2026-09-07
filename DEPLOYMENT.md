# Stable/dev deployment and access

Operational snapshot: 2026-09-07. PIN login is still awaiting a clean-browser
check; HTTP redirects alone do not establish successful login.

## Release ownership

Stable serves <https://recall.hialt.dev> from namespace `hialt-recall`.
A push to GitHub `main` builds and tests the application, publishes `latest`
and SHA tags to `ghcr.io/hialtdev/hialt-recall`, deploys the build output's
immutable digest to the UI and consumer, and waits for both rollouts.

The immutable-digest and security changes are on `fix/stable-image-digest`,
targeting GitHub `agent-integration` for review. Until they reach GitHub main,
the live workflow still sets the mutable `:latest` reference. Setting the same
tag does not necessarily change the pod template, so that old workflow may not
replace pods when the tag moves.

Dev serves <https://recall-dev.hialt.dev> from namespace `hialt-recall-dev`.
Gitea PR 4, branch `ci/gitea-actions`, contains the direct development release
path. Pushes and pull requests build and test. Pushes also build and test the
AMD64 release image and publish an immutable SHA tag. A Gitea main push updates
`:main` for retention without deploying. Any other branch push updates `:dev`,
deploys the published digest directly to dev, and waits for both rollouts.

The Gitea runner publishes through `localhost:3000` on its OCI Docker host to
avoid Cloudflare's upload limit. Cluster pulls continue to use
`https://gitea.hialt.dev`. The first direct deployment completed successfully
on 2026-09-07. The old dev polling CronJob and its obsolete ServiceAccount,
RBAC, script ConfigMap, completed Job, and registry-reader Secret were removed.

The stable Gitea release CronJob remains suspended. Keep it suspended while
GitHub owns stable releases; otherwise it could overwrite the stable image.

## Shared release contract

The GitHub and Gitea workflows use the same release shape:

1. Build and test the source that triggered the workflow.
2. Publish the tested image to the environment's registry.
3. Capture and validate the registry's `sha256` digest.
4. Update only the Recall UI and consumer to `registry/image@sha256:...`.
5. Wait until both deployments have observed the change and are available.

Both workflows use `scripts/deploy-kubernetes-image.sh`. It reads each named
Deployment directly while waiting, avoiding namespace-wide list/watch access.
External Actions are pinned to full commit SHAs so moved upstream tags cannot
silently replace executable workflow code.

## CI credentials and RBAC

Each codebase and environment has a separate deployment identity.

Gitea stores `KUBECONFIG_DEV_CI`, which authenticates as
`system:serviceaccount:hialt-recall-dev:hialt-recall-ci`. Its Role permits only
`get` and `patch` on `hialt-recall-ui` and `hialt-recall-consumer` in
`hialt-recall-dev`.

GitHub stores `KUBECONFIG_STABLE_CI`, which authenticates as
`system:serviceaccount:hialt-recall:hialt-recall-ci`. Its Role permits only
`get` and `patch` on those two named Deployments in `hialt-recall`.

The previous GitHub identity lived in `kube-system` and had a `cluster-admin`
ClusterRoleBinding. That binding and ServiceAccount were deleted on 2026-09-07.
`KUBECONFIG_CI` temporarily contains the same restricted stable credential so
the current GitHub main workflow continues to work until it adopts the new
secret name.

During review, the live stable Role also has namespace-wide read-only `list`
and `watch` on Deployments because the old workflow invokes
`kubectl rollout status`. Once the new exact-resource script reaches main,
apply `k8s/stable-ci-rbac.yaml` to remove that temporary compatibility access.

Both CI tokens were issued through Kubernetes TokenRequest, are bound to an
empty namespace-local Secret that acts as a revocation anchor, and expire on
2026-12-06. Delete the corresponding `hialt-recall-ci-token-binding` Secret to
revoke a token early. Rotate the token, update the repository secret, validate
a release, and then delete the old binding Secret before the expiration date.

These permissions are deliberately narrow, but deployment authority is still
sensitive. A compromised workflow can deploy malicious application code into
its two pods and access the application credentials available there. It cannot
use the Kubernetes credential to read Secrets, change other workloads, or
cross into the other environment.

Registry credentials are separate from Kubernetes credentials:

- GitHub uses its job-scoped `GITHUB_TOKEN` to publish to GHCR.
- Gitea uses repository secret `REGISTRY_PUSH_TOKEN` to publish packages.
- Dev pods use namespace Secret `gitea-registry` to pull images.
- Application credentials remain in namespace-local `hialt-recall-secrets`.

Never commit kubeconfigs, tokens, PINs, registry credentials, or Secret values.

## Remote and branch semantics

Local remote aliases are:

- `origin`: GitHub; `git push origin main` triggers stable.
- `oci`: Gitea; a non-main branch push triggers dev when that branch contains
  `.gitea/workflows/ci.yml`.

`git push oci main` publishes Gitea `:main` but does not deploy. Because every
non-main Gitea push shares the `:dev` release tag, the most recent successful
branch push owns dev.

Relevant sources:

- [GitHub stable workflow](https://github.com/hialtdev/hialt-recall/blob/main/.github/workflows/deploy.yml)
- [Gitea PR 4](https://gitea.hialt.dev/hialtdev/hialt-recall/pulls/4)
- `k8s/stable-ci-rbac.yaml` and Gitea branch `k8s/dev-ci-rbac.yaml`
- `scripts/deploy-kubernetes-image.sh`

## Traffic and data boundaries

Both public UI hostnames are protected by Cloudflare Access. The operator
configured one-time PIN under Zero Trust → Integrations → Identity providers,
with Access policies allowing `hialtdev2@tutamail.com`. Unauthenticated
requests currently redirect to the matching login path under
`https://hialt-lab.cloudflareaccess.com/cdn-cgi/access/login/`.

Cloudflare tunnel hostname routing is remotely managed in Zero Trust.
Kubernetes Ingress changes do not configure those routes or Access policies.
The Streamlit application has no independent user authentication check, so
internal Service access and port-forwarding rely on cluster and network access
controls rather than Cloudflare Access.

`https://k8s.hialt.dev` is externally reachable and returns `401 Unauthorized`
without Kubernetes credentials. The CI kubeconfigs use that endpoint with
normal public TLS certificate verification. Kubernetes RBAC, separate from the
UI email/PIN policy, controls authenticated API access.

Namespaces do not guarantee data isolation. The inspected stable and dev
ConfigMaps use the same Kafka topic and consumer group, so the consumers divide
ingestion work rather than each receiving every message. Dev also uses stable's
Ollama Service. Both refer to namespace-local Postgres Secrets; their database
targets have not been compared.

When Groq is enabled, the query engine sends the question and retrieved context
to Groq. The UI's Ollama-only option avoids that request.

## Verify Access login

1. Open a fresh incognito/private session and visit the stable UI. Confirm that
   Access appears before Recall content.
2. Request a PIN for the allowed address and complete login. Confirm the browser
   returns to the stable hostname and Recall loads.
3. Close every private window, open a new private session, and repeat for dev.
4. Record only pass/fail for PIN delivery, authentication, and UI loading. Do
   not record the PIN or session cookie.

Cloudflare intentionally displays a sent-code message even for an address that
policy blocks. That message alone does not prove delivery. See
[Cloudflare's OTP documentation](https://developers.cloudflare.com/cloudflare-one/integrations/identity-providers/one-time-pin/).

## Operational checks

```bash
kubectl -n hialt-recall get deployments,pods,ingress,cronjobs
kubectl -n hialt-recall-dev get deployments,pods,ingress
kubectl -n hialt-recall get pods -o custom-columns='NAME:.metadata.name,IMAGE:.spec.containers[*].image,IMAGE_ID:.status.containerStatuses[*].imageID'
kubectl -n hialt-recall-dev get pods -o custom-columns='NAME:.metadata.name,IMAGE:.spec.containers[*].image,IMAGE_ID:.status.containerStatuses[*].imageID'
curl -sS -o /dev/null -w '%{http_code}\n' https://recall.hialt.dev
curl -sS -o /dev/null -w '%{http_code}\n' https://recall-dev.hialt.dev
curl -sS -o /dev/null -w '%{http_code}\n' https://k8s.hialt.dev
```

Expected public results without credentials are `302`, `302`, and `401`.
Streamlit's internal health endpoint is `/_stcore/health` on port 8501.

### Verification record: 2026-09-07

- Gitea push and PR workflows completed successfully for commit `0115e63`.
- Direct Gitea deployment moved both dev workloads to digest
  `sha256:ac54bcc3a69641b5ab237b70d1dae143e5cb8721500856f8544f15fb55dc44b5`.
- Dev UI and consumer became ready at `1/1` with zero restarts.
- The restricted dev identity could patch only the two named dev Deployments;
  attempts to list Deployments, read Secrets, patch Ollama, or patch stable were
  denied.
- The old GitHub cluster-admin binding and ServiceAccount were removed. The new
  stable identity cannot read Secrets or access dev.
- Dev returned the expected Cloudflare Access `302` after deployment.
- **Pending:** PIN delivery and authenticated UI loading in a clean browser.
