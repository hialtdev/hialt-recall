#!/usr/bin/env bash
# deploy.sh — Build and push hialt-recall to GHCR, then restart k8s deployments.
# Usage:
#   ./deploy.sh              # build + push + restart
#   ./deploy.sh --build-only # build + push, skip kubectl
#   ./deploy.sh --restart-only # skip build, just restart pods

set -euo pipefail

IMAGE="ghcr.io/hialtdev/hialt-recall"
NAMESPACE="hialt-recall"

BUILD=true
RESTART=true

for arg in "$@"; do
  case $arg in
    --build-only)   RESTART=false ;;
    --restart-only) BUILD=false ;;
  esac
done

# ── Build & Push ─────────────────────────────────────────────────────────────
if [ "$BUILD" = true ]; then
  echo "▶ Logging in to GHCR..."
  echo "$GHCR_TOKEN" | docker login ghcr.io -u hialtdev --password-stdin

  echo "▶ Building image..."
  docker build -t "${IMAGE}:latest" .

  echo "▶ Pushing to GHCR..."
  docker push "${IMAGE}:latest"

  echo "✓ Image pushed: ${IMAGE}:latest"
fi

# ── Restart Pods ─────────────────────────────────────────────────────────────
if [ "$RESTART" = true ]; then
  echo "▶ Restarting deployments in namespace '${NAMESPACE}'..."

  kubectl rollout restart deployment/hialt-recall-ui       -n "${NAMESPACE}"
  kubectl rollout restart deployment/hialt-recall-consumer -n "${NAMESPACE}"

  echo "▶ Waiting for rollout..."
  kubectl rollout status deployment/hialt-recall-ui       -n "${NAMESPACE}" --timeout=120s
  kubectl rollout status deployment/hialt-recall-consumer -n "${NAMESPACE}" --timeout=120s

  echo "✓ Deployments restarted successfully."
  kubectl get pods -n "${NAMESPACE}"
fi