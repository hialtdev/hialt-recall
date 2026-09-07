#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${1:-}"
IMAGE="${2:-}"
TIMEOUT_SECONDS="${ROLLOUT_TIMEOUT_SECONDS:-180}"

if [[ -z "$NAMESPACE" || ! "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "Usage: $0 <namespace> <image@sha256:digest>" >&2
  exit 2
fi
if [[ ! "$IMAGE" =~ ^(ghcr\.io|gitea\.hialt\.dev)/hialtdev/hialt-recall@sha256:[0-9a-f]{64}$ ]]; then
  echo "Refusing non-immutable or unexpected image: $IMAGE" >&2
  exit 2
fi

wait_for_deployment() {
  local name="$1"
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  local status generation observed desired updated available unavailable

  while ((SECONDS < deadline)); do
    status=$(kubectl -n "$NAMESPACE" get "deployment/$name" \
      -o jsonpath='{.metadata.generation}|{.status.observedGeneration}|{.spec.replicas}|{.status.updatedReplicas}|{.status.availableReplicas}|{.status.unavailableReplicas}')
    IFS='|' read -r generation observed desired updated available unavailable <<< "$status"
    desired="${desired:-1}"
    updated="${updated:-0}"
    available="${available:-0}"
    unavailable="${unavailable:-0}"
    if [[ "$observed" == "$generation" && "$updated" == "$desired" && "$available" == "$desired" && "$unavailable" == "0" ]]; then
      echo "deployment/$name successfully rolled out"
      return 0
    fi
    sleep 3
  done

  echo "Timed out waiting for deployment/$name in $NAMESPACE" >&2
  kubectl -n "$NAMESPACE" get "deployment/$name" -o wide >&2 || true
  return 1
}

kubectl -n "$NAMESPACE" set image deployment/hialt-recall-ui "ui=$IMAGE"
kubectl -n "$NAMESPACE" set image deployment/hialt-recall-consumer "consumer=$IMAGE"
wait_for_deployment hialt-recall-ui
wait_for_deployment hialt-recall-consumer
