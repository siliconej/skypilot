#!/bin/bash
# Deletes a pod of a Kubernetes cluster while `sky launch` waits for its Ray
# cluster to form, and checks that the launch fails fast and names the pod.
#
# Usage:
#   delete_pod_mid_launch.sh <cluster> <worker|head> <max_seconds> \
#       <sky launch args...>
#
# The pod is force-deleted (no grace period) once the head is running the
# Ray wait, i.e. right after runtime setup finished on every node. The
# launch must then exit non-zero within <max_seconds> of the deletion, with
# InstancesLostDuringSetupError naming the pod. Before the fail-fast fix it
# took ~106 s: the Ray wait ran its full 90 s and the fallback retried a pod
# that no longer existed.
#
# The window between runtime setup and every node joining Ray is a few
# seconds. If the launch gets through it anyway (it succeeds, or fails
# quickly for another reason because the pod was deleted too late), the
# attempt is retried with a fresh cluster, up to 3 times.
set -u
CLUSTER=$1
TARGET=$2
MAX_SECONDS=$3
shift 3
LOG=$(mktemp)
DONE=$(mktemp -u)
trap 'rm -f "$LOG" "$DONE"' EXIT

# The context and namespace that hold the cluster's pods (like
# smoke_tests_utils.kubectl_for_cluster, resolved once).
find_kubectl() {
  local c ns
  for c in $(kubectl config get-contexts -o name); do
    ns=$(kubectl --context "$c" get pods -A -l parent=skypilot \
      -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' \
      2>/dev/null | awk -v p="$CLUSTER-" 'index($2, p) == 1 {print $1; exit}')
    if [ -n "$ns" ]; then
      echo "kubectl --context $c -n $ns"
      return 0
    fi
  done
  return 1
}

for attempt in 1 2 3; do
  echo "=== attempt $attempt: delete the $TARGET pod of $CLUSTER mid-launch"
  rm -f "$DONE"
  ( sky launch -y -c "$CLUSTER" "$@" > "$LOG" 2>&1; echo $? > "$DONE" ) &

  until KC=$(find_kubectl); do
    if [ -f "$DONE" ]; then
      echo "Launch ended before its pods appeared:"; cat "$LOG"; exit 1
    fi
    sleep 1
  done
  HEAD=$($KC get pods -o name | grep "/$CLUSTER-" | grep -m1 -- '-head')
  # The head runs the Ray wait (shipped as base64) only after runtime setup
  # finished on every node.
  until $KC exec "$HEAD" -c ray-node -- \
      pgrep -f 'import base64; exec' > /dev/null 2>&1; do
    [ -f "$DONE" ] && break
    sleep 0.2
  done
  if [ -f "$DONE" ]; then
    echo "Launch ended (exit $(cat "$DONE")) before the Ray wait was seen;" \
      "retrying."
    tail -5 "$LOG"
    sky down -y "$CLUSTER" > /dev/null 2>&1
    continue
  fi

  POD=$($KC get pods -o name | grep "/$CLUSTER-" | grep -m1 -- "-$TARGET")
  $KC delete "$POD" --grace-period=0 --force --wait=false 2>&1 \
    | grep -v '^Warning'
  DELETED_AT=$(date +%s)
  until [ -f "$DONE" ]; do sleep 0.5; done
  ELAPSED=$(( $(date +%s) - DELETED_AT ))
  RC=$(cat "$DONE")
  POD_NAME=${POD#pod/}
  echo "Launch exited $RC, ${ELAPSED}s after $POD_NAME was deleted."
  tail -5 "$LOG"
  sky down -y "$CLUSTER" > /dev/null 2>&1

  if [ "$ELAPSED" -gt "$MAX_SECONDS" ]; then
    echo "FAIL: took ${ELAPSED}s to notice the deletion (max ${MAX_SECONDS}s)."
    exit 1
  fi
  if [ "$RC" != 0 ] && grep -q 'InstancesLostDuringSetupError' "$LOG" &&
      grep -q -- "$POD_NAME" "$LOG"; then
    echo "OK: failed fast and named the deleted pod."
    exit 0
  fi
  echo "The deletion landed after the Ray wait finished; retrying."
done
echo "FAIL: no attempt deleted the pod during the Ray wait."
exit 1
