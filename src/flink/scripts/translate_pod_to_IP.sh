#!/usr/bin/env bash
set -euo pipefail

# Where your scheduler expects the config
CFG="${FLINK_CUSTOM_HOME:-/opt/flink}/scripts/schedulercfg_raw"
CFG_OUT="${FLINK_CUSTOM_HOME:-/opt/flink}/scripts/schedulercfg"

NS="${POD_NAMESPACE:-default}"
API="https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT}"
TOKEN_FILE="/var/run/secrets/kubernetes.io/serviceaccount/token"
CACERT="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

echo "DEBUG: CFG=$CFG"
echo "DEBUG: NS=$NS"
echo "DEBUG: API=$API"

# If we don't have a schedulercfg, just chain to Flink
if [ ! -f "$CFG" ]; then
  echo "No schedulercfg found at $CFG, starting Flink normally"
  exec /docker-entrypoint.sh "$@"
fi

if [ ! -f "$TOKEN_FILE" ]; then
  echo "No K8s serviceaccount token, cannot resolve pod IPs; starting Flink normally"
  exec /docker-entrypoint.sh "$@"
fi

TOKEN="$(cat "$TOKEN_FILE")"
TMP="$(mktemp)"

echo "Pre-start: rewriting $CFG (pod names → pod IPs) in namespace $NS"
echo "DEBUG: current schedulercfg content:"
cat "$CFG" || echo "DEBUG: cannot cat $CFG"

while IFS= read -r line; do
    # skip empty lines
    if [ -z "$line" ]; then
        echo "DEBUG: skipping empty line"
        continue
    fi

    echo "DEBUG: raw line: '$line'"

    # Format: Operator; PodName
    op="${line%%;*}"
    pod="${line#*;}"
    op="$(echo "$op" | xargs)"
    pod="$(echo "$pod" | xargs)"

    echo "DEBUG: op='$op', pod='$pod'"

    # Query K8s API: GET /api/v1/namespaces/{ns}/pods/{pod}
    resp="$(curl -sS -w '\nHTTP_STATUS:%{http_code}\n' \
        --connect-timeout 3 --max-time 5 \
        --cacert "$CACERT" \
        -H "Authorization: Bearer $TOKEN" \
        "$API/api/v1/namespaces/$NS/pods/$pod" || true)"

    status="$(printf '%s\n' "$resp" | awk -F: '/HTTP_STATUS/ {print $2}' | tr -d '[:space:]')"
    body="$(printf '%s\n' "$resp" | sed '/HTTP_STATUS:/d')"

    echo "DEBUG: HTTP status for pod '$pod' is '$status'"

    if [ -z "$status" ]; then
        echo "WARN: no HTTP status for pod '$pod' (curl failed or timed out)"
        echo "WARN: Raw response: $resp"
        ip="$pod"
    elif [ "$status" != "200" ] || [ -z "$body" ]; then
        echo "WARN: API call for pod '$pod' failed (status=$status)"
        echo "WARN: Response body: $body"
        ip="$pod"
    else
    # Extract podIP tolerant to spaces: "podIP": "10.244.2.43"
    ip="$(printf '%s\n' "$body" \
        | sed -n 's/.*"podIP"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
        | head -n1 || true)"

    if [ -z "$ip" ]; then
        echo "WARN: pod '$pod' has no podIP in response, keeping name as-is"
        ip="$pod"
    else
        echo "DEBUG: pod '$pod' → IP '$ip'"
    fi
fi
    echo "$op; $ip" >> "$TMP"

done < "$CFG"

mv "$TMP" "$CFG_OUT"

echo "Pre-start: final schedulercfg:"
cat "$CFG_OUT"

# Hand over control to the original Flink entrypoint
exec /docker-entrypoint.sh "$@"
