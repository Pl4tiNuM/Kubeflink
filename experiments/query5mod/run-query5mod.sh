#!/bin/bash

# Run Query5mod on Kubernetes Flink cluster
# This query performs sliding window aggregation with bid transformation

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
JAR_FILE="$SCRIPT_DIR/../target/Query5mod.jar"

# Check if JAR exists
if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: JAR file not found at $JAR_FILE"
    echo "Please run 'mvn clean package -Pquery5mod -DskipTests' in the experiments directory first"
    exit 1
fi

# Query5mod parameters (use environment variables or defaults)
CLUSTER_ID="flink-query5mod"
# RATELIST="${RATELIST:-5000_7200000}"  # Default: 5000 events/sec for 2 hours
RATELIST="${RATELIST:-5000_60000_10000_60000_15000_60000_20000_60000_30000_60000_40000_60000_60000_60000}"  # Use env var or default
TOPSIZE="${TOPSIZE:-5}"
SWL_MIN="${SWL_MIN:-60}"  # Sliding window length in minutes
SWS_MIN="${SWS_MIN:-1}"   # Sliding window slide in minutes
WTM_MS="${WTM_MS:-1000}" # Watermark interval in milliseconds
EXTSIZE="${EXTSIZE:-1000}" # Extra computation size

echo "Starting Flink application with cluster ID: $CLUSTER_ID"
echo "JAR: $JAR_FILE"
echo "Rate list: $RATELIST"
echo "Parameters: topsize=$TOPSIZE swl_min=$SWL_MIN sws_min=$SWS_MIN wtm_ms=$WTM_MS extsize=$EXTSIZE"

docker run --rm -it \
  --network host \
  -v "$PROJECT_ROOT/.kube:/.kube" \
  -v "$PROJECT_ROOT/pod_template.yaml:/pod_template.yaml:ro" \
  -e KUBECONFIG=/.kube/config \
  iwita/kubeflink:v0.1-nexmark \
  /opt/flink/bin/flink run --target kubernetes-application \
    -Dkubernetes.cluster-id="$CLUSTER_ID" \
    -Dkubernetes.container.image.ref=iwita/kubeflink:v0.1-nexmark \
    -Dkubernetes.container.image.pull-policy=Always \
    -Dkubernetes.taskmanager.replicas=4 \
    -Dparallelism.default=1 \
    -Dkubernetes.cluster.persist-on-exception=true \
    -Dkubernetes.cluster.persist.deployment=true \
    -Dresourcemanager.taskmanager-timeout=100 \
    -Dkubernetes.rest-service.exposed.type=ClusterIP \
    -Dkubernetes.pod-template-file.default=/pod_template.yaml \
    -Denv.log.dir=/var/log/flink \
    -Dkubernetes.flink.log.dir=/var/log/flink \
    -Djobmanager.scheduler=Custom \
    local:///opt/flink/usrlib/Query5mod.jar \
    --ratelist "$RATELIST" \
    --topsize "$TOPSIZE" \
    --swl_min "$SWL_MIN" \
    --sws_min "$SWS_MIN" \
    --wtm_ms "$WTM_MS" \
    --extsize "$EXTSIZE" \
    --psrc 1 \
    --ptrans 1 \
    --pwindow 1 \
    --psink 1
