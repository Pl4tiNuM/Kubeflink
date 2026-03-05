#!/bin/bash

# Run Query1 on Kubernetes Flink cluster
# This query performs currency conversion on bid events

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
JAR_FILE="$SCRIPT_DIR/../target/Query1.jar"

# Check if JAR exists
if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: JAR file not found at $JAR_FILE"
    echo "Please run 'mvn clean package -Pquery1 -DskipTests' in the experiments directory first"
    exit 1
fi

# Query1 parameters
CLUSTER_ID="flink-query1"
RATELIST="${RATELIST:-50000_60000_100000_60000_200000_60000_300000_60000}"  # Use env var or default
EXCHANGE_RATE="${EXCHANGE_RATE:-0.82}"

# Check for existing cluster with same ID
echo "Checking for existing Flink cluster with ID: $CLUSTER_ID"
if kubectl get deployment -l app=$CLUSTER_ID 2>/dev/null | grep -q $CLUSTER_ID; then
    echo "WARNING: Found existing Flink cluster with ID '$CLUSTER_ID'"
    echo "This may cause conflicts. Consider cleaning up first:"
    echo "  kubectl delete deployment,service,configmap -l app=$CLUSTER_ID"
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo "Starting Flink application with cluster ID: $CLUSTER_ID"
echo "JAR: $JAR_FILE"
echo "Rate list: $RATELIST"

docker run --rm -it \
  -v $(pwd)/../../.kube:/.kube \
  -v $(pwd)/../../pod_template.yaml:/pod_template.yaml:ro \
  -e KUBECONFIG=/.kube/config \
  iwita/kubeflink:v0.1-nexmark \
  /opt/flink/bin/flink run --target kubernetes-application \
    -Dkubernetes.cluster-id="$CLUSTER_ID" \
    -Dkubernetes.container.image.ref=iwita/kubeflink:v0.1-nexmark \
    -Dkubernetes.container.image.pull-policy=Always \
    -Dkubernetes.taskmanager.replicas=3 \
    -Dparallelism.default=1 \
    -Dkubernetes.cluster.persist-on-exception=true \
    -Dkubernetes.cluster.persist.deployment=true \
    -Dresourcemanager.taskmanager-timeout=100 \
    -Dkubernetes.rest-service.exposed.type=ClusterIP \
    -Dkubernetes.pod-template-file.default=/pod_template.yaml \
    -Denv.log.dir=/var/log/flink \
    -Dkubernetes.flink.log.dir=/var/log/flink \
    -Djobmanager.scheduler=Custom \
    local:///opt/flink/usrlib/Query1.jar \
    --ratelist "$RATELIST" \
    --exchange-rate "$EXCHANGE_RATE" \
    --psrc 1 \
    --pmap 1 \
    --psink 1
