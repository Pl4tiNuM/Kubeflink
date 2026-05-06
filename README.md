# Query5 Benchmark Reproducibility Guide

This README describes how to reproduce the Query5 benchmark using:
- Static TaskManager placement
- Fixed vertex scheduling
- External power agent
- Demo orchestrator

The steps below assume a Kubernetes-based Flink deployment.

---

## Prerequisites

- Kubernetes cluster running Flink
- `kubectl` configured
- Access to server **cheetara**
- Python 3 installed on cheetara and control node


---

## 1) Fix Scheduler and Placement

This benchmark uses **static placement**:
1. TaskManager → Kubernetes node mapping
2. Vertex → TaskManager mapping

Both are injected via Kubernetes ConfigMaps.

---

## 2) Configure TM → Node Mapping

In Control-plane VM (.228) configure the file:

```
query5mod_tm_config.csv
```

Contents:

```csv
id,k8s_cpu,jvm_task_heap,jvm_task_offheap,jvm_network,jvm_managed,flink_slots,k8s_affinity
1,2,2048,1024,1024,2048,2,kubeflink-minion
2,2,2048,1024,1024,2048,2,kubeflink-worker-01
3,2,2048,1024,1024,2048,2,kubeflink-worker-02
4,2,2048,1024,1024,2048,2,kubeflink-minion
5,2,2048,1024,1024,2048,2,kubeflink-worker-01
6,2,2048,1024,1024,2048,2,kubeflink-worker-02
```

### Field Overview

- `id`: TM identifier used by scheduler
- `k8s_affinity`: target Kubernetes node
- `k8s_cpu`: requested CPU
- `jvm_*`: Flink memory layout
- `flink_slots`: slots per TaskManager

---

## 3) Configure Vertex → TM Mapping

Also, create or update the scheduler config file:

```
scripts/query5_conf
```

Contents:

```
Q1_Source; flink-query5mod-taskmanager-1-1
Transform; flink-query5mod-taskmanager-1-2
SlidingWindow; flink-query5mod-taskmanager-1-3
Q1_Sink; flink-query5mod-taskmanager-1-4
```

This pins each vertex to a specific TaskManager pod.

---

## 4) Apply ConfigMaps

Run from the directory containing the config files.

### Scheduler ConfigMap

```bash
kubectl create configmap flink-schedulercfg \
  --from-file=schedulercfg="$(pwd)/scripts/query5_conf"
```

### TM Mapping ConfigMap

```bash
kubectl create configmap tm-configmap \
  --from-file=tms_config.csv="$(pwd)/query5mod_tm_config.csv"
```

### Verify

```bash
kubectl get configmap flink-schedulercfg tm-configmap
kubectl describe configmap flink-schedulercfg
kubectl describe configmap tm-configmap
```

> If ConfigMaps already exist, delete and recreate them or use `kubectl apply`.

---

## 5) Start the Power Agent (cheetara)

The agent must run on **cheetara**.

Agent path:

```
kubeflink/agent.py
```

---


### Start the agent

Preferred (sudo):

```bash
sudo python3 agent.py
```

If using a virtualenv:

```bash
sudo -E python3 agent.py
```

---

### Run in Background (Optional)

```bash
sudo nohup python3 agent.py > agent.log 2>&1 &
```

Monitor logs:

```bash
tail -f agent.log
```

---

### Verify Agent

```bash
ps aux | grep agent.py
```

Optional port check:

```bash
ss -lntp | grep python
```

---

## 6) Run the Demo Orchestrator

After the agent is running, start the orchestrator.

Script:

```
demo_orchestrator.py
```

---

### Step 1 — Move to

```bash
cd ~/kubeflink/experiments/
```

---

### Step 2 — Run with Query5 config

```bash
python3 demo_orchestrator.py \
  --config config_examples/experiment_query5mod.json
```

This configuration:
- Uses Query5 modified topology
- Assumes fixed placement configs
- Communicates with the external agent

The current `experiment_query5mod.json` can also drive newer orchestrator features such as:

- pod-level pinning
- thread-level pinning
- CPU governor control
- frequency monitoring via `frequency_configs`

If you want to use or adjust those options, see:

```text
experiments/config_examples/README.md
```

and:

```text
experiments/config_examples/experiment_query5mod.json
```

---

### Stop the Orchestrator

Foreground: `Ctrl+C`

Or kill:

```bash
pkill -f demo_orchestrator.py
```

Or wait for it to finish.

---
