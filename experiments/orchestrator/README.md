# Experiment Orchestrator

Modular orchestration framework for Flink-on-Kubernetes experiments with core pinning, DVFS control, and multi-source metric collection.

## Architecture

### Core Components

**Phase 1 (Foundation) - ✅ IMPLEMENTED:**

1. **`RunContext`** - Shared state and resources
   - Run configuration snapshot
   - File paths and output handles
   - HTTP session
   - Tick counter
   - Resolved topology (jobId, vertices, TM pods)

2. **`ExperimentOrchestrator`** - Main state machine
   - 8-state workflow execution
   - Critical vs non-critical failure handling
   - Event logging

3. **`Scraper` interface** - Base for metric collection
   - Standardized result format (JSONL)
   - Timeout protection
   - Error handling

4. **`Ticker`** - Periodic scraper driver
   - Fixed-interval clock
   - Parallel scraper execution with timeouts
   - Robust: one scraper failure doesn't block others
   - Per-scraper JSONL output

5. **`TopologyResolver`** - Discover runtime topology
   - **FlinkTopologyResolver**: jobId, vertices via REST
   - **KubeTopologyResolver**: TM pods, pod→node mapping

**Phase 2 (Actions) - ✅ IMPLEMENTED:**

6. **`WorkloadDriver`** - Flink job submission and management
   - Integrates with query scripts (query1, query5mod)
   - Builds ratelist from ramp steps
   - Cluster lifecycle management

7. **`PinnerClient`** - Pod→core pinning via DaemonSet
   - HTTP client for `src/core-controllers/pin` agent
   - Pin pods to specific CPU cores
   - Set cgroup CPU quotas
   - Supports cgroup v1 and v2

8. **`DvfsClient`** - CPU frequency control via DaemonSet
   - HTTP client for `src/dvfs-controller` agent
   - Set/get core frequencies per node
   - Multi-node operations

**Phase 3 (Observability) - TODO:**

10. **`FlinkRestScraper`** - Discovery-based Flink metrics
11. **`VmUtilScraper`** - VM utilization via API
12. **`PowerScraper`** - Physical node power via API

## State Machine

```
0. Initialize Run       → Create run folder, metadata
1. Submit Flink Job     → (External for now)
2. Wait for Readiness   → Flink REST + TM pods Ready
3. Prewarm             → No logging, health checks only
4. Apply Pinning       → Pod→VM cores (if enabled)
5. Apply DVFS          → Physical CPU freq (if enabled)
6. Settle Window       → Let system stabilize
7. Run Experiment      → Workload ramp + metric collection
8. Teardown            → Stop ticker, finalize
```

## Output Structure

```
runs/<run_id>/
  meta.json           # Configuration snapshot
  events.log          # Orchestration events (human-readable)
  metrics/
    flink_rest.jsonl  # Flink metrics per tick
    vm_util.jsonl     # VM utilization per tick
    power.jsonl       # Physical node power per tick
    workload.jsonl    # Workload achieved rates per tick
```

### JSONL Record Format

Every line is a JSON object with mandatory fields:

```json
{
  "t_unix_ms": 1706457845123,
  "iso_time": "2026-01-28T15:30:45.123Z",
  "tick": 42,
  "source": "flink_rest",
  "ok": true,
  "scrape_duration_ms": 250,
  "data": {
    // Source-specific payload
  }
}
```

On error:
```json
{
  "ok": false,
  "error": "Connection timeout",
  "error_type": "TimeoutError",
  "scrape_duration_ms": 5000
}
```

## Usage

### Basic Example

```python
from orchestrator.run_context import (
    RunContext, RunConfig, generate_run_id,
    PinningConfig, DvfsConfig, WorkloadConfig, RampStep
)
from orchestrator.orchestrator import ExperimentOrchestrator
from orchestrator.scraper import DummyScraper
from orchestrator.ticker import Ticker

# Configure experiment
config = RunConfig(
    run_id=generate_run_id("query1"),
    query_name="query1",
    namespace="default",
    flink_rest_url="http://localhost:8081",
    tick_seconds=5,
    prewarm_seconds=10,
    settle_seconds=10,
    pinning=PinningConfig(enabled=False),
    dvfs=DvfsConfig(enabled=False),
    workload=WorkloadConfig(
        generator_type="nexmark",
        ramp_steps=[
            RampStep(0, 100, 30),  # 100 RPS for 30s
            RampStep(1, 200, 30),  # 200 RPS for 30s
        ]
    )
)

# Create context and components
ctx = RunContext(config)
ticker = Ticker(ctx, scrapers=[DummyScraper("flink_rest")], tick_seconds=5)

# Create and run orchestrator
orchestrator = ExperimentOrchestrator(ctx)
orchestrator.ticker = ticker

success = orchestrator.execute()
```

### Run Example

```bash
cd /home/achilleas/boston/Kubeflink/experiments

# Ensure Flink job is running and port-forwarded
kubectl port-forward svc/flink-query1-rest 8081:8081 &

# Run orchestrator example
./run_example_orchestrator.py
```

## Failure Semantics

### Critical Failures (Abort Run)
- Workload driver crash
- Flink job enters failed/canceled state
- REST unreachable for prolonged time
- Topology cannot be resolved

### Non-Critical Failures (Log + Continue)
- Single scraper fails/times out
- Missing metrics (version/config differences)
- Transient network errors

All errors logged in:
- `events.log` (human-readable)
- Source JSONL as `ok=false` record

## Next Steps

### Phase 2: Actions
- [ ] Implement `PinnerClient` (daemonset API wrapper)
- [ ] Implement `MappingProvider` (file-based + API)
- [ ] Implement `DvfsClient` (node API + VM→physical mapping)
- [ ] Implement `WorkloadDriver` (Nexmark + ramp steps)

### Phase 3: Observability
- [ ] Implement `FlinkRestScraper` with discovery
  - Enumerate metrics via REST
  - Collect: throughput, backpressure, busy/idle, memory
  - TM-level + per-vertex/operator
- [ ] Implement `VmUtilScraper` (per-VM core utilization)
- [ ] Implement `PowerScraper` (per-node power)

### Phase 4: Integration
- [ ] Integrate with Query1/Query5mod runners
- [ ] Add job submission automation
- [ ] Add post-processing (JSONL → analysis)
- [ ] Optional: Prometheus scraper alternative

## Design Principles

1. **Discovery-based metrics**: Don't hardcode metric names - discover and collect what's available
2. **Explicit event anchors**: Emit RAMP_STEP_START/END, PIN_APPLIED, etc. for post-hoc alignment
3. **Resilient scraping**: One scraper failure doesn't stop the run
4. **Append-only logging**: JSONL per tick, never rewrite
5. **Best-effort collection**: Collect TM-level off-heap always; per-operator if available

## Files

```
orchestrator/
  __init__.py           # Package init with exports
  run_context.py        # RunContext, RunConfig, dataclasses
  orchestrator.py       # ExperimentOrchestrator state machine
  scraper.py            # Scraper interface, ScraperResult
  ticker.py             # Ticker with timeout protection
  topology.py           # FlinkTopologyResolver, KubeTopologyResolver
  workload_driver.py    # WorkloadDriver (query submission)
  pinner_client.py      # PinnerClient (core pinning)
  dvfs_client.py        # DvfsClient (frequency control)

  # TODO:
  mapping.py            # MappingProvider (pod→core mappings)
  scrapers/
    flink_rest.py       # FlinkRestScraper
    vm_util.py          # VmUtilScraper
    power.py            # PowerScraper
```

## Client Usage Examples

### WorkloadDriver

```python
from orchestrator import WorkloadDriver, validate_query_setup

# Validate query is ready
status = validate_query_setup("query1")
if not status["jar_exists"]:
    print("Build JAR first: mvn clean package -Pquery1 -DskipTests")

# Submit job
driver = WorkloadDriver("query1")
result = driver.submit_job(
    ratelist="100_60_200_60_300_60",  # 100 RPS for 60s, 200 RPS for 60s, 300 RPS for 60s
    capture_output=True
)

if result["ok"]:
    print(f"Job submitted: {result['cluster_id']}")
else:
    print(f"Failed: {result['error']}")

# Cleanup when done
driver.cleanup_cluster(namespace="default")
```

### PinnerClient

```python
from orchestrator import PinnerClient

client = PinnerClient(timeout=10)

# Pin pod to cores 0-3
result = client.pin_pod_cores(
    node_ip="10.0.0.1",
    pod_name="flink-query1-taskmanager-1",
    cores="0-3"
)
print(result["message"])

# Set CPU quota (150% = 1.5 cores)
result = client.set_cgroup_quota(
    node_ip="10.0.0.1",
    pod_name="flink-query1-taskmanager-1",
    quota_pct=150.0
)

# Batch pin multiple pods
configs = [
    {"node_ip": "10.0.0.1", "pod_name": "tm-1", "cores": "0-3"},
    {"node_ip": "10.0.0.2", "pod_name": "tm-2", "cores": "0-3"},
]
results = client.pin_pods_batch(configs)

# Start CPU utilization monitor (host scope)
client.start_cpu_monitor(
    node_ip="10.0.0.1",
    scope="host",
    cpus="all",
    interval_s=1.0,
    window_s=0.5,
    history_size=60
)

# Get cached CPU utilization
util = client.get_cpu_utilization_by_cpu(
    node_ip="10.0.0.1",
    scope="host",
    cpus="2,3",
    mode="last"  # Use cached data from monitor
)
# Returns: {'per_cpu': {'cpu2': 45.2, 'cpu3': 67.8}, 'timestamp': ...}

# Container-scope monitoring (after pinning)
client.start_cpu_monitor(
    node_ip="10.0.0.1",
    scope="container",
    pod_name="flink-query1-taskmanager-1",
    cpus="0-3"
)
```

### DvfsClient

```python
from orchestrator import DvfsClient

client = DvfsClient(timeout=5)

# Set frequency for cores 0-3 to 2.4 GHz
result = client.set_frequency(
    node_ip="10.0.0.1",
    cores=["0", "1", "2", "3"],
    freq_khz=2400000,
    reset=True  # Force set even if current is higher
)

# Get current frequencies
freqs = client.get_frequencies(node_ip="10.0.0.1")
print(freqs)  # {"cpu0": "2400000", "cpu1": "2400000", ...}

# Multi-node configuration
configs = {
    "10.0.0.1": {"cores": ["0", "1", "2", "3"], "freq_khz": 2400000},
    "10.0.0.2": {"cores": ["0", "1", "2", "3"], "freq_khz": 1800000}
}
results = client.set_frequency_multi_node(configs)

# Get node power (Watts)
power = client.get_power(node_ip="10.0.0.1")
print(power)  # {"power": 72.4, "request_duration_ms": 15}

# Multi-node power readings
node_ips = ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
results = client.get_power_multi_node(node_ips)
# Returns: {"10.0.0.1": {"ok": True, "power": 72.4}, ...}
```

## Dependencies

```bash
pip install requests kubernetes
```

## Notes

- Job submission is currently external (assumes job already running)
- Pinning/DVFS clients are stubs (require daemonset/node API endpoints)
- Dummy scrapers used in example (real scrapers TODO)
- K8s metrics collection intentionally omitted (interface reserved)
