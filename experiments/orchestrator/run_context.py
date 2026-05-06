"""
RunContext: Shared state and resources for a single experiment run.

Holds:
- run_id, paths, config snapshot
- tick index counter
- shared HTTP session/client
- resolved topology handles (jobId, vertices, TM pods)
"""

import json
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Any
import requests
from datetime import datetime


@dataclass
class RampStep:
    """Single step in the workload ramp schedule"""
    step_idx: int
    target_rps: float
    duration_s: int


@dataclass
class PinningConfig:
    """Pod→VM-core pinning configuration"""
    enabled: bool
    mapping_source: str  # "file", "api", "deterministic"
    mapping_file: Optional[str] = None
    pod_core_map: Dict[str, List[int]] = field(default_factory=dict)


@dataclass
class ThreadPinningPolicy:
    """Single thread pinning policy (pattern → cores)"""
    thread_pattern: str
    cores: str
    pod_pattern: Optional[str] = None
    namespace_pattern: Optional[str] = None
    container_pattern: Optional[str] = None
    only_if_cmdline_matches: Optional[str] = None
    reapply_seconds: Optional[int] = None


@dataclass
class ThreadPinningConfig:
    """Thread-level pinning configuration (pin specific threads by pattern to CPU cores)"""
    enabled: bool
    policies: List['ThreadPinningPolicy'] = field(default_factory=list)


@dataclass
class GovernorEntry:
    """Single governor assignment: a set of cores on a node → governor"""
    node_ip: str
    cores: str    # e.g. "0-3,8" or "all"
    governor: str  # "performance", "powersave", "ondemand", "conservative", "schedutil"


@dataclass
class GovernorConfig:
    """CPU governor configuration (independent of frequency DVFS)"""
    enabled: bool
    entries: List['GovernorEntry'] = field(default_factory=list)


@dataclass
class DvfsConfig:
    """DVFS frequency configuration for physical CPUs"""
    enabled: bool
    mapping_file: Optional[str] = None  # VM-core → physical CPU mapping
    target_freq_ghz: Optional[float] = None
    per_core_freq: Dict[str, float] = field(default_factory=dict)


@dataclass
class WorkloadConfig:
    """Workload driver configuration"""
    generator_type: str  # "nexmark", "custom"
    endpoint: Optional[str] = None
    ramp_steps: List[RampStep] = field(default_factory=list)
    extra_params: Dict[str, Any] = field(default_factory=dict)  # Query-specific extra parameters


@dataclass
class RunConfig:
    """Complete experiment configuration snapshot"""
    run_id: str
    query_name: str
    namespace: str
    flink_rest_url: str

    # Cluster
    expected_tm_count: int = 3
    vm_ips: List[str] = field(default_factory=list)  # VM IPs for Pinner agent (CPU util)
    cpu_cores_file: Optional[str] = None  # Path to CPU cores configuration file
    physical_node_ips: List[str] = field(default_factory=list)  # Physical node IPs for DVFS agent (Power/Freq)
    power_socket: Optional[str] = None  # Which socket to read power from: "package-0", "package-1", or None for total
    frequency_configs: Optional[Dict[str, str]] = None  # Node IP -> cores specification for frequency monitoring
    prometheus_url: Optional[str] = None  # Prometheus server URL for Scaphandre metrics
    image_tag: Optional[str] = None

    # Timing
    tick_seconds: int = 5
    settle_seconds: int = 5

    # Tuning configurations
    pinning: Optional['PinningConfig'] = None
    thread_pinning: Optional['ThreadPinningConfig'] = None
    governor: Optional['GovernorConfig'] = None
    dvfs: Optional['DvfsConfig'] = None
    workload: Optional['WorkloadConfig'] = None

    # Metadata
    git_commit: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dict"""
        return asdict(self)


class RunContext:
    """
    Shared context for a single experiment run.

    Provides:
    - File paths and handles
    - HTTP session
    - Configuration
    - Resolved topology
    - Tick counter
    """

    def __init__(self, config: RunConfig, base_runs_dir: Path = None):
        self.config = config

        # Create run directory
        if base_runs_dir is None:
            base_runs_dir = Path(__file__).parent.parent / "runs"
        self.base_runs_dir = Path(base_runs_dir)
        self.run_dir = self.base_runs_dir / config.run_id
        self.metrics_dir = self.run_dir / "metrics"

        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.metrics_dir.mkdir(exist_ok=True)

        # Paths
        self.meta_path = self.run_dir / "meta.json"
        self.events_path = self.run_dir / "events.log"

        # Metric output paths
        self.flink_rest_path = self.metrics_dir / "flink_rest.jsonl"
        self.vm_util_path = self.metrics_dir / "vm_util.jsonl"
        self.power_path = self.metrics_dir / "power.jsonl"
        self.workload_path = self.metrics_dir / "workload.jsonl"

        # HTTP session (shared across scrapers)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "FlinkExperimentOrchestrator/0.1"})

        # Tick counter
        self.tick_idx = 0

        # Resolved topology (populated by TopologyResolver)
        self.job_id: Optional[str] = None
        self.vertices: List[Dict[str, Any]] = []
        self.tm_pods: List[Dict[str, Any]] = []  # {name, node, ip, ...}

        # Timestamps
        self.created_at = datetime.utcnow().isoformat() + "Z"
        self.started_at: Optional[str] = None
        self.completed_at: Optional[str] = None

    def write_meta(self):
        """Write metadata JSON"""
        meta = self.config.to_dict()
        meta["created_at"] = self.created_at
        meta["started_at"] = self.started_at
        meta["completed_at"] = self.completed_at
        meta["run_dir"] = str(self.run_dir)

        with open(self.meta_path, "w") as f:
            json.dump(meta, f, indent=2)

    def log_event(self, event_type: str, **kwargs):
        """
        Append event to events.log

        Format: timestamp | event_type | JSON details
        """
        ts = datetime.utcnow().isoformat() + "Z"
        unix_ms = int(time.time() * 1000)

        details = {
            "t_unix_ms": unix_ms,
            "iso_time": ts,
            "event_type": event_type,
            **kwargs
        }

        with open(self.events_path, "a") as f:
            line = f"{ts} | {event_type} | {json.dumps(details)}\n"
            f.write(line)

    def next_tick(self) -> int:
        """Increment and return next tick index"""
        self.tick_idx += 1
        return self.tick_idx

    def close(self):
        """Cleanup resources"""
        self.session.close()


def generate_run_id(query_name: str) -> str:
    """
    Generate unique run ID

    Format: {query_name}_{timestamp}_{short_hash}
    Example: query1_20260128T153045Z_a3f2
    """
    import hashlib
    import random

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    # Short hash from timestamp + random
    hash_input = f"{timestamp}{random.random()}".encode()
    short_hash = hashlib.sha256(hash_input).hexdigest()[:4]

    return f"{query_name}_{timestamp}_{short_hash}"
