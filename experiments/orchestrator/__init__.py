"""
Experiment Orchestrator for Flink-on-Kubernetes

Modular orchestration of:
- Flink job submission
- Pod/TM topology resolution
- Core pinning (via daemonset API)
- DVFS control (via node API)
- Workload ramp execution
- Multi-source metric collection (Flink REST, VM util, power)
"""

__version__ = "0.1.0"

from .run_context import (
    RunContext, RunConfig,
    RampStep, PinningConfig, ThreadPinningConfig, ThreadPinningPolicy, DvfsConfig, WorkloadConfig,
    generate_run_id
)
from .orchestrator import ExperimentOrchestrator, OrchestratorState
from .scraper import Scraper, ScraperResult, DummyScraper
from .ticker import Ticker
from .topology import TopologyResolver, FlinkTopologyResolver, KubeTopologyResolver
from .dvfs_client import DvfsClient
from .pinner_client import PinnerClient
from .workload_driver import WorkloadDriver, get_available_queries, validate_query_setup
from .port_forward import PortForwardManager
from .scrapers import FlinkRestScraper, PowerScraper, CpuUtilScraper, PrometheusScraper, FrequencyScraper

__all__ = [
    "RunContext",
    "RunConfig",
    "RampStep",
    "PinningConfig",
    "ThreadPinningConfig",
    "ThreadPinningPolicy",
    "DvfsConfig",
    "WorkloadConfig",
    "generate_run_id",
    "ExperimentOrchestrator",
    "OrchestratorState",
    "Scraper",
    "ScraperResult",
    "DummyScraper",
    "Ticker",
    "TopologyResolver",
    "FlinkTopologyResolver",
    "KubeTopologyResolver",
    "DvfsClient",
    "PinnerClient",
    "WorkloadDriver",
    "get_available_queries",
    "validate_query_setup",
    "PortForwardManager",
    "FlinkRestScraper",
    "PowerScraper",
    "CpuUtilScraper",
    "PrometheusScraper",
    "FrequencyScraper",
]

