"""
CPU Utilization Scraper - Collects per-core CPU utilization from cluster nodes

Uses PinnerClient to query CPU utilization metrics from all nodes
running the Pinner agent.
"""

import logging
from typing import List, Dict, Optional

from ..scraper import Scraper, ScraperResult
from ..run_context import RunContext
from ..pinner_client import PinnerClient


class CpuUtilScraper(Scraper):
    """
    Scrapes per-core CPU utilization from cluster nodes via Pinner agent.

    Collects CPU utilization metrics from the continuous monitoring service
    exposed by the Pinner daemonset agent.

    Agent response format:
    {
      "ok": true,
      "mode": "last",
      "data": {
        "ok": true,
        "scope": "host",
        "cpus": [1, 2, 3],
        "util_pct_one_core": {"1": 2.08, "2": 8.0, "3": 10.0},
        "window_s": 0.5,
        "ts": 1769613628.63
      }
    }
    """

    def __init__(
        self,
        node_ips: List[str],
        port: int = 4002,
        scope: str = "host",
        mode: str = "last",
        cpus: Optional[List[int]] = None,
        cpu_cores_map: Optional[Dict[str, Dict]] = None,
        tm_pinning_map: Optional[Dict[str, Dict]] = None,
        timeout: int = 5
    ):
        """
        Args:
            node_ips: List of node IP addresses running Pinner agent
            port: Pinner agent port (default: 4002)
            scope: Monitoring scope - "host" or "container" (default: "host")
            mode: Sampling mode - "last" (cached) or "sample" (on-demand) (default: "last")
            cpus: List of CPU core IDs to monitor (e.g., [0, 1, 2, 3]). None means all CPUs.
            cpu_cores_map: Dict mapping node_ip to {"cores": [0, 1, 2, ...]}. Overrides cpus if provided.
            tm_pinning_map: Dict mapping pod_name to {"node_ip": str, "cores": [int]}. Used for TM-grouped output.
            timeout: Request timeout in seconds

        Example (basic):
            scraper = CpuUtilScraper(
                node_ips=['192.168.1.10', '192.168.1.11'],
                scope='host',
                mode='last'
            )

        Example (with cores file):
            cpu_cores_map = {
                "192.168.1.10": {"cores": [0, 1, 2, 3]},
                "192.168.1.11": {"cores": [0, 1, 2, 3, 4, 5, 6, 7]}
            }
            scraper = CpuUtilScraper(
                node_ips=['192.168.1.10', '192.168.1.11'],
                cpu_cores_map=cpu_cores_map
            )

        Example (container-level):
            scraper = CpuUtilScraper(
                node_ips=['192.168.1.10'],
                scope='container',
                mode='sample'
            )
        """
        self.node_ips = node_ips
        self.port = port
        self.scope = scope
        self.mode = mode
        self.cpus = cpus  # Default CPU list (if no per-node map)
        self.cpu_cores_map = cpu_cores_map or {}  # Per-node CPU core mappings
        self.tm_pinning_map = tm_pinning_map or {}  # TM pinning info for grouped output
        self.timeout = timeout

        # Create PinnerClient instances for each node
        self.clients = {
            ip: PinnerClient(timeout=timeout)
            for ip in node_ips
        }

        # Track monitoring status
        self.monitoring_started = False
        self.tracked_cpus_per_node = {}  # Store which CPUs each node's monitor is tracking

        logging.info(f"[CpuUtilScraper] Initialized for {len(node_ips)} nodes (scope={scope}, mode={mode})")

    @staticmethod
    def _format_core_range(cores: List[int]) -> str:
        """Format list of cores as compact range string (e.g., [0,1,2,3] -> '0-3')."""
        if not cores:
            return ""
        sorted_cores = sorted(cores)
        if len(sorted_cores) == 1:
            return str(sorted_cores[0])
        # Check if contiguous
        if sorted_cores == list(range(sorted_cores[0], sorted_cores[-1] + 1)):
            return f"{sorted_cores[0]}-{sorted_cores[-1]}"
        # Non-contiguous, show as comma-separated
        return ",".join(map(str, sorted_cores))

    @property
    def name(self) -> str:
        return "cpu_util"

    def start_monitoring(self) -> Dict[str, bool]:
        """
        Start CPU monitoring on all nodes.

        Should be called once before first scrape (typically during SETUP phase).

        Note: The monitor always tracks ALL CPUs. CPU filtering is done during scraping.

        Returns:
            Dict mapping node_ip to success status
        """
        results = {}

        for node_ip, client in self.clients.items():
            try:
                # Start monitor for ALL CPUs (filtering happens during scraping)
                result = client.start_cpu_monitor(
                    node_ip=node_ip,
                    scope=self.scope,
                    cpus="all",  # Monitor tracks all CPUs
                    interval_s=1.0,
                    window_s=0.5,
                    history_size=60
                )
                success = result.get("ok", False)
                results[node_ip] = success

                if success:
                    # Store the CPUs being tracked (will use for queries)
                    config = result.get("config", {})
                    tracked_cpus = config.get("cpus", [])
                    if tracked_cpus:
                        self.tracked_cpus_per_node[node_ip] = tracked_cpus
                    logging.info(f"[CpuUtilScraper] Started monitoring on {node_ip} (tracking {len(tracked_cpus)} CPUs)")
                else:
                    logging.warning(f"[CpuUtilScraper] Failed to start monitoring on {node_ip}: {result.get('error')}")

            except Exception as e:
                results[node_ip] = False
                logging.error(f"[CpuUtilScraper] Exception starting monitoring on {node_ip}: {e}")

        self.monitoring_started = any(results.values())
        return results

    def stop_monitoring(self) -> Dict[str, bool]:
        """
        Stop CPU monitoring on all nodes.

        Lists all active monitors and stops them by key.

        Should be called during teardown/cleanup.

        Returns:
            Dict mapping node_ip to success status
        """
        results = {}

        for node_ip, client in self.clients.items():
            try:
                # List all monitors on this node
                list_result = client.list_cpu_monitors(node_ip=node_ip)

                if not list_result.get("ok"):
                    logging.warning(f"[CpuUtilScraper] Failed to list monitors on {node_ip}: {list_result.get('error')}")
                    results[node_ip] = False
                    continue

                monitors = list_result.get("monitors", [])

                if not monitors:
                    logging.info(f"[CpuUtilScraper] No monitors running on {node_ip}")
                    results[node_ip] = True
                    continue

                # Stop each monitor by key
                stopped_count = 0
                for monitor in monitors:
                    key = monitor.get("key")
                    if key:
                        try:
                            stop_result = client.stop_cpu_monitor_by_key(node_ip=node_ip, key=key)
                            if stop_result.get("ok"):
                                stopped_count += 1
                                logging.info(f"[CpuUtilScraper] Stopped monitor {key} on {node_ip}")
                            else:
                                logging.warning(f"[CpuUtilScraper] Failed to stop monitor {key} on {node_ip}")
                        except Exception as e:
                            logging.error(f"[CpuUtilScraper] Exception stopping monitor {key} on {node_ip}: {e}")

                results[node_ip] = stopped_count > 0
                logging.info(f"[CpuUtilScraper] Stopped {stopped_count}/{len(monitors)} monitors on {node_ip}")

            except Exception as e:
                results[node_ip] = False
                logging.error(f"[CpuUtilScraper] Exception stopping monitoring on {node_ip}: {e}")

        self.monitoring_started = False
        return results

    def scrape(self, ctx: RunContext) -> ScraperResult:
        """Scrape CPU utilization from all nodes, organized by TaskManager."""
        try:
            # Check if monitoring is started
            if not self.monitoring_started:
                logging.warning("[CpuUtilScraper] Monitoring not started yet, skipping scrape")
                return ScraperResult.error(
                    tick=ctx.tick_idx,
                    source=self.name,
                    error="CPU monitoring not started",
                    error_type="MonitorNotStarted"
                )

            # First, collect raw CPU data from all nodes
            node_cpu_data = {}  # {node_ip: {core_id: util_pct}}
            errors = []

            for node_ip, client in self.clients.items():
                try:
                    # Determine which CPUs to request for this node
                    node_cpus = None

                    # Check per-node configuration first
                    if node_ip in self.cpu_cores_map:
                        cores_config = self.cpu_cores_map[node_ip]
                        if "cores" in cores_config:
                            node_cpus = cores_config["cores"]

                    # Fall back to global cpus setting
                    if node_cpus is None and self.cpus:
                        node_cpus = self.cpus

                    # If still no CPUs specified, use what the monitor is tracking
                    if node_cpus is None and node_ip in self.tracked_cpus_per_node:
                        node_cpus = self.tracked_cpus_per_node[node_ip]

                    # Convert to comma-separated string
                    if node_cpus:
                        cpus_str = ",".join(map(str, node_cpus))
                    else:
                        # Last resort: query first 16 cores (most VMs have at least this many)
                        cpus_str = "0-15"
                        logging.warning(f"[CpuUtilScraper] No CPU config for {node_ip}, defaulting to cpus=0-15")

                    # Get CPU utilization
                    result = client.get_cpu_utilization_by_cpu(
                        node_ip=node_ip,
                        scope=self.scope,
                        cpus=cpus_str,
                        mode=self.mode
                    )

                    if result.get("ok"):
                        cpu_utils_raw = result.get("cpu_utilization", {})
                        # Convert to int keys and float values
                        node_cpu_data[node_ip] = {int(k): float(v) for k, v in cpu_utils_raw.items() if v is not None}
                    else:
                        errors.append(f"{node_ip}: {result.get('error', 'Unknown error')}")
                        logging.warning(f"[CpuUtilScraper] Failed to get data from {node_ip}: {result.get('error')}")

                except Exception as e:
                    errors.append(f"{node_ip}: {str(e)}")
                    logging.warning(f"[CpuUtilScraper] Exception getting data from {node_ip}: {e}")

            # If all nodes failed, return error
            if not node_cpu_data:
                return ScraperResult.error(
                    tick=ctx.tick_idx,
                    source=self.name,
                    error=f"All nodes failed: {'; '.join(errors)}",
                    error_type="CpuUtilCollectionError"
                )

            # Now organize by TaskManager if pinning map available
            data = {}

            if self.tm_pinning_map:
                # Group by TaskManager
                data["taskmanagers"] = {}
                all_tm_utils = []

                for pod_name, pin_info in self.tm_pinning_map.items():
                    node_ip = pin_info["node_ip"]
                    cores = pin_info["cores"]

                    # Skip if we don't have data for this node
                    if node_ip not in node_cpu_data:
                        continue

                    # Extract utilization for this TM's cores
                    per_cpu_util = {}
                    tm_utils = []
                    for core_id in cores:
                        if core_id in node_cpu_data[node_ip]:
                            util = node_cpu_data[node_ip][core_id]
                            per_cpu_util[core_id] = util
                            tm_utils.append(util)

                    # Calculate aggregates
                    if tm_utils:
                        data["taskmanagers"][pod_name] = {
                            "node_ip": node_ip,
                            "cores": cores,
                            "core_range": self._format_core_range(cores),
                            "num_cores": len(cores),
                            "per_cpu_utilization": per_cpu_util,
                            "avg_utilization_pct": round(sum(tm_utils) / len(tm_utils), 2),
                            "max_utilization_pct": round(max(tm_utils), 2),
                            "min_utilization_pct": round(min(tm_utils), 2)
                        }
                        all_tm_utils.extend(tm_utils)

                # Global aggregate across all TMs
                if all_tm_utils:
                    data["aggregate"] = {
                        "total_taskmanagers": len(data["taskmanagers"]),
                        "total_cores": len(all_tm_utils),
                        "avg_utilization_pct": round(sum(all_tm_utils) / len(all_tm_utils), 2),
                        "max_utilization_pct": round(max(all_tm_utils), 2),
                        "min_utilization_pct": round(min(all_tm_utils), 2)
                    }
            else:
                # No TM pinning map - fall back to per-node organization
                data["nodes"] = {}
                all_utils = []

                for node_ip, cpu_utils in node_cpu_data.items():
                    utils = list(cpu_utils.values())
                    if utils:
                        data["nodes"][node_ip] = {
                            "num_cpus": len(utils),
                            "per_cpu_utilization": cpu_utils,
                            "avg_utilization_pct": round(sum(utils) / len(utils), 2),
                            "max_utilization_pct": round(max(utils), 2),
                            "min_utilization_pct": round(min(utils), 2)
                        }
                        all_utils.extend(utils)

                if all_utils:
                    data["aggregate"] = {
                        "total_cores": len(all_utils),
                        "avg_utilization_pct": round(sum(all_utils) / len(all_utils), 2),
                        "max_utilization_pct": round(max(all_utils), 2),
                        "min_utilization_pct": round(min(all_utils), 2)
                    }

            # Add metadata
            data["metadata"] = {
                "scope": self.scope,
                "mode": self.mode,
                "organized_by": "taskmanager" if self.tm_pinning_map else "node"
            }

            # Add warnings if any nodes failed
            if errors:
                data["warnings"] = errors

            return ScraperResult.ok(
                tick=ctx.tick_idx,
                source=self.name,
                data=data
            )

        except Exception as e:
            logging.error(f"[CpuUtilScraper] Unexpected error: {e}", exc_info=True)
            return ScraperResult.error(
                tick=ctx.tick_idx,
                source=self.name,
                error=str(e),
                error_type="CpuUtilScraperError"
            )

    def get_cluster_avg_utilization(self) -> Optional[float]:
        """
        Convenience method to get current average CPU utilization across cluster.

        Returns:
            Average CPU utilization percentage, or None if collection failed
        """
        try:
            all_utils = []

            for node_ip, client in self.clients.items():
                result = client.get_cpu_utilization_by_cpu(mode=self.mode)
                if "error" not in result:
                    cpu_utils = result.get("cpu_utilization", {})
                    utils = [u for u in cpu_utils.values() if isinstance(u, (int, float))]
                    all_utils.extend(utils)

            if all_utils:
                return sum(all_utils) / len(all_utils)
            return None
        except Exception as e:
            logging.error(f"[CpuUtilScraper] Failed to get cluster avg utilization: {e}")
            return None
