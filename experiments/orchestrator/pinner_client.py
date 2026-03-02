"""
Pinner Client - Interfaces with the core pinning agent (src/core-controllers/pin/pinning.py)

The agent runs as a DaemonSet on each worker node at port 4002.
It provides REST API for:
- Pinning containers to specific CPU cores
- Setting cgroup CPU quotas
- Monitoring per-core CPU utilization (host and container scope)
"""

import requests
from typing import List, Optional, Dict
import time


class PinnerClient:
    """Client for core pinning agent."""

    def __init__(self, timeout: int = 10):
        """
        Args:
            timeout: Request timeout in seconds (higher than dvfs due to crictl lookups)
        """
        self.timeout = timeout

    def _node_url(self, node_ip: str, port: int = 4002) -> str:
        """Build URL for agent on specific node."""
        return f"http://{node_ip}:{port}"

    def pin_pod_cores(
        self,
        node_ip: str,
        pod_name: str,
        cores: str,
        container_name: Optional[str] = None
    ) -> Dict:
        """
        Pin a pod's container to specific CPU cores.

        Args:
            node_ip: IP address of the worker node where pod is running
            pod_name: Kubernetes pod name (must be on the specified node)
            cores: Core specification (e.g., "0-3", "0,2,4", "0-3,8-11")
            container_name: Optional container name within pod

        Returns:
            Dict with status message and timing

        Raises:
            requests.RequestException: If request fails
        """
        url = f"{self._node_url(node_ip)}/api/pin_pod_cores"

        payload = {
            "pod_name": pod_name,
            "cores": cores
        }

        if container_name:
            payload["container_name"] = container_name

        start = time.time()
        resp = requests.post(url, json=payload, timeout=self.timeout)
        elapsed_ms = int((time.time() - start) * 1000)

        resp.raise_for_status()
        result = {"message": resp.json(), "request_duration_ms": elapsed_ms}

        return result

    def set_cgroup_quota(
        self,
        node_ip: str,
        pod_name: str,
        quota_pct: Optional[float] = None,
        quota_us: Optional[int] = None,
        period_us: int = 100_000,
        container_name: Optional[str] = None
    ) -> Dict:
        """
        Set CPU CFS quota for a pod's container.

        Args:
            node_ip: IP address of the worker node
            pod_name: Kubernetes pod name
            quota_pct: CPU quota as percentage (e.g., 150.0 for 1.5 cores)
            quota_us: CPU quota in microseconds (alternative to quota_pct)
            period_us: CFS period in microseconds (default 100ms)
            container_name: Optional container name within pod

        Returns:
            Dict with status message and timing

        Raises:
            requests.RequestException: If request fails
            ValueError: If neither quota_pct nor quota_us provided
        """
        if quota_pct is None and quota_us is None:
            raise ValueError("Must provide either quota_pct or quota_us")

        url = f"{self._node_url(node_ip)}/api/set_cgroup_quota"

        payload = {
            "pod_name": pod_name,
            "period_us": period_us
        }

        if quota_pct is not None:
            payload["quota_pct"] = quota_pct
        elif quota_us is not None:
            payload["quota_us"] = quota_us

        if container_name:
            payload["container_name"] = container_name

        start = time.time()
        resp = requests.post(url, json=payload, timeout=self.timeout)
        elapsed_ms = int((time.time() - start) * 1000)

        resp.raise_for_status()
        result = {"message": resp.json(), "request_duration_ms": elapsed_ms}

        return result

    def pin_threads_by_pattern(
        self,
        node_ip: str,
        pod_pattern: str,
        namespace_pattern: str,
        container_pattern: str,
        thread_pattern: str,
        cores: str,
        only_if_cmdline_matches: Optional[str] = None,
        reapply_seconds: Optional[int] = None
    ) -> Dict:
        """
        Pin threads matching a pattern to specific CPU cores.

        Args:
            node_ip: IP address of the worker node
            pod_pattern: Pattern to match pod names
            namespace_pattern: Pattern to match namespace
            container_pattern: Pattern to match container name
            thread_pattern: Regex pattern for thread names (e.g., "^flink-pekko|^flink-scheduler")
            cores: Core specification (e.g., "2-3", "0,2,4")
            only_if_cmdline_matches: Optional filter - only pin if cmdline matches this string
            reapply_seconds: Optional - interval for reapplying pinning (useful for dynamic threads)

        Returns:
            Dict with status message and timing

        Raises:
            requests.RequestException: If request fails
        """
        url = f"{self._node_url(node_ip)}/api/pin_threads_by_pattern"

        payload = {
            "pod_pattern": pod_pattern,
            "namespace_pattern": namespace_pattern,
            "container_pattern": container_pattern,
            "thread_pattern": thread_pattern,
            "cores": cores
        }

        if only_if_cmdline_matches:
            payload["only_if_cmdline_matches"] = only_if_cmdline_matches

        if reapply_seconds is not None:
            payload["reapply_seconds"] = reapply_seconds

        # Debug logging
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"[DEBUG] pin_threads_by_pattern to {node_ip}: {payload}")

        start = time.time()
        resp = requests.post(url, json=payload, timeout=self.timeout)
        elapsed_ms = int((time.time() - start) * 1000)

        resp.raise_for_status()
        result = {"message": resp.json(), "request_duration_ms": elapsed_ms}

        return result

    def pin_pods_batch(
        self,
        pin_configs: List[Dict[str, any]]
    ) -> Dict[str, Dict]:
        """
        Pin multiple pods in batch.

        Args:
            pin_configs: List of config dicts with keys:
                - node_ip: str
                - pod_name: str
                - cores: str
                - container_name: Optional[str]

        Returns:
            Dict mapping pod_name to result dict (or error dict)

        Example:
            pin_configs = [
                {"node_ip": "10.0.0.1", "pod_name": "flink-tm-1", "cores": "0-3"},
                {"node_ip": "10.0.0.2", "pod_name": "flink-tm-2", "cores": "4-7"}
            ]
        """
        results = {}

        for config in pin_configs:
            pod_name = config["pod_name"]
            try:
                result = self.pin_pod_cores(
                    node_ip=config["node_ip"],
                    pod_name=pod_name,
                    cores=config["cores"],
                    container_name=config.get("container_name")
                )
                results[pod_name] = {"ok": True, "result": result}
            except Exception as e:
                results[pod_name] = {"ok": False, "error": str(e)}

        return results

    # ========== CPU Utilization Monitoring ==========

    def start_cpu_monitor(
        self,
        node_ip: str,
        scope: str = "host",
        cpus: str = "all",
        interval_s: float = 1.0,
        window_s: float = 0.5,
        history_size: int = 60,
        pod_name: Optional[str] = None,
        container_name: Optional[str] = None
    ) -> Dict:
        """
        Start continuous CPU utilization monitoring on a node.

        Args:
            node_ip: IP address of the worker node
            scope: "host" or "container"
            cpus: "all" or cpuset string (e.g., "0-15" or "2,3")
            interval_s: How often to produce samples (seconds)
            window_s: How long each sample measures deltas (seconds)
            history_size: Number of past samples to retain
            pod_name: Required if scope="container"
            container_name: Optional container name (for scope="container")

        Returns:
            Dict with status and monitor configuration

        Raises:
            requests.RequestException: If request fails
            ValueError: If scope="container" but pod_name not provided

        Note:
            Must call this before using get_cpu_utilization_by_cpu with mode="last"
        """
        if scope == "container" and not pod_name:
            raise ValueError("pod_name required for scope='container'")

        url = f"{self._node_url(node_ip)}/api/start_cpu_monitor"

        payload = {
            "scope": scope,
            "cpus": cpus,
            "interval_s": interval_s,
            "window_s": window_s,
            "history_size": history_size
        }

        if scope == "container":
            payload["pod_name"] = pod_name
            if container_name:
                payload["container_name"] = container_name

        start = time.time()
        resp = requests.post(url, json=payload, timeout=self.timeout)
        elapsed_ms = int((time.time() - start) * 1000)

        resp.raise_for_status()
        result = resp.json()
        result["request_duration_ms"] = elapsed_ms

        return result

    def stop_cpu_monitor(
        self,
        node_ip: str,
        scope: str = "host",
        pod_name: Optional[str] = None,
        container_name: Optional[str] = None
    ) -> Dict:
        """
        Stop a running CPU monitor on a node.

        Args:
            node_ip: IP address of the worker node
            scope: "host" or "container"
            pod_name: Required if scope="container"
            container_name: Optional container name (for scope="container")

        Returns:
            Dict with status ("stopping" or "not_running")

        Raises:
            requests.RequestException: If request fails
            ValueError: If scope="container" but pod_name not provided
        """
        if scope == "container" and not pod_name:
            raise ValueError("pod_name required for scope='container'")

        url = f"{self._node_url(node_ip)}/api/stop_cpu_monitor"

        payload = {
            "scope": scope
        }

        if scope == "container":
            payload["pod_name"] = pod_name
            if container_name:
                payload["container_name"] = container_name

        start = time.time()
        resp = requests.post(url, json=payload, timeout=self.timeout)
        elapsed_ms = int((time.time() - start) * 1000)

        resp.raise_for_status()
        result = resp.json()
        result["request_duration_ms"] = elapsed_ms

        return result

    def list_cpu_monitors(self, node_ip: str) -> Dict:
        """
        List all active CPU monitors on a node.

        Args:
            node_ip: IP address of the worker node

        Returns:
            Dict with list of monitors and their status

        Raises:
            requests.RequestException: If request fails
        """
        url = f"{self._node_url(node_ip)}/api/list_cpu_monitors"

        start = time.time()
        resp = requests.get(url, timeout=self.timeout)
        elapsed_ms = int((time.time() - start) * 1000)

        resp.raise_for_status()
        result = resp.json()
        result["request_duration_ms"] = elapsed_ms

        return result

    def stop_cpu_monitor_by_key(self, node_ip: str, key: str) -> Dict:
        """
        Stop a CPU monitor by its key.

        Args:
            node_ip: IP address of the worker node
            key: Monitor key (e.g., "host::::")

        Returns:
            Dict with status ("stopping" or "not_running")

        Raises:
            requests.RequestException: If request fails
        """
        url = f"{self._node_url(node_ip)}/api/stop_cpu_monitor"

        payload = {"key": key}

        start = time.time()
        resp = requests.post(url, json=payload, timeout=self.timeout)
        elapsed_ms = int((time.time() - start) * 1000)

        resp.raise_for_status()
        result = resp.json()
        result["request_duration_ms"] = elapsed_ms

        return result

    def get_cpu_utilization_by_cpu(
        self,
        node_ip: str,
        scope: str = "host",
        cpus: str = "all",
        mode: str = "last",
        window_s: Optional[float] = None,
        history_n: int = 0,
        pod_name: Optional[str] = None,
        container_name: Optional[str] = None
    ) -> Dict:
        """
        Get CPU utilization per core.

        Args:
            node_ip: IP address of the worker node
            scope: "host" or "container"
            cpus: Subset of CPUs (e.g., "2,3" or "0-7")
            mode: "last" (cached from monitor) or "sample" (on-demand)
            window_s: Measurement window (only for mode="sample")
            history_n: If >0, include last N samples from history
            pod_name: Required if scope="container"
            container_name: Optional container name (for scope="container")

        Returns:
            Dict with CPU utilization data:
                - per_cpu: Dict[str, float] (e.g., {"cpu2": 45.2, "cpu3": 67.8})
                - timestamp: float (unix timestamp)
                - history: List[Dict] (if history_n > 0)
                - request_duration_ms: int

        Raises:
            requests.RequestException: If request fails
            ValueError: If scope="container" but pod_name not provided

        Note:
            - Values are % of one core (100% = fully busy)
            - For mode="last", must call start_cpu_monitor() first
            - Container per-CPU attribution works best when pod is pinned
        """
        if scope == "container" and not pod_name:
            raise ValueError("pod_name required for scope='container'")

        url = f"{self._node_url(node_ip)}/api/get_cpu_utilization_by_cpu"

        params = {
            "scope": scope,
            "cpus": cpus,
            "mode": mode,
            "history_n": history_n
        }

        if mode == "sample" and window_s is not None:
            params["window_s"] = window_s

        if scope == "container":
            params["pod_name"] = pod_name
            if container_name:
                params["container_name"] = container_name

        start = time.time()
        resp = requests.get(url, params=params, timeout=self.timeout)
        elapsed_ms = int((time.time() - start) * 1000)

        resp.raise_for_status()
        result = resp.json()

        # Agent returns: {"ok": true, "mode": "last", "key": "...", "data": {...}}
        # Extract the data field which contains the actual CPU utilization
        if result.get("ok") and "data" in result:
            cpu_data = result["data"]
            # data contains: {"ok": true, "util_pct_one_core": {"0": 45.2, ...}, ...}
            return {
                "ok": cpu_data.get("ok", True),
                "mode": result.get("mode"),
                "cpu_utilization": cpu_data.get("util_pct_one_core", {}),
                "scope": cpu_data.get("scope"),
                "cpus": cpu_data.get("cpus", []),
                "window_s": cpu_data.get("window_s"),
                "timestamp": cpu_data.get("ts"),
                "request_duration_ms": elapsed_ms
            }
        else:
            return {
                "ok": False,
                "error": result.get("error", "Unknown error"),
                "request_duration_ms": elapsed_ms
            }

    def start_monitors_multi_node(
        self,
        node_configs: Dict[str, Dict[str, any]]
    ) -> Dict[str, Dict]:
        """
        Start CPU monitors on multiple nodes.

        Args:
            node_configs: Dict mapping node_ip to config dict with keys:
                - scope: str (default "host")
                - cpus: str (default "all")
                - interval_s: float (default 1.0)
                - window_s: float (default 0.5)
                - history_size: int (default 60)
                - pod_name: Optional[str] (required if scope="container")
                - container_name: Optional[str]

        Returns:
            Dict mapping node_ip to result dict (or error dict)

        Example:
            node_configs = {
                "10.0.0.1": {"scope": "host", "cpus": "all", "interval_s": 1.0},
                "10.0.0.2": {"scope": "host", "cpus": "all", "interval_s": 1.0}
            }
        """
        results = {}

        for node_ip, config in node_configs.items():
            try:
                result = self.start_cpu_monitor(
                    node_ip=node_ip,
                    scope=config.get("scope", "host"),
                    cpus=config.get("cpus", "all"),
                    interval_s=config.get("interval_s", 1.0),
                    window_s=config.get("window_s", 0.5),
                    history_size=config.get("history_size", 60),
                    pod_name=config.get("pod_name"),
                    container_name=config.get("container_name")
                )
                results[node_ip] = {"ok": True, "result": result}
            except Exception as e:
                results[node_ip] = {"ok": False, "error": str(e)}

        return results
