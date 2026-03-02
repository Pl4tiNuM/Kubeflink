"""
DVFS Client - Interfaces with the dvfs-controller agent (src/dvfs-controller/agent.py)

The agent runs as a DaemonSet on each worker node at port 4002.
It provides REST API for:
- Setting CPU frequencies per core
- Reading current frequencies
- Reading node power (Intel RAPL)
"""

import requests
from typing import Dict, List, Optional
import time


class DvfsClient:
    """Client for DVFS controller agent."""

    def __init__(self, timeout: int = 5):
        """
        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout

    def _node_url(self, node_ip: str, port: int = 4002) -> str:
        """Build URL for agent on specific node."""
        return f"http://{node_ip}:{port}"

    def set_frequency(
        self,
        node_ip: str,
        cores: List[str],
        freq_khz: int,
        reset: bool = False
    ) -> Dict:
        """
        Set CPU frequency for specified cores on a node.

        Args:
            node_ip: IP address of the worker node
            cores: List of core IDs (e.g., ["0", "1", "2"])
            freq_khz: Target frequency in kHz
            reset: If True, always set frequency. If False, only increase frequency.

        Returns:
            Dict with status and applied settings

        Raises:
            requests.RequestException: If request fails
        """
        url = f"{self._node_url(node_ip)}/api/set_frequency"

        payload = {
            "cores": cores,
            "freq": freq_khz,
            "reset": "1" if reset else "0"
        }

        start = time.time()
        resp = requests.post(url, json=payload, timeout=self.timeout)
        elapsed_ms = int((time.time() - start) * 1000)

        resp.raise_for_status()
        result = resp.json()
        result["request_duration_ms"] = elapsed_ms

        return result

    def get_frequencies(self, node_ip: str) -> Dict[str, str]:
        """
        Get current frequencies for all cores on a node.

        Args:
            node_ip: IP address of the worker node

        Returns:
            Dict mapping core names to frequencies (e.g., {"cpu0": "2400000", ...})

        Raises:
            requests.RequestException: If request fails
        """
        url = f"{self._node_url(node_ip)}/api/get_frequencies"

        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()

        return resp.json()

    def get_frequencies_for_cores(self, node_ip: str, cores: str) -> Dict:
        """
        Get frequencies for specific cores on a node.

        Args:
            node_ip: IP address of the worker node
            cores: Core specification (e.g., "0-2,12-14")

        Returns:
            Dict with frequency readings for specified cores

        Raises:
            requests.RequestException: If request fails
        """
        url = f"{self._node_url(node_ip)}/api/get_frequencies_for_cores"

        params = {"cores": cores}

        start = time.time()
        resp = requests.get(url, params=params, timeout=self.timeout)
        elapsed_ms = int((time.time() - start) * 1000)

        resp.raise_for_status()
        raw_result = resp.json()
        
        # Transform agent response format to expected format
        # Agent returns: {"status": "ok", "requested": [0,1,2], "freq_khz": {"cpu0": 1200000, ...}}
        # We want: {"status": "ok", "cores": {"0": 1200000, ...}, "unit": "kHz"}
        
        cores_data = {}
        if raw_result.get("status") == "ok":
            freq_khz = raw_result.get("freq_khz", {})
            for cpu_key, freq_val in freq_khz.items():
                # Extract core number from "cpu0" -> "0"
                core_num = cpu_key.replace("cpu", "")
                if freq_val != "N/A" and freq_val is not None:
                    cores_data[core_num] = freq_val
        
        result = {
            "status": raw_result.get("status", "error"),
            "cores": cores_data,
            "unit": "kHz",
            "request_duration_ms": elapsed_ms
        }
        
        if raw_result.get("error"):
            result["error"] = raw_result["error"]
        
        return result

    def set_frequency_multi_node(
        self,
        node_configs: Dict[str, Dict[str, any]]
    ) -> Dict[str, Dict]:
        """
        Set frequencies on multiple nodes.

        Args:
            node_configs: Dict mapping node_ip to config dict with keys:
                - cores: List[str]
                - freq_khz: int
                - reset: bool (optional, default False)

        Returns:
            Dict mapping node_ip to result dict (or error dict)

        Example:
            node_configs = {
                "10.0.0.1": {"cores": ["0", "1"], "freq_khz": 2400000},
                "10.0.0.2": {"cores": ["2", "3"], "freq_khz": 1800000, "reset": True}
            }
        """
        results = {}

        for node_ip, config in node_configs.items():
            try:
                result = self.set_frequency(
                    node_ip=node_ip,
                    cores=config["cores"],
                    freq_khz=config["freq_khz"],
                    reset=config.get("reset", False)
                )
                results[node_ip] = {"ok": True, "result": result}
            except Exception as e:
                results[node_ip] = {"ok": False, "error": str(e)}

        return results

    def get_power(self, node_ip: str) -> Dict:
        """
        Get current power consumption for a node.

        Args:
            node_ip: IP address of the worker node

        Returns:
            Dict with power reading:
                - power: float (Watts)
                - request_duration_ms: int

        Raises:
            requests.RequestException: If request fails

        Note:
            - For on-demand sampling: returns fresh reading
            - For continuous monitoring: returns latest cached value
        """
        url = f"{self._node_url(node_ip)}/api/get_power"

        start = time.time()
        resp = requests.get(url, timeout=self.timeout)
        elapsed_ms = int((time.time() - start) * 1000)

        resp.raise_for_status()
        result = resp.json()
        result["request_duration_ms"] = elapsed_ms

        return result

    def get_power_multi_node(self, node_ips: List[str]) -> Dict[str, Dict]:
        """
        Get power readings from multiple nodes.

        Args:
            node_ips: List of node IP addresses

        Returns:
            Dict mapping node_ip to result dict (or error dict)

        Example:
            node_ips = ["10.0.0.1", "10.0.0.2"]
            results = client.get_power_multi_node(node_ips)
            # {"10.0.0.1": {"ok": True, "power": 72.4}, ...}
        """
        results = {}

        for node_ip in node_ips:
            try:
                result = self.get_power(node_ip)
                results[node_ip] = {"ok": True, **result}
            except Exception as e:
                results[node_ip] = {"ok": False, "error": str(e)}

        return results
