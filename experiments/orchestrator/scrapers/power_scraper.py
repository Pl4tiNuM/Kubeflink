"""
Power Scraper - Collects power consumption metrics from cluster nodes

Uses DvfsClient to query power consumption (via RAPL) from all nodes
running the DVFS agent.
"""

import logging
from typing import List, Optional

from ..scraper import Scraper, ScraperResult
from ..run_context import RunContext
from ..dvfs_client import DvfsClient


class PowerScraper(Scraper):
    """
    Scrapes power consumption from cluster nodes via DVFS agent.

    Collects per-node power readings using RAPL (Running Average Power Limit)
    interface exposed by the DVFS daemonset agent.

    Agent response format:
    {
      "status": "ok",
      "total_w": 77.5,
      "per_socket_w": {"package-0": 40.4, "package-1": 37.0},
      "dram_w": {"package-0": 10.9, "package-1": 6.5},
      "window_s": 1.0,
      "ts": 1769600739.46
    }
    """

    def __init__(self, node_ips: List[str], port: int = 4002, timeout: int = 5, socket: Optional[str] = None):
        """
        Args:
            node_ips: List of node IP addresses running DVFS agent
            port: DVFS agent port (default: 4002)
            timeout: Request timeout in seconds
            socket: Which socket to read power from ("package-0", "package-1", or None for total)

        Example:
            # Total power across all sockets
            scraper = PowerScraper(node_ips=['192.168.1.10'])

            # Power from specific socket
            scraper = PowerScraper(node_ips=['192.168.1.10'], socket="package-0")
        """
        self.node_ips = node_ips
        self.port = port
        self.timeout = timeout
        self.socket = socket

        # Create DvfsClient instances for each node
        self.clients = {
            ip: DvfsClient(timeout=timeout)
            for ip in node_ips
        }

        logging.info(f"[PowerScraper] Initialized for {len(node_ips)} nodes")

    @property
    def name(self) -> str:
        return "power"

    def scrape(self, ctx: RunContext) -> ScraperResult:
        """Scrape power consumption from all nodes."""
        try:
            data = {
                "nodes": {},
                "total_power_watts": 0.0,
                "successful_nodes": 0,
                "failed_nodes": 0
            }

            errors = []

            for node_ip, client in self.clients.items():
                try:
                    result = client.get_power(node_ip=node_ip)

                    if result.get("status") == "ok":
                        # Select power value based on socket configuration
                        if self.socket:
                            # Get power from specific socket
                            per_socket = result.get("per_socket_w", {})
                            power_watts = per_socket.get(self.socket, 0.0)
                        else:
                            # Get total power
                            power_watts = result.get("total_w", 0.0)

                        data["nodes"][node_ip] = {
                            "power_watts": power_watts,
                            "total_w": result.get("total_w"),
                            "per_socket_w": result.get("per_socket_w"),
                            "dram_w": result.get("dram_w"),
                            "window_s": result.get("window_s"),
                            "timestamp": result.get("ts"),
                            "request_duration_ms": result.get("request_duration_ms", 0)
                        }
                        data["total_power_watts"] += power_watts
                        data["successful_nodes"] += 1
                    else:
                        data["nodes"][node_ip] = {
                            "error": result.get("error", "Unknown error")
                        }
                        data["failed_nodes"] += 1
                        errors.append(f"{node_ip}: {result.get('error')}")

                except Exception as e:
                    data["nodes"][node_ip] = {
                        "error": str(e)
                    }
                    data["failed_nodes"] += 1
                    errors.append(f"{node_ip}: {str(e)}")
                    logging.warning(f"[PowerScraper] Failed to get power from {node_ip}: {e}")

            # If all nodes failed, return error result
            if data["failed_nodes"] == len(self.node_ips):
                return ScraperResult.error(
                    tick=ctx.tick_idx,
                    source=self.name,
                    error=f"All nodes failed: {'; '.join(errors)}",
                    error_type="PowerCollectionError"
                )

            # Partial success - include data with warnings
            if errors:
                data["warnings"] = errors

            return ScraperResult.ok(
                tick=ctx.tick_idx,
                source=self.name,
                data=data
            )

        except Exception as e:
            logging.error(f"[PowerScraper] Unexpected error: {e}", exc_info=True)
            return ScraperResult.error(
                tick=ctx.tick_idx,
                source=self.name,
                error=str(e),
                error_type="PowerScraperError"
            )

    def get_total_power(self) -> Optional[float]:
        """
        Convenience method to get current total power consumption.

        Returns:
            Total power in watts, or None if collection failed
        """
        try:
            results = {}
            for node_ip, client in self.clients.items():
                result = client.get_power(node_ip=node_ip)
                if result.get("status") == "ok":
                    if self.socket:
                        per_socket = result.get("per_socket_w", {})
                        results[node_ip] = per_socket.get(self.socket, 0.0)
                    else:
                        results[node_ip] = result.get("total_w", 0.0)

            if results:
                return sum(results.values())
            return None
        except Exception as e:
            logging.error(f"[PowerScraper] Failed to get total power: {e}")
            return None
