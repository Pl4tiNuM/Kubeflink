"""
Frequency Scraper - Collects CPU frequency metrics for specific cores from cluster nodes

Uses DvfsClient to query CPU frequencies from specified cores on nodes
running the DVFS agent.
"""

import logging
from typing import List, Dict, Optional

from ..scraper import Scraper, ScraperResult
from ..run_context import RunContext
from ..dvfs_client import DvfsClient


class FrequencyScraper(Scraper):
    """
    Scrapes CPU frequencies for specific cores from cluster nodes via DVFS agent.

    Collects per-core frequency readings from specified nodes and cores.

    Agent response format:
    {
      "status": "ok",
      "cores": {
        "0": 2400000,
        "1": 2400000,
        "12": 1800000
      },
      "unit": "kHz"
    }
    """

    def __init__(self, node_configs: Dict[str, str], port: int = 4002, timeout: int = 5):
        """
        Args:
            node_configs: Dict mapping node IP to core specification
                         Example: {"192.168.1.228": "0-3,8-11", "192.168.1.229": "0-7"}
            port: DVFS agent port (default: 4002)
            timeout: Request timeout in seconds

        Example:
            # Monitor cores 0-2 and 12-14 on two nodes
            scraper = FrequencyScraper(node_configs={
                "192.168.1.228": "0-2,12-14",
                "192.168.1.229": "0-7"
            })
        """
        self.node_configs = node_configs
        self.port = port
        self.timeout = timeout

        # Create DvfsClient instances for each node
        self.clients = {
            ip: DvfsClient(timeout=timeout)
            for ip in node_configs.keys()
        }

        logging.info(f"[FrequencyScraper] Initialized for {len(node_configs)} nodes")

    @property
    def name(self) -> str:
        return "frequency"

    def scrape(self, ctx: RunContext) -> ScraperResult:
        """Scrape CPU frequencies from configured nodes and cores."""
        try:
            data = {
                "nodes": {},
                "successful_nodes": 0,
                "failed_nodes": 0
            }

            errors = []

            for node_ip, cores_spec in self.node_configs.items():
                client = self.clients[node_ip]
                
                try:
                    result = client.get_frequencies_for_cores(
                        node_ip=node_ip,
                        cores=cores_spec
                    )

                    if result.get("status") == "ok":
                        cores_data = result.get("cores", {})
                        
                        # Calculate statistics
                        if cores_data:
                            frequencies = [freq for freq in cores_data.values()]
                            avg_freq_khz = sum(frequencies) / len(frequencies)
                            max_freq_khz = max(frequencies)
                            min_freq_khz = min(frequencies)
                        else:
                            avg_freq_khz = 0
                            max_freq_khz = 0
                            min_freq_khz = 0

                        data["nodes"][node_ip] = {
                            "cores_spec": cores_spec,
                            "cores": cores_data,
                            "unit": result.get("unit", "kHz"),
                            "avg_freq_khz": avg_freq_khz,
                            "max_freq_khz": max_freq_khz,
                            "min_freq_khz": min_freq_khz,
                            "num_cores": len(cores_data),
                            "request_duration_ms": result.get("request_duration_ms", 0)
                        }
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
                    logging.warning(f"[FrequencyScraper] Failed to get frequencies from {node_ip}: {e}")

            # If all nodes failed, return error result
            if data["failed_nodes"] == len(self.node_configs):
                return ScraperResult.error(
                    tick=ctx.tick_idx,
                    source=self.name,
                    error=f"All nodes failed: {'; '.join(errors)}",
                    error_type="FrequencyCollectionError"
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
            logging.error(f"[FrequencyScraper] Unexpected error: {e}", exc_info=True)
            return ScraperResult.error(
                tick=ctx.tick_idx,
                source=self.name,
                error=str(e),
                error_type="FrequencyScraperError"
            )

    def get_frequencies(self, node_ip: str) -> Optional[Dict]:
        """
        Convenience method to get current frequencies for a specific node.

        Args:
            node_ip: Node IP address

        Returns:
            Dict with core frequencies, or None if collection failed
        """
        if node_ip not in self.clients:
            logging.error(f"[FrequencyScraper] Node {node_ip} not configured")
            return None

        try:
            cores_spec = self.node_configs[node_ip]
            client = self.clients[node_ip]
            result = client.get_frequencies_for_cores(node_ip=node_ip, cores=cores_spec)
            
            if result.get("status") == "ok":
                return result.get("cores", {})
            return None
        except Exception as e:
            logging.error(f"[FrequencyScraper] Failed to get frequencies from {node_ip}: {e}")
            return None
