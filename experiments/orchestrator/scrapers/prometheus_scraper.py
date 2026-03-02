"""
Prometheus Scraper - Collects power metrics from Scaphandre via Prometheus

Queries Prometheus for:
- scaph_host_power_microwatts: Total host power consumption
- scaph_process_power_consumption_microwatts: Per-process power consumption

These metrics are collected from Scaphandre exporters running on physical nodes.
"""

import requests
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from ..scraper import Scraper, ScraperResult
from ..run_context import RunContext


class PrometheusScraper(Scraper):
    """Scraper for Prometheus metrics (Scaphandre power metrics)"""

    def __init__(
        self,
        prometheus_url: str = "http://localhost:9090",
        lookback_seconds: Optional[int] = None
    ):
        """
        Args:
            prometheus_url: Prometheus server URL
            lookback_seconds: How far back to query (if None, uses tick interval)
        """
        self.prometheus_url = prometheus_url.rstrip("/")
        self.lookback_seconds = lookback_seconds

    @property
    def name(self) -> str:
        return "prometheus"

    def _query_prometheus(self, query: str, time_seconds: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute instant query against Prometheus.

        Args:
            query: PromQL query string
            time_seconds: Optional Unix timestamp for query (defaults to now)

        Returns:
            Parsed JSON response

        Raises:
            requests.RequestException: If query fails
        """
        url = f"{self.prometheus_url}/api/v1/query"
        params = {"query": query}

        if time_seconds:
            params["time"] = time_seconds

        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _query_prometheus_range(
        self,
        query: str,
        start: int,
        end: int,
        step: str = "15s"
    ) -> Dict[str, Any]:
        """
        Execute range query against Prometheus.

        Args:
            query: PromQL query string
            start: Start time (Unix timestamp)
            end: End time (Unix timestamp)
            step: Query resolution (e.g., "15s", "1m")

        Returns:
            Parsed JSON response

        Raises:
            requests.RequestException: If query fails
        """
        url = f"{self.prometheus_url}/api/v1/query_range"
        params = {
            "query": query,
            "start": start,
            "end": end,
            "step": step
        }

        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def scrape(self, ctx: RunContext) -> ScraperResult:
        """
        Scrape Prometheus metrics at current tick.

        Args:
            ctx: Run context with configuration and tick info

        Returns:
            ScraperResult with metrics and metadata
        """
        start_time = time.time()

        # Determine query time window
        now = int(time.time())
        lookback = self.lookback_seconds if self.lookback_seconds else ctx.config.tick_seconds

        # Use range query to get metrics over the last interval
        start = now - lookback
        end = now

        try:
            # Query host power (total system power)
            host_power_result = self._query_prometheus_range(
                query="scaph_host_power_microwatts",
                start=start,
                end=end,
                step="5s"
            )

            # Query process power (per-process breakdown)
            process_power_result = self._query_prometheus_range(
                query="scaph_process_power_consumption_microwatts",
                start=start,
                end=end,
                step="5s"
            )

            # Parse results
            host_power_data = self._parse_result(host_power_result)
            process_power_data = self._parse_result(process_power_result)

            # Calculate statistics
            host_stats = self._calculate_stats(host_power_data)
            process_stats = self._calculate_process_stats(process_power_data)

            elapsed_ms = int((time.time() - start_time) * 1000)

            data = {
                "query_window": {
                    "start": start,
                    "end": end,
                    "duration_s": lookback
                },
                "host_power": host_stats,
                "process_power": process_stats
            }

            return ScraperResult.ok(
                tick=ctx.tick_idx,
                source=self.name,
                data=data,
                duration_ms=elapsed_ms
            )

        except Exception as e:
            elapsed_ms = int((time.time() - start_time) * 1000)
            return ScraperResult.error(
                tick=ctx.tick_idx,
                source=self.name,
                error=str(e),
                error_type=type(e).__name__,
                duration_ms=elapsed_ms
            )

    def _parse_result(self, prom_response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse Prometheus API response.

        Args:
            prom_response: JSON response from Prometheus API

        Returns:
            List of parsed time series with labels and values
        """
        if prom_response.get("status") != "success":
            return []

        result = prom_response.get("data", {}).get("result", [])

        parsed = []
        for series in result:
            metric_labels = series.get("metric", {})
            values = series.get("values", [])

            # Each value is [timestamp, value_str]
            timeseries = [
                {"timestamp": int(ts), "value": float(val)}
                for ts, val in values
            ]

            parsed.append({
                "labels": metric_labels,
                "values": timeseries
            })

        return parsed

    def _calculate_stats(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate statistics for host power metrics.

        Args:
            data: Parsed time series data

        Returns:
            Dict with aggregated statistics
        """
        if not data:
            return {"available": False}

        # Aggregate all values across all time series
        all_values = []
        by_instance = {}

        for series in data:
            labels = series["labels"]
            instance = labels.get("instance", "unknown")
            node = labels.get("node", "unknown")
            values = [v["value"] for v in series["values"]]

            all_values.extend(values)

            if values:
                by_instance[node] = {
                    "instance": instance,
                    "count": len(values),
                    "avg_microwatts": sum(values) / len(values),
                    "min_microwatts": min(values),
                    "max_microwatts": max(values),
                    "last_microwatts": values[-1]
                }

        if not all_values:
            return {"available": False}

        return {
            "available": True,
            "sample_count": len(all_values),
            "avg_microwatts": sum(all_values) / len(all_values),
            "min_microwatts": min(all_values),
            "max_microwatts": max(all_values),
            "by_instance": by_instance
        }

    def _calculate_process_stats(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate statistics for per-process power metrics.

        Args:
            data: Parsed time series data

        Returns:
            Dict with per-process aggregated statistics
        """
        if not data:
            return {"available": False}

        by_node = {}

        for series in data:
            labels = series["labels"]
            exe = labels.get("exe", "unknown")
            pid = labels.get("pid", "unknown")
            cmdline = labels.get("cmdline", "")
            instance = labels.get("instance", "unknown")
            node = labels.get("node", "unknown")

            if node not in by_node:
                by_node[node] = {}

            process_key = f"{exe}_{pid}"
            values = [v["value"] for v in series["values"]]

            if values:
                by_node[node][process_key] = {
                    "instance": instance,
                    "exe": exe,
                    "pid": pid,
                    "cmdline": cmdline[:100],  # Truncate long cmdlines
                    "sample_count": len(values),
                    "avg_microwatts": sum(values) / len(values),
                    "min_microwatts": min(values),
                    "max_microwatts": max(values),
                    "last_microwatts": values[-1]
                }

        # Sort processes by node, then by average power (descending) and take top processes per node
        top_by_node = {}
        total_processes = 0

        for node, processes in by_node.items():
            sorted_processes = sorted(
                processes.items(),
                key=lambda x: x[1]["avg_microwatts"],
                reverse=True
            )
            top_by_node[node] = {
                "total_processes": len(processes),
                "top_processes": dict(sorted_processes[:20])  # Top 20 per node
            }
            total_processes += len(processes)

        return {
            "available": True,
            "total_processes": total_processes,
            "by_node": top_by_node
        }
