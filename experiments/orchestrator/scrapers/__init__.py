"""
Scrapers Package

This package contains concrete scraper implementations for the orchestrator.

Available Scrapers:
- FlinkRestScraper: Collects comprehensive Flink metrics via REST API
- PowerScraper: Collects power consumption from cluster nodes via DVFS agent
- CpuUtilScraper: Collects per-core CPU utilization via Pinner agent
- PrometheusScraper: Collects power metrics from Scaphandre via Prometheus
- FrequencyScraper: Collects CPU frequencies for specific cores via DVFS agent
"""

from .flink_rest_scraper import FlinkRestScraper
from .power_scraper import PowerScraper
from .cpu_util_scraper import CpuUtilScraper
from .prometheus_scraper import PrometheusScraper
from .frequency_scraper import FrequencyScraper

__all__ = ["FlinkRestScraper", "PowerScraper", "CpuUtilScraper", "PrometheusScraper", "FrequencyScraper"]

