"""
Flink REST Scraper - Collects metrics from Flink REST API

Self-contained implementation that directly queries Flink REST API.
No dependencies on src/kubeflink - uses only requests library.

Key robustness improvements vs the original:
- Uses a single requests.Session (keep-alive)
- Retries transient failures (429/5xx) with backoff
- Splits timeout into (connect_timeout, read_timeout)
- Caches metric-name discovery (does NOT re-list available metrics every tick)
- Selects a RUNNING job when possible (instead of jobs[0])
- Optional "batch subtask metrics" endpoint usage when supported by your Flink version:
    /jobs/{job}/vertices/{vertex}/subtasks/metrics?get=...
  with fallback to per-subtask fetching.
- Adds slow-endpoint logging to pinpoint bottlenecks.
"""

import logging
import time
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..scraper import Scraper, ScraperResult
from ..run_context import RunContext


class FlinkRestScraper(Scraper):
    """
    Scrapes Flink metrics via REST API.

    This scraper is self-contained and only requires the Flink REST URL.
    It discovers the job topology and collects metrics on each tick.

    Extensibility:
    - Customize task_metric_names to collect specific task-level metrics
    - Customize vertex_metric_bases to collect specific vertex aggregations
    - Set discover_all_metrics=True to collect ALL available metrics
    """

    def __init__(
        self,
        flink_rest_url: str,
        collect_task_metrics: bool = True,
        collect_vertex_metrics: bool = True,
        collect_tm_metrics: bool = True,
        task_metric_names: Optional[List[str]] = None,
        vertex_metric_bases: Optional[List[str]] = None,
        tm_metric_names: Optional[List[str]] = None,
        discover_all_metrics: bool = False,
        # NOTE: changed semantics: connect/read timeouts (seconds)
        connect_timeout: int = 2,
        read_timeout: int = 20,
        # cache refresh settings
        metric_id_cache_ttl_sec: int = 300,
        # slow endpoint logging
        slow_endpoint_sec: float = 2.0,
        # try batch endpoint for subtasks metrics if available
        try_batch_subtask_metrics: bool = True,
    ):
        """
        Args:
            flink_rest_url: Flink REST API URL (e.g., "http://flink-query1-rest:8081")
            collect_task_metrics: If True, collect per-task metrics
            collect_vertex_metrics: If True, collect per-vertex aggregated metrics
            collect_tm_metrics: If True, collect per-TaskManager metrics (JVM, GC, network)
            task_metric_names: List of task metric names to collect.
                If None, uses default list.
                If empty list [], collects all available metrics.
            vertex_metric_bases: List of vertex metric base names (without .min/.max/.avg/.sum).
                If None, uses default list.
                If empty list [], collects all available metrics.
            tm_metric_names: List of TaskManager metric names to collect.
                If None, uses default list.
                If empty list [], collects all available metrics.
            discover_all_metrics: If True, discovers and collects ALL available metrics
                (overrides task_metric_names/vertex_metric_bases/tm_metric_names)
            connect_timeout: Connection timeout in seconds
            read_timeout: Read timeout in seconds
            metric_id_cache_ttl_sec: TTL for discovered metric-id lists
            slow_endpoint_sec: Log warnings when an endpoint call exceeds this duration
            try_batch_subtask_metrics: Try Flink's batch subtask metrics endpoint when supported

        Example - Default metrics:
            scraper = FlinkRestScraper('http://flink:8081')

        Example - Custom task metrics:
            scraper = FlinkRestScraper(
                'http://flink:8081',
                task_metric_names=['numRecordsIn', 'numRecordsOut', 'backPressuredTimeMsPerSecond']
            )

        Example - Discover all:
            scraper = FlinkRestScraper('http://flink:8081', discover_all_metrics=True)
        """
        self.base_url = flink_rest_url.rstrip("/")
        self.collect_task_metrics = collect_task_metrics
        self.collect_vertex_metrics = collect_vertex_metrics
        self.collect_tm_metrics = collect_tm_metrics
        self.discover_all_metrics = discover_all_metrics

        self.timeout: Tuple[int, int] = (connect_timeout, read_timeout)
        self.metric_id_cache_ttl_sec = metric_id_cache_ttl_sec
        self.slow_endpoint_sec = slow_endpoint_sec
        self.try_batch_subtask_metrics = try_batch_subtask_metrics

        # Requests session + retries
        self.session = requests.Session()
        retry = Retry(
            total=3,
            connect=2,
            read=2,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Default task metrics (per-subtask level)
        self.task_metric_names = task_metric_names if task_metric_names is not None else [
            # Throughput and backpressure
            "backPressuredTimeMsPerSecond",
            "idleTimeMsPerSecond",
            "busyTimeMsPerSecond",
            "numRecordsIn",
            "numRecordsOut",
            "numBytesIn",
            "numBytesOut",
            "numRecordsInPerSecond",
            "numRecordsOutPerSecond",
            # Network buffer metrics (task-specific)
            "buffers.inPoolUsage",
            "buffers.outPoolUsage",
            "numBuffersInLocal",
            "numBuffersInRemote",
            "inputQueueLength",
            "outputQueueLength",
            "buffers.inputQueueLength",
            "buffers.outputQueueLength",
            "buffers.inPoolLength",
            "buffers.outPoolLength",
            # Network metrics
            "numBytesInLocal",
            "numBytesInRemote",
            "numBytesOutLocal",
            "numBytesOutRemote",
            # Flink-managed memory (operator state, RocksDB)
            "managedMemoryUsed",
            "numBytesInFlight",
            # Records/buffers dropped
            "numRecordsDropped",
            "numBuffersDropped",
            # Latency tracking
            "latency",
            # Watermarks
            "currentInputWatermark",
            "currentOutputWatermark",
            # Checkpoint metrics
            "checkpointAlignmentTime",
            "checkpointStartDelayNanos",
            # Operator-specific metrics
            "numLateRecordsDropped",
            "numSplitsProcessed",
        ]

        # TaskManager-level metrics (JVM, GC, memory, network)
        self.tm_metric_names = tm_metric_names if tm_metric_names is not None else [
            # JVM Memory
            "Status.JVM.Memory.Heap.Used",
            "Status.JVM.Memory.Heap.Max",
            "Status.JVM.Memory.Heap.Committed",
            "Status.JVM.Memory.NonHeap.Used",
            "Status.JVM.Memory.NonHeap.Max",
            "Status.JVM.Memory.NonHeap.Committed",
            "Status.JVM.Memory.Metaspace.Used",
            "Status.JVM.Memory.Metaspace.Max",
            "Status.JVM.Memory.Direct.Count",
            "Status.JVM.Memory.Direct.MemoryUsed",
            "Status.JVM.Memory.Direct.TotalCapacity",
            "Status.JVM.Memory.Mapped.Count",
            "Status.JVM.Memory.Mapped.MemoryUsed",
            "Status.JVM.Memory.Mapped.TotalCapacity",
            # GC metrics (common names; may differ by JVM/collector)
            "Status.JVM.GarbageCollector.G1_Young_Generation.Count",
            "Status.JVM.GarbageCollector.G1_Young_Generation.Time",
            "Status.JVM.GarbageCollector.G1_Old_Generation.Count",
            "Status.JVM.GarbageCollector.G1_Old_Generation.Time",
            # Network buffers
            "Status.Network.TotalMemorySegments",
            "Status.Network.AvailableMemorySegments",
            # Shuffle metrics
            "Status.Shuffle.Netty.TotalMemory",
            "Status.Shuffle.Netty.UsedMemory",
            "Status.Shuffle.Netty.AvailableMemory",
            # CPU
            "Status.JVM.CPU.Load",
            "Status.JVM.CPU.Time",
            # Threads
            "Status.JVM.Threads.Count",
        ]

        # Default vertex metric bases (will add .min/.max/.avg/.sum)
        self.vertex_metric_bases = vertex_metric_bases if vertex_metric_bases is not None else [
            "backPressuredTimeMsPerSecond",
            "idleTimeMsPerSecond",
            "busyTimeMsPerSecond",
            "numRecordsInPerSecond",
            "numRecordsOutPerSecond",
            "buffers.inPoolUsage",
            "buffers.outPoolUsage",
            "numBuffersInLocal",
            "numBuffersInRemote",
            "inputQueueLength",
            "outputQueueLength",
            "numBytesInLocal",
            "numBytesInRemote",
            "managedMemoryUsed",
            "numBytesInFlight",
            "currentInputWatermark",
            "currentOutputWatermark",
        ]

        # Cached topology
        self.job_id: Optional[str] = None
        self.vertices: Dict[str, Dict] = {}  # vertex_id -> {id, name, status, parallelism}

        # Cache: discovered metric IDs (avoid "list available metrics" every tick)
        now = time.time()
        self._subtask_metric_ids: Dict[Tuple[str, int], Tuple[float, List[str]]] = {}  # (vertex_id, subtask_id) -> (ts, ids)
        self._vertex_metric_ids: Dict[str, Tuple[float, List[str]]] = {}              # vertex_id -> (ts, ids)
        self._tm_metric_ids: Dict[str, Tuple[float, List[str]]] = {}                  # tm_id -> (ts, ids)
        self._last_cache_prune = now

    @property
    def name(self) -> str:
        return "flink_rest"

    # ------------------------
    # Public entry
    # ------------------------
    def scrape(self, ctx: RunContext) -> ScraperResult:
        """Scrape Flink metrics for current tick."""
        try:
            if not self.job_id:
                success = self._discover_job()
                if not success:
                    return ScraperResult.error(
                        tick=ctx.tick_idx,
                        source=self.name,
                        error="No running job found",
                    )

            self._maybe_prune_caches()

            data = {
                "job_id": self.job_id,
                "vertices": {},
                "taskmanagers": {},
            }

            if self.collect_tm_metrics:
                data["taskmanagers"] = self._collect_taskmanager_metrics()

            for vertex_id, vertex in self.vertices.items():
                vertex_data = {
                    "name": vertex["name"],
                    "parallelism": vertex["parallelism"],
                    "status": vertex["status"],
                }

                if self.collect_task_metrics:
                    vertex_data["tasks"] = self._collect_task_metrics(vertex_id)

                if self.collect_vertex_metrics:
                    vertex_data["aggregated"] = self._collect_vertex_metrics(vertex_id)

                data["vertices"][vertex_id] = vertex_data

            return ScraperResult.ok(
                tick=ctx.tick_idx,
                source=self.name,
                data=data,
            )

        except requests.HTTPError as e:
            # If job disappeared (JM restart, job finished), reset and try next tick
            status = getattr(e.response, "status_code", None)
            if status in (404, 410):
                logging.warning(f"[FlinkRestScraper] HTTP {status}; resetting cached job/topology")
                self._reset_topology_and_caches()
            logging.error(f"[FlinkRestScraper] HTTP error: {e}", exc_info=True)
            return ScraperResult.error(
                tick=ctx.tick_idx,
                source=self.name,
                error=f"HTTPError: {e}",
            )
        except requests.Timeout as e:
            logging.error(f"[FlinkRestScraper] Timeout: {e}", exc_info=True)
            return ScraperResult.error(
                tick=ctx.tick_idx,
                source=self.name,
                error=f"Timeout: {e}",
            )
        except Exception as e:
            logging.error(f"[FlinkRestScraper] Error: {e}", exc_info=True)
            return ScraperResult.error(
                tick=ctx.tick_idx,
                source=self.name,
                error=str(e),
            )

    # ------------------------
    # Internal helpers
    # ------------------------
    def _reset_topology_and_caches(self) -> None:
        self.job_id = None
        self.vertices = {}
        self._subtask_metric_ids.clear()
        self._vertex_metric_ids.clear()
        self._tm_metric_ids.clear()

    def _maybe_prune_caches(self) -> None:
        now = time.time()
        if now - self._last_cache_prune < 60:
            return
        ttl = self.metric_id_cache_ttl_sec

        def _prune_dict(d):
            dead = [k for k, (ts, _) in d.items() if now - ts > ttl]
            for k in dead:
                d.pop(k, None)

        _prune_dict(self._subtask_metric_ids)
        _prune_dict(self._vertex_metric_ids)
        _prune_dict(self._tm_metric_ids)

        self._last_cache_prune = now

    def _get_json(self, url: str, *, params: Optional[Dict] = None) -> Dict:
        """GET JSON with timing + slow-endpoint logging."""
        t0 = time.time()
        resp = self.session.get(url, params=params, timeout=self.timeout)
        dt = time.time() - t0

        if dt >= self.slow_endpoint_sec:
            logging.warning(f"[FlinkRestScraper] Slow endpoint {url} took {dt:.2f}s params={params}")

        resp.raise_for_status()
        return resp.json()

    def _discover_job(self) -> bool:
        """
        Discover running job and topology.
        Returns True if job found, False otherwise.
        """
        try:
            overview = self._get_json(f"{self.base_url}/jobs/overview")
            jobs = overview.get("jobs", [])

            if not jobs:
                logging.warning("[FlinkRestScraper] No jobs found")
                return False

            # Prefer RUNNING job if present (Flink usually uses 'state': 'RUNNING')
            running = [j for j in jobs if j.get("state") == "RUNNING"]
            chosen = running[0] if running else jobs[0]

            self.job_id = chosen["jid"]
            logging.info(f"[FlinkRestScraper] Discovered job: {self.job_id} (state={chosen.get('state')})")

            job_detail = self._get_json(f"{self.base_url}/jobs/{self.job_id}")

            self.vertices = {}
            for vertex in job_detail.get("vertices", []):
                self.vertices[vertex["id"]] = {
                    "id": vertex["id"],
                    "name": vertex.get("name", "unknown"),
                    "status": vertex.get("status", "UNKNOWN"),
                    "parallelism": vertex.get("parallelism", 1),
                }

            logging.info(f"[FlinkRestScraper] Discovered {len(self.vertices)} vertices")
            return True

        except Exception as e:
            logging.error(f"[FlinkRestScraper] Job discovery failed: {e}", exc_info=True)
            self._reset_topology_and_caches()
            return False

    # ------------------------
    # Task metrics
    # ------------------------
    def _collect_task_metrics(self, vertex_id: str) -> Dict:
        """
        Collect per-task metrics for a vertex.
        Returns dict mapping subtask_id to metrics dict.
        """
        try:
            vertex_detail = self._get_json(f"{self.base_url}/jobs/{self.job_id}/vertices/{vertex_id}")
            subtasks = vertex_detail.get("subtasks", [])

            # Prefer batch endpoint when possible to reduce REST calls drastically
            batch_metrics_by_subtask: Dict[int, Dict[str, str]] = {}
            metrics_to_fetch = self._compute_task_metrics_to_fetch(vertex_id, subtasks)

            if self.try_batch_subtask_metrics and metrics_to_fetch:
                batch_metrics_by_subtask = self._try_get_batch_subtask_metrics(vertex_id, metrics_to_fetch)

            task_metrics = {}
            for task in subtasks:
                subtask_id = int(task.get("subtask", 0))

                if batch_metrics_by_subtask:
                    metrics = batch_metrics_by_subtask.get(subtask_id, {})
                else:
                    metrics = self._get_subtask_metrics(vertex_id, subtask_id)

                task_metrics[str(subtask_id)] = {
                    "status": task.get("status", "UNKNOWN"),
                    "taskmanager_id": task.get("taskmanager-id", "UNKNOWN"),
                    "host": task.get("host", "unknown"),
                    "metrics": metrics,
                }

            return task_metrics

        except Exception as e:
            logging.warning(f"[FlinkRestScraper] Failed to collect task metrics for {vertex_id}: {e}", exc_info=True)
            return {}

    def _compute_task_metrics_to_fetch(self, vertex_id: str, subtasks: List[Dict]) -> List[str]:
        """
        Determine which task-level metric ids to fetch (once), using cached discovery
        from subtask 0 (or first subtask) as the "available metric universe".
        """
        if not subtasks:
            return []

        # Use first subtask to discover availability
        first_subtask_id = int(subtasks[0].get("subtask", 0))
        avail = self._get_cached_subtask_metric_ids(vertex_id, first_subtask_id)
        if not avail:
            return []

        if self.discover_all_metrics:
            return avail

        # If task_metric_names is [] (empty), collect all
        if self.task_metric_names == []:
            return avail

        # If task_metric_names is None, we already set defaults; if user passed an empty list, handled above
        configured = self.task_metric_names or []
        return [m for m in configured if m in avail]

    def _get_cached_subtask_metric_ids(self, vertex_id: str, subtask_id: int) -> List[str]:
        key = (vertex_id, subtask_id)
        now = time.time()
        cached = self._subtask_metric_ids.get(key)
        if cached and (now - cached[0] <= self.metric_id_cache_ttl_sec):
            return cached[1]

        # Discover available metric IDs
        url = f"{self.base_url}/jobs/{self.job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics"
        metric_list = self._get_json(url)
        ids = [m["id"] for m in metric_list]
        self._subtask_metric_ids[key] = (now, ids)
        return ids

    def _get_subtask_metrics(self, vertex_id: str, subtask_id: int) -> Dict:
        """Get metrics for a specific subtask (no re-discovery each tick)."""
        try:
            available = self._get_cached_subtask_metric_ids(vertex_id, subtask_id)

            if self.discover_all_metrics:
                metrics_to_fetch = available
            elif self.task_metric_names == []:
                metrics_to_fetch = available
            else:
                configured = self.task_metric_names or []
                metrics_to_fetch = [m for m in configured if m in available]

            if not metrics_to_fetch:
                return {}

            url = f"{self.base_url}/jobs/{self.job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics"
            payload = self._get_json(url, params={"get": ",".join(metrics_to_fetch)})

            metrics: Dict[str, str] = {}
            for metric in payload:
                metrics[metric["id"]] = metric.get("value")
            return metrics

        except Exception as e:
            logging.warning(
                f"[FlinkRestScraper] Failed to get subtask metrics for {vertex_id}/{subtask_id}: {e}",
                exc_info=True,
            )
            return {}

    def _try_get_batch_subtask_metrics(self, vertex_id: str, metrics_to_fetch: List[str]) -> Dict[int, Dict[str, str]]:
        """
        Try batch endpoint:
          /jobs/{job}/vertices/{vertex}/subtasks/metrics?get=...
        Response shape varies by Flink version; we try to parse common forms.
        If unsupported or parsing fails, return {} and caller falls back to per-subtask fetch.
        """
        try:
            url = f"{self.base_url}/jobs/{self.job_id}/vertices/{vertex_id}/subtasks/metrics"
            payload = self._get_json(url, params={"get": ",".join(metrics_to_fetch)})

            # Common patterns seen across versions:
            # 1) {"subtasks":[{"subtask":0,"metrics":[{"id":"x","value":"y"}, ...]}, ...]}
            # 2) [{"subtask":0,"metrics":[{"id":"x","value":"y"}, ...]}, ...]   (less common)
            # 3) [{"id":"x","values":[{"subtask":0,"value":"y"}, ...]}, ...]   (metric-major)
            out: Dict[int, Dict[str, str]] = {}

            if isinstance(payload, dict) and "subtasks" in payload and isinstance(payload["subtasks"], list):
                for st in payload["subtasks"]:
                    sid = int(st.get("subtask", 0))
                    out.setdefault(sid, {})
                    for m in st.get("metrics", []):
                        out[sid][m["id"]] = m.get("value")
                return out

            if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                # subtask-major list
                if "subtask" in payload[0] and "metrics" in payload[0]:
                    for st in payload:
                        sid = int(st.get("subtask", 0))
                        out.setdefault(sid, {})
                        for m in st.get("metrics", []):
                            out[sid][m["id"]] = m.get("value")
                    return out

                # metric-major list
                if "id" in payload[0] and "values" in payload[0]:
                    for metric in payload:
                        mid = metric.get("id")
                        for v in metric.get("values", []):
                            sid = int(v.get("subtask", 0))
                            out.setdefault(sid, {})
                            out[sid][mid] = v.get("value")
                    return out

            # If format unknown, just fail to fallback
            logging.debug(f"[FlinkRestScraper] Unrecognized batch metrics response format for vertex {vertex_id}")
            return {}

        except Exception as e:
            # Most likely 404/400 if endpoint not supported
            logging.debug(f"[FlinkRestScraper] Batch subtask metrics unsupported/failed for {vertex_id}: {e}")
            return {}

    # ------------------------
    # Vertex (aggregated) metrics
    # ------------------------
    def _get_cached_vertex_metric_ids(self, vertex_id: str) -> List[str]:
        now = time.time()
        cached = self._vertex_metric_ids.get(vertex_id)
        if cached and (now - cached[0] <= self.metric_id_cache_ttl_sec):
            return cached[1]

        url = f"{self.base_url}/jobs/{self.job_id}/vertices/{vertex_id}/metrics"
        metric_list = self._get_json(url)
        ids = [m["id"] for m in metric_list]
        self._vertex_metric_ids[vertex_id] = (now, ids)
        return ids

    def _collect_vertex_metrics(self, vertex_id: str) -> Dict:
        """
        Collect aggregated metrics for a vertex.
        Returns dict with aggregated statistics (min/max/avg/sum).
        """
        try:
            metric_names = self._get_cached_vertex_metric_ids(vertex_id)

            if self.discover_all_metrics:
                metrics_to_fetch = metric_names
            elif self.vertex_metric_bases == []:
                metrics_to_fetch = metric_names
            else:
                metrics_to_fetch: List[str] = []
                for base in (self.vertex_metric_bases or []):
                    if base in metric_names:
                        metrics_to_fetch.append(base)
                    for agg in [".min", ".max", ".avg", ".sum"]:
                        full = base + agg
                        if full in metric_names:
                            metrics_to_fetch.append(full)

                if not metrics_to_fetch and metric_names:
                    logging.debug(
                        f"[FlinkRestScraper] No vertex metrics matched for {vertex_id}. "
                        f"Available sample: {metric_names[:10]}..."
                    )

            if not metrics_to_fetch:
                # Attempt a few common patterns if configured list doesn't match actual names
                common_patterns = [
                    "numRecordsInPerSecond",
                    "numRecordsOutPerSecond",
                    "backPressuredTimeMsPerSecond",
                    "busyTimeMsPerSecond",
                    "idleTimeMsPerSecond",
                ]
                for pattern in common_patterns:
                    metrics_to_fetch.extend([m for m in metric_names if pattern in m])
                metrics_to_fetch = list(dict.fromkeys(metrics_to_fetch))  # dedup

                if not metrics_to_fetch:
                    return {}

            url = f"{self.base_url}/jobs/{self.job_id}/vertices/{vertex_id}/metrics"
            payload = self._get_json(url, params={"get": ",".join(metrics_to_fetch)})

            metrics: Dict[str, str] = {}
            for metric in payload:
                metrics[metric["id"]] = metric.get("value")
            return metrics

        except Exception as e:
            logging.warning(f"[FlinkRestScraper] Failed to collect vertex metrics for {vertex_id}: {e}", exc_info=True)
            return {}

    # ------------------------
    # TaskManager metrics
    # ------------------------
    def _get_cached_tm_metric_ids(self, tm_id: str) -> List[str]:
        now = time.time()
        cached = self._tm_metric_ids.get(tm_id)
        if cached and (now - cached[0] <= self.metric_id_cache_ttl_sec):
            return cached[1]

        url = f"{self.base_url}/taskmanagers/{tm_id}/metrics"
        metric_list = self._get_json(url)
        ids = [m["id"] for m in metric_list]
        self._tm_metric_ids[tm_id] = (now, ids)
        return ids

    def _collect_taskmanager_metrics(self) -> Dict:
        """
        Collect metrics from all TaskManagers.
        Returns dict mapping tm_id to metrics dict (JVM, GC, memory, network).
        """
        try:
            tms_payload = self._get_json(f"{self.base_url}/taskmanagers")
            taskmanagers = tms_payload.get("taskmanagers", [])

            tm_metrics: Dict[str, Dict] = {}

            for tm in taskmanagers:
                tm_id = tm["id"]
                metric_names = self._get_cached_tm_metric_ids(tm_id)

                if self.discover_all_metrics:
                    metrics_to_fetch = metric_names
                elif self.tm_metric_names == []:
                    metrics_to_fetch = metric_names
                else:
                    configured = self.tm_metric_names or []
                    metrics_to_fetch = [m for m in configured if m in metric_names]

                if not metrics_to_fetch:
                    tm_metrics[tm_id] = {
                        "path": tm.get("path", "unknown"),
                        "dataPort": tm.get("dataPort", 0),
                        "timeSinceLastHeartbeat": tm.get("timeSinceLastHeartbeat", 0),
                        "slotsNumber": tm.get("slotsNumber", 0),
                        "freeSlots": tm.get("freeSlots", 0),
                        "metrics": {},
                    }
                    continue

                url = f"{self.base_url}/taskmanagers/{tm_id}/metrics"
                payload = self._get_json(url, params={"get": ",".join(metrics_to_fetch)})

                metrics: Dict[str, str] = {}
                for metric in payload:
                    metrics[metric["id"]] = metric.get("value")

                tm_metrics[tm_id] = {
                    "path": tm.get("path", "unknown"),
                    "dataPort": tm.get("dataPort", 0),
                    "timeSinceLastHeartbeat": tm.get("timeSinceLastHeartbeat", 0),
                    "slotsNumber": tm.get("slotsNumber", 0),
                    "freeSlots": tm.get("freeSlots", 0),
                    "metrics": metrics,
                }

            return tm_metrics

        except Exception as e:
            logging.warning(f"[FlinkRestScraper] Failed to collect TaskManager metrics: {e}", exc_info=True)
            return {}
