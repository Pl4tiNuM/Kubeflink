"""
Topology Resolution: Discover runtime Flink and Kubernetes topology.

Two resolvers:
1. FlinkTopologyResolver: jobId, vertices/operators via Flink REST
2. KubeTopologyResolver: TM pods, pod→node mapping via K8s API
"""

import time
from typing import List, Dict, Any, Optional
import requests
from kubernetes import client, config as k8s_config


class FlinkTopologyResolver:
    """
    Resolve Flink job topology via REST API.

    Discovers:
    - jobId
    - vertices/operators (id, name, parallelism)
    """

    def __init__(self, flink_rest_url: str, timeout: int = 10):
        """
        Args:
            flink_rest_url: Flink REST endpoint (e.g., http://localhost:8081)
            timeout: Request timeout in seconds
        """
        self.base_url = flink_rest_url.rstrip("/")
        self.timeout = timeout

    def resolve_job_id(self, session: requests.Session, job_name: Optional[str] = None) -> Optional[str]:
        """
        Get running job ID.

        Args:
            session: requests session
            job_name: Optional job name filter

        Returns:
            jobId string or None
        """
        try:
            resp = session.get(f"{self.base_url}/jobs", timeout=self.timeout)
            resp.raise_for_status()

            jobs = resp.json().get("jobs", [])

            # Filter for RUNNING jobs
            running_jobs = [j for j in jobs if j.get("status") == "RUNNING"]

            if not running_jobs:
                return None

            # If job_name provided, filter by name
            if job_name:
                for job in running_jobs:
                    # Fetch job details to get name
                    job_id = job["id"]
                    detail_resp = session.get(f"{self.base_url}/jobs/{job_id}", timeout=self.timeout)
                    if detail_resp.ok:
                        detail = detail_resp.json()
                        if detail.get("name") == job_name:
                            return job_id

                return None  # No match

            # Return first running job
            return running_jobs[0]["id"]

        except Exception as e:
            print(f"Error resolving job ID: {e}")
            return None

    def resolve_vertices(self, session: requests.Session, job_id: str) -> List[Dict[str, Any]]:
        """
        Get list of vertices/operators for a job.

        Returns:
            List of dicts with keys: id, name, parallelism
        """
        try:
            resp = session.get(f"{self.base_url}/jobs/{job_id}", timeout=self.timeout)
            resp.raise_for_status()

            job_details = resp.json()
            vertices_raw = job_details.get("vertices", [])

            vertices = []
            for v in vertices_raw:
                vertices.append({
                    "id": v["id"],
                    "name": v.get("name", "unknown"),
                    "parallelism": v.get("parallelism", 1)
                })

            return vertices

        except Exception as e:
            print(f"Error resolving vertices: {e}")
            return []

    def is_rest_reachable(self, session: requests.Session) -> bool:
        """Check if Flink REST is reachable"""
        try:
            resp = session.get(f"{self.base_url}/overview", timeout=self.timeout)
            return resp.ok
        except:
            return False

    def is_job_running(self, session: requests.Session, job_id: str) -> bool:
        """Check if job is in RUNNING state"""
        try:
            resp = session.get(f"{self.base_url}/jobs/{job_id}", timeout=self.timeout)
            if not resp.ok:
                return False

            job = resp.json()
            return job.get("state") == "RUNNING"
        except:
            return False


class KubeTopologyResolver:
    """
    Resolve Kubernetes pod topology.

    Discovers:
    - TaskManager pods
    - Pod → node mapping
    - Pod IP addresses
    """

    def __init__(self, namespace: str = "default"):
        """
        Args:
            namespace: Kubernetes namespace
        """
        self.namespace = namespace
        self._v1 = None

    def _get_client(self):
        """Lazy-load K8s client"""
        if self._v1 is None:
            try:
                k8s_config.load_kube_config()
            except:
                k8s_config.load_incluster_config()

            self._v1 = client.CoreV1Api()

        return self._v1

    def resolve_tm_pods(self, label_selector: str = None) -> List[Dict[str, Any]]:
        """
        Get TaskManager pods.

        Args:
            label_selector: K8s label selector (e.g., "app=flink,component=taskmanager")

        Returns:
            List of dicts with keys: name, node, ip, phase, ready
        """
        try:
            v1 = self._get_client()

            pods = v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=label_selector
            )

            tm_pods = []
            for pod in pods.items:
                # Check if it's a TaskManager (by name convention or labels)
                pod_name = pod.metadata.name
                if "taskmanager" not in pod_name.lower():
                    continue

                # Check readiness
                ready = False
                if pod.status.conditions:
                    for condition in pod.status.conditions:
                        if condition.type == "Ready" and condition.status == "True":
                            ready = True
                            break

                tm_pods.append({
                    "name": pod_name,
                    "node": pod.spec.node_name,
                    "ip": pod.status.pod_ip,
                    "phase": pod.status.phase,
                    "ready": ready,
                    "labels": pod.metadata.labels or {}
                })

            return tm_pods

        except Exception as e:
            print(f"Error resolving TM pods: {e}")
            return []

    def wait_for_pods_ready(
        self,
        label_selector: str,
        expected_count: int,
        timeout_seconds: int = 120,
        stable_seconds: int = 5
    ) -> bool:
        """
        Wait until expected number of TM pods are Ready and stable.

        Args:
            label_selector: K8s label selector
            expected_count: Expected number of TM pods
            timeout_seconds: Max wait time
            stable_seconds: How long count must remain stable

        Returns:
            True if condition met, False if timeout
        """
        start_time = time.time()
        stable_start = None
        last_count = 0

        while time.time() - start_time < timeout_seconds:
            pods = self.resolve_tm_pods(label_selector)
            ready_count = sum(1 for p in pods if p["ready"])

            if ready_count == expected_count:
                # Start stability timer
                if stable_start is None:
                    stable_start = time.time()
                    last_count = ready_count
                elif ready_count == last_count:
                    # Still stable
                    if time.time() - stable_start >= stable_seconds:
                        return True
                else:
                    # Count changed - reset
                    stable_start = time.time()
                    last_count = ready_count
            else:
                # Not at expected count - reset stability
                stable_start = None
                last_count = ready_count

            time.sleep(2)

        return False


class TopologyResolver:
    """
    Combined topology resolver (Flink + K8s).

    Populates RunContext with:
    - job_id
    - vertices
    - tm_pods
    """

    def __init__(self, ctx):
        self.ctx = ctx
        self.flink_resolver = FlinkTopologyResolver(ctx.config.flink_rest_url)
        self.kube_resolver = KubeTopologyResolver(ctx.config.namespace)

    def resolve(self, job_name: Optional[str] = None, label_selector: Optional[str] = None):
        """
        Resolve full topology and populate ctx.

        Args:
            job_name: Optional Flink job name to filter
            label_selector: K8s label selector for TM pods
        """
        # Resolve Flink topology
        self.ctx.job_id = self.flink_resolver.resolve_job_id(self.ctx.session, job_name)

        if self.ctx.job_id:
            self.ctx.vertices = self.flink_resolver.resolve_vertices(self.ctx.session, self.ctx.job_id)

        # Resolve K8s topology
        self.ctx.tm_pods = self.kube_resolver.resolve_tm_pods(label_selector)

    def wait_for_ready(
        self,
        expected_tm_count: int,
        label_selector: str,
        timeout_seconds: int = 120
    ) -> bool:
        """
        Wait for cluster and job to be ready.

        Checks:
        - Flink REST reachable
        - Job is running
        - Expected TM pods are Ready

        Returns:
            True if ready, False if timeout
        """
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            # Skip Flink REST checks if no URL configured
            if self.flink_resolver.base_url and self.flink_resolver.base_url.strip():
                # Check Flink REST
                if not self.flink_resolver.is_rest_reachable(self.ctx.session):
                    print("Waiting for Flink REST API...")
                    time.sleep(2)
                    continue

                # Try to resolve job_id if not set
                if not self.ctx.job_id:
                    self.ctx.job_id = self.flink_resolver.resolve_job_id(self.ctx.session)
                    if not self.ctx.job_id:
                        print("Waiting for job to start...")
                        time.sleep(2)
                        continue

                # Check job is still running
                if not self.flink_resolver.is_job_running(self.ctx.session, self.ctx.job_id):
                    print("Waiting for job to be in RUNNING state...")
                    time.sleep(2)
                    continue
            else:
                print("Skipping Flink REST checks (no URL configured)")

            # Check TM pods
            pods = self.kube_resolver.resolve_tm_pods(label_selector)
            ready_count = sum(1 for p in pods if p["ready"])

            print(f"TM pods ready: {ready_count}/{expected_tm_count}")

            if ready_count >= expected_tm_count:
                # Wait for stability
                if not self.kube_resolver.wait_for_pods_ready(
                    label_selector,
                    expected_tm_count,
                    timeout_seconds=int(timeout_seconds - (time.time() - start_time)),
                    stable_seconds=5
                ):
                    return False

                # All checks passed
                return True

            time.sleep(2)

        print(f"Timeout after {timeout_seconds}s waiting for cluster ready")
        return False
