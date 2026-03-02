"""
Workload Driver - Manages Flink job submission and workload generation.

Integrates with existing query scripts:
- experiments/query1/run-query1.sh
- experiments/query5mod/run-query5mod.sh
"""

import subprocess
import os
import time
from typing import Optional, Dict, List
from pathlib import Path
from dataclasses import dataclass


@dataclass
class QueryConfig:
    """Configuration for a Flink query."""
    name: str
    script_path: Path
    jar_path: Path
    cluster_id: str
    default_args: Dict[str, str]


# Query configurations
QUERY1_CONFIG = QueryConfig(
    name="query1",
    script_path=Path("/home/achilleas/boston/Kubeflink/experiments/query1/run-query1.sh"),
    jar_path=Path("/home/achilleas/boston/Kubeflink/experiments/target/Query1.jar"),
    cluster_id="flink-query1",
    default_args={
        "ratelist": "50000_60000_100000_60000_200000_60000_300000_60000",
        "exchange_rate": "0.82"
    }
)

QUERY5MOD_CONFIG = QueryConfig(
    name="query5mod",
    script_path=Path("/home/achilleas/boston/Kubeflink/experiments/query5mod/run-query5mod.sh"),
    jar_path=Path("/home/achilleas/boston/Kubeflink/experiments/target/Query5mod.jar"),
    cluster_id="flink-query5mod",
    default_args={
        "ratelist": "5000_7200000",
        "topsize": "5",
        "swl_min": "60",
        "sws_min": "1",
        "wtm_ms": "1000",
        "extsize": "1000"
    }
)

QUERIES = {
    "query1": QUERY1_CONFIG,
    "query5mod": QUERY5MOD_CONFIG
}


class WorkloadDriver:
    """Driver for submitting and managing Flink workloads."""

    def __init__(self, query_name: str):
        """
        Args:
            query_name: Name of query to run ("query1" or "query5mod")

        Raises:
            ValueError: If query_name not recognized
        """
        if query_name not in QUERIES:
            raise ValueError(f"Unknown query: {query_name}. Valid: {list(QUERIES.keys())}")

        self.config = QUERIES[query_name]
        self.process: Optional[subprocess.Popen] = None

    def build_ratelist(self, ramp_steps: List[tuple]) -> str:
        """
        Build ratelist string from ramp steps.

        Args:
            ramp_steps: List of (rate, duration_sec) tuples

        Returns:
            Ratelist string (e.g., "100_60000_200_60000_300_60000")
            Note: Duration is converted from seconds to milliseconds

        Example:
            ramp_steps = [(100, 60), (200, 60), (300, 60)]
            -> "100_60000_200_60000_300_60000"
        """
        parts = []
        for rate, duration_sec in ramp_steps:
            duration_ms = int(duration_sec * 1000)  # Convert seconds to milliseconds
            parts.extend([str(int(rate)), str(duration_ms)])

        return "_".join(parts)

    def submit_job(
        self,
        ratelist: Optional[str] = None,
        extra_args: Optional[Dict[str, str]] = None,
        capture_output: bool = True
    ) -> Dict:
        """
        Submit Flink job using the appropriate query script.

        Args:
            ratelist: Custom ratelist string (uses default if None)
            extra_args: Additional query-specific arguments
            capture_output: If True, capture stdout/stderr

        Returns:
            Dict with job submission result:
                - ok: bool
                - cluster_id: str
                - command: str
                - returncode: int (if completed)
                - stdout: str (if capture_output=True)
                - stderr: str (if capture_output=True)

        Raises:
            FileNotFoundError: If JAR file doesn't exist
        """
        # Check JAR exists
        if not self.config.jar_path.exists():
            raise FileNotFoundError(
                f"JAR not found: {self.config.jar_path}\n"
                f"Build with: cd experiments && mvn clean package -P{self.config.name} -DskipTests"
            )

        # Build environment variables for script
        env = os.environ.copy()

        # Set ratelist
        if ratelist:
            env["RATELIST"] = ratelist

        # Set extra args
        if extra_args:
            for key, value in extra_args.items():
                env[key.upper()] = str(value)

        # Run the script
        cmd = [str(self.config.script_path)]

        print(f"[WorkloadDriver] Submitting job: {self.config.name}")
        print(f"[WorkloadDriver] Cluster ID: {self.config.cluster_id}")
        print(f"[WorkloadDriver] Command: {' '.join(cmd)}")

        if ratelist:
            print(f"[WorkloadDriver] Ratelist: {ratelist}")

        try:
            if capture_output:
                result = subprocess.run(
                    cmd,
                    env=env,
                    cwd=self.config.script_path.parent,
                    capture_output=True,
                    text=True,
                    timeout=120  # 2 minute timeout for job submission
                )

                return {
                    "ok": result.returncode == 0,
                    "cluster_id": self.config.cluster_id,
                    "command": " ".join(cmd),
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            else:
                # Run without capturing output (interactive mode)
                result = subprocess.run(
                    cmd,
                    env=env,
                    cwd=self.config.script_path.parent,
                    timeout=120
                )

                return {
                    "ok": result.returncode == 0,
                    "cluster_id": self.config.cluster_id,
                    "command": " ".join(cmd),
                    "returncode": result.returncode
                }

        except subprocess.TimeoutExpired as e:
            return {
                "ok": False,
                "cluster_id": self.config.cluster_id,
                "command": " ".join(cmd),
                "error": "Job submission timed out after 120s",
                "exception": str(e)
            }

        except Exception as e:
            return {
                "ok": False,
                "cluster_id": self.config.cluster_id,
                "command": " ".join(cmd),
                "error": str(e),
                "exception": str(type(e).__name__)
            }

    def get_cluster_id(self) -> str:
        """Get the cluster ID for this query."""
        return self.config.cluster_id

    def get_rest_service_name(self) -> str:
        """Get the Kubernetes service name for Flink REST API."""
        return f"{self.config.cluster_id}-rest"

    def cleanup_cluster(self, namespace: str = "default") -> Dict:
        """
        Clean up Flink cluster resources.

        Args:
            namespace: Kubernetes namespace

        Returns:
            Dict with cleanup result
        """
        cmd = [
            "kubectl", "delete",
            "deployment,service,configmap",
            "-l", f"app={self.config.cluster_id}",
            "-n", namespace
        ]

        print(f"[WorkloadDriver] Cleaning up cluster: {self.config.cluster_id}")
        print(f"[WorkloadDriver] Command: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )

            return {
                "ok": result.returncode == 0,
                "command": " ".join(cmd),
                "stdout": result.stdout,
                "stderr": result.stderr
            }

        except Exception as e:
            return {
                "ok": False,
                "command": " ".join(cmd),
                "error": str(e)
            }


def get_available_queries() -> List[str]:
    """Get list of available query names."""
    return list(QUERIES.keys())


def validate_query_setup(query_name: str) -> Dict[str, bool]:
    """
    Validate that query is ready to run.

    Returns:
        Dict with validation results:
            - script_exists: bool
            - jar_exists: bool
            - script_executable: bool
    """
    if query_name not in QUERIES:
        return {"error": f"Unknown query: {query_name}"}

    config = QUERIES[query_name]

    return {
        "script_exists": config.script_path.exists(),
        "jar_exists": config.jar_path.exists(),
        "script_executable": os.access(config.script_path, os.X_OK)
    }
