"""
Port Forward Helper - Automatically manage kubectl port-forward for Flink REST API

Handles:
- Starting port-forward as subprocess
- Waiting for port to be accessible
- Cleanup on teardown
"""

import subprocess
import time
import requests
import logging
from typing import Optional


class PortForwardManager:
    """Manages kubectl port-forward subprocess for Flink REST API"""

    def __init__(self, namespace: str = "default", local_port: int = 8081):
        """
        Args:
            namespace: Kubernetes namespace
            local_port: Local port to forward to (default: 8081)
        """
        self.namespace = namespace
        self.local_port = local_port
        self.process: Optional[subprocess.Popen] = None
        self.service_name: Optional[str] = None

    def start(self, service_name: str, timeout_seconds: int = 30, remote_port: int = 8081) -> bool:
        """
        Start port-forward for a Kubernetes service.

        Args:
            service_name: Kubernetes service name (e.g., "flink-query1-rest", "prometheus")
            timeout_seconds: Max time to wait for port to be accessible
            remote_port: Remote service port to forward from (default: 8081 for Flink)

        Returns:
            True if port-forward started and accessible, False otherwise
        """
        self.service_name = service_name

        # Check if already accessible (maybe manual port-forward running)
        if self._is_port_accessible():
            logging.info(f"Port {self.local_port} already accessible, skipping port-forward")
            return True

        # Wait for service to exist
        logging.info(f"Waiting for service {service_name} to be created...")
        if not self._wait_for_service(service_name, timeout_seconds=30):
            logging.error(f"Service {service_name} not found after 30s")
            return False

        # Start port-forward
        cmd = [
            "kubectl", "port-forward",
            f"service/{service_name}",
            f"{self.local_port}:{remote_port}",
            "-n", self.namespace
        ]

        logging.info(f"Starting port-forward: {' '.join(cmd)}")

        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
        except Exception as e:
            logging.error(f"Failed to start port-forward: {e}")
            return False

        # Wait for port to become accessible
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            if self._is_port_accessible():
                logging.info(f"✓ Port-forward ready: localhost:{self.local_port} -> {service_name}:{remote_port}")
                return True

            # Check if process died
            if self.process.poll() is not None:
                stderr = self.process.stderr.read() if self.process.stderr else ""
                logging.error(f"Port-forward process died: {stderr}")
                return False

            time.sleep(1)

        logging.error(f"Timeout waiting for port {self.local_port} to become accessible")
        self.stop()
        return False

    def _wait_for_service(self, service_name: str, timeout_seconds: int = 30) -> bool:
        """Wait for K8s service to exist"""
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            cmd = ["kubectl", "get", "service", service_name, "-n", self.namespace]
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0:
                return True

            time.sleep(2)

        return False

    def _is_port_accessible(self) -> bool:
        """Check if local port is accessible"""
        try:
            resp = requests.get(
                f"http://localhost:{self.local_port}/config",
                timeout=2
            )
            return resp.status_code == 200
        except:
            return False

    def stop(self):
        """Stop port-forward process"""
        if self.process and self.process.poll() is None:
            logging.info("Stopping port-forward...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logging.warning("Port-forward didn't terminate, killing...")
                self.process.kill()
            self.process = None

    def __del__(self):
        """Cleanup on destruction"""
        self.stop()
