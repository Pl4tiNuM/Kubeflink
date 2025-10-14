from kubernetes import client, config
from structs import Service, Pod

import logging

class KubeCollector:
    """Utility class for collecting Kubernetes cluster information."""

    def __init__(self):
        """Initialize the Kubernetes API client."""
        self.v1 = self._get_client()
        self.services: list[Service] = []
        self.namespaces = []

    def _get_client(self):
        """Load configuration and return CoreV1Api client."""
        try:
            config.load_incluster_config()
            logging.info("[KubeCollector] Loaded in-cluster configuration.")
        except Exception:
            config.load_kube_config()
            logging.info("[KubeCollector] Loaded local kubeconfig.")
        return client.CoreV1Api()
    
    def _update_namespaces(self):
        """Update the list of namespaces."""
        self.namespaces = [ns.metadata.name for ns in self.v1.list_namespace().items]

    def update_svc_list(self) -> list[Service]:
        """List all Services in all namespaces.
           Returns a list of Service objects with their associated Pods.
        """
        self._update_namespaces()

        logging.info(f"[KubeCollector] Found namespaces: {self.namespaces}")
        self.services = []
        for namespace in self.namespaces:
            logging.info(f"[KubeCollector] Scanning namespace: {namespace}")
            services = self.v1.list_namespaced_service(namespace)
            for svc in services.items:
                self.services.append(
                    Service(
                        name=svc.metadata.name,
                        namespace=namespace,
                    )
                )
                
        return self.services