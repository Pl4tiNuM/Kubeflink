import time
import logging
import threading

from kube_collector import KubeCollector
from flink_collector import FlinkCollector

from structs import Service

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s]: %(message)s')


class KubeFlink:

    def __init__(self):
        self.kc = KubeCollector()
        self.fc = FlinkCollector(self.kc)

    def filter_flink_svc(self, svc_list: list[Service]) -> list[Service]:
        """Filter services to find those related to Flink REST."""

        flink_svcs: list[Service] = []
        for service in svc_list:
            # support both dict-like items and objects with .name
            name = service.name if hasattr(service, 'name') else service.get('name', '')
            if 'flink' in name.lower() and 'rest' in name.lower():
                # if original was a dict, wrap into a simple Service-like object for consistency
                if isinstance(service, dict):
                    flink_svcs.append(type('Svc', (), {'name': name})())
                else:
                    flink_svcs.append(service)
        return flink_svcs

    def run(self):
        while True:
            running_apps = self.fc.update_state()
            logging.info(f"[KubeFlink] Currently running Flink applications: {list(running_apps.keys())}")
            time.sleep(5)