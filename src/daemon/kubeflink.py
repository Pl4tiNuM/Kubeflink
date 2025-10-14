import time, logging
from kube_collector import KubeCollector
from flink_collector import FlinkCollector

from structs import Service

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s]: %(message)s')


class KubeFlink:

    def __init__(self):
        self.kc = KubeCollector()
        self.fc = FlinkCollector()

    def filter_flink_svc(self, svc_list: list[Service]) -> list[str]:
        """Filter services to find those related to Flink REST."""

        flink_svcs: list[Service] = []
        for service in svc_list:
            if 'flink' in service.name.lower() and 'rest' in service.name.lower():
                flink_svcs.append(service)
        return flink_svcs
    
    def run(self):
        while True:
            running_apps = self.fc.update_state()
            logging.info(f"[KubeFlink] Currently running Flink applications: {list(running_apps.keys())}")
            time.sleep(5)