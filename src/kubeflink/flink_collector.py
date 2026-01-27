import requests, logging

from kube_collector import KubeCollector
from typing import Dict

from structs import FlinkApp, FlinkTask, FlinkVertex

class FlinkCollector:
    def __init__(self, kc: KubeCollector = None):
        self.kc = kc
        self.running_apps: Dict[str, FlinkApp] = {}
        self.completed_apps: Dict[str, FlinkApp] = {}

    def update_state(self) -> list[FlinkApp]:
        """Gathers Flink applications from all detected Flink REST services."""

        logging.info("[FlinkCollector] Updating state of Flink applications...")

        svc_list = self.kc.update_svc_list()
        flink_svcs = [svc for svc in svc_list if 'flink' in svc.name.lower() and 'rest' in svc.name.lower()]

        for service in flink_svcs:
            # Get name of Flink cluster id == app's name
            name = service.name.split('-')[0:-1]
            name = '-'.join(name)
            
            if name not in self.running_apps:
                logging.info(f"[FlinkCollector] Detected new Flink service: {service.name} in namespace {service.namespace}")
                self.running_apps[name] = FlinkApp(name=name, 
                                                   namespace=service.namespace,
                                                   url=f"http://{service.name}.{service.namespace}.svc:8081")
            
            logging.info(f"[FlinkCollector] Updating Flink service: {service.name} in namespace {service.namespace}")
            jobs = self.get_jobs(self.running_apps[name].url)
            if jobs is not None:
                logging.info(f"[FlinkCollector] Found {len(jobs)} running jobs in Flink service: {service.name}")
                job_id = jobs[0]['jid']
                self.running_apps[name].job_id = job_id
                self.running_apps[name].vertices = self.get_vertices(self.running_apps[name].url, self.running_apps[name].job_id)
            else:
                logging.info(f"[FlinkCollector] No running jobs found in Flink service: {service.name}. Marking application as completed.")

        return self.running_apps


    def get_jobs(self, svc_url):
        """Fetches the list of running jobs."""
        try:
            resp = requests.get(f"{svc_url}/jobs/overview")
            resp.raise_for_status()
            jobs = resp.json().get('jobs', [])
        except requests.RequestException as e:
            logging.warning(f"[FlinkCollector] Could not fetch jobs from service {svc_url}: {e}")
            return None
        else:
            logging.warning(f"[FlinkCollector] Successfully fetched jobs from service {svc_url}")
            return jobs

    def get_vertices(self, svc_url, job_id) -> Dict[str, FlinkVertex]:
        """
        Fetches the list of vertices for a given job.
        Returns a list of dicts with vertex info.
        """
        try:
            resp = requests.get(f"{svc_url}/jobs/{job_id}")
            resp.raise_for_status()
        except requests.RequestException as e:
            logging.warning(f"[FlinkCollector] Could not fetch vertices for job {job_id} from service {svc_url}: {e}")
            return None
        else:
            tmp_dict = {}
            for vertex in resp.json().get('vertices', []):
                tmp_dict[vertex['id']] = FlinkVertex(
                                            id=vertex['id'],
                                            name=vertex.get('name'),
                                            status=vertex.get('status', 'UNKNOWN'),
                                            parallelism=vertex.get('parallelism', 1),
                                        )
                tmp_dict[vertex['id']].tasks = self.get_tasks(svc_url, job_id, vertex['id'])
                logging.info(f"[FlinkCollector] Fetched vertex {vertex.get('name')} with {len(tmp_dict[vertex['id']].tasks)} tasks.")
                logging.info(f"[FlinkCollector] Vertex details: {tmp_dict[vertex['id']]}")
            return tmp_dict

            
    def get_tasks(self, svc_url, job_id, vertex_id) -> Dict[str, FlinkTask]:
        """
        Fetches the list of tasks for a given job and their assigned task managers.
        Returns a list of dicts with task info and assigned task manager.
        """
        try:
            resp = requests.get(f"{svc_url}/jobs/{job_id}/vertices/{vertex_id}")
            resp.raise_for_status()
            subtasks = resp.json().get('subtasks', [])
        except requests.RequestException as e:
            logging.warning(f"[FlinkCollector] Could not fetch information for vertex id {vertex_id} from service {svc_url}: {e}")
            return None
        else:
            tasks = {}
            for task in subtasks:
                task_id = task.get('subtask', 'UNKNOWN')
                tasks[task_id] = FlinkTask(
                    id=task_id,
                    status=task.get('status', 'UNKNOWN'),
                    timestamp_start=task.get('start-time', 0),
                    timestamp_end=task.get('end-time', 0),
                    taskmanager_id=task.get('taskmanager-id', 'UNKNOWN'),
                    # metrics=
                )
            logging.info(f"[FlinkCollector] Fetched {len(tasks)} tasks for vertex id {vertex_id}.")
            logging.info(f"[FlinkCollector] Tasks details: {tasks}")
        return tasks

    def get_backpressure(self, job_id):
        """Fetches backpressure metrics for a given job."""
        resp = requests.get(f"{self.base_url}/jobs/{job_id}/backpressure")
        resp.raise_for_status()
        return resp.json()

    def collect_metrics(self):
        """Collects metrics for all running jobs."""
        metrics = []
        jobs = self.get_jobs()
        for job in jobs:
            job_id = job['jid']
            backpressure = self.get_backpressure(job_id)
            metrics.append({
                'job_id': job_id,
                'name': job.get('name'),
                'backpressure': backpressure
            })
        return metrics
