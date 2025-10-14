from dataclasses import dataclass, field
from typing import Dict

@dataclass
class Pod:
    name: str
    node_name: str

@dataclass
class Service:
    name: str
    namespace: str

@dataclass
class TaskMetrics:
    backpressure: int = 0

@dataclass
class FlinkTask:
    id: str
    status: str = "UNKNOWN"
    taskmanager: str = ""
    metrics: TaskMetrics = field(default_factory=TaskMetrics)

@dataclass
class FlinkVertex:
    id: str
    name: str
    status: str = "UNKNOWN"
    parallelism: int = 0
    tasks: Dict[str, FlinkTask] = field(default_factory=dict)

@dataclass
class FlinkApp:
    name: str
    namespace: str
    url: str = ""
    job_id: str = ""
    status: str = ""
    vertices: Dict[str, FlinkVertex] = field(default_factory=dict)