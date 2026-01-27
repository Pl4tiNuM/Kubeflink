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
    timestamp: int = 0
    read_bytes: int = 0
    write_bytes: int = 0
    read_records: int = 0
    write_records: int = 0
    accumulated_backpressured_time: int = 0
    accumulated_idle_time: int = 0
    accumulated_busy_time: int = 0

@dataclass
class FlinkTask:
    id: str = ""
    status: str = "UNKNOWN"
    timestamp_start: int = 0
    timestamp_end: int = 0
    taskmanager_id: str = ""
    metrics: list[TaskMetrics] = field(default_factory=list)

@dataclass
class VertexMetrics:
    timestamp: int = 0
    acc_backpressured_time_p25: int = 0
    acc_backpressured_time_p75: int = 0
    acc_backpressured_time_p95: int = 0
    acc_backpressured_time_min: int = 0
    acc_backpressured_time_avg: int = 0
    acc_backpressured_time_median: int = 0
    acc_backpressured_time_max: int = 0
    acc_idle_time_p25: int = 0
    acc_idle_time_p75: int = 0
    acc_idle_time_p95: int = 0
    acc_idle_time_min: int = 0
    acc_idle_time_avg: int = 0
    acc_idle_time_median: int = 0
    acc_idle_time_max: int = 0
    acc_busy_time_p25: int = 0
    acc_busy_time_p75: int = 0
    acc_busy_time_p95: int = 0
    acc_busy_time_min: int = 0
    acc_busy_time_avg: int = 0
    acc_busy_time_median: int = 0
    acc_busy_time_max: int = 0
    write_bytes_p25: int = 0
    write_bytes_p75: int = 0
    write_bytes_p95: int = 0
    write_bytes_min: int = 0
    write_bytes_avg: int = 0
    write_bytes_median: int = 0
    write_bytes_max: int = 0
    read_bytes_p25: int = 0
    read_bytes_p75: int = 0
    read_bytes_p95: int = 0
    read_bytes_min: int = 0
    read_bytes_avg: int = 0
    read_bytes_median: int = 0
    read_bytes_max: int = 0
    write_records_p25: int = 0
    write_records_p75: int = 0
    write_records_p95: int = 0
    write_records_min: int = 0
    write_records_avg: int = 0
    write_records_median: int = 0
    write_records_max: int = 0
    read_records_p25: int = 0
    read_records_p75: int = 0
    read_records_p95: int = 0
    read_records_min: int = 0
    read_records_avg: int = 0
    read_records_median: int = 0
    read_records_max: int = 0

@dataclass
class FlinkVertex:
    id: str = ""
    name: str = ""
    status: str = "UNKNOWN"
    parallelism: int = 0
    max_parallelism: int = 0
    tasks: Dict[str, FlinkTask] = field(default_factory=dict)
    metrics: list[VertexMetrics] = field(default_factory=list)

@dataclass
class FlinkApp:
    name: str
    namespace: str
    url: str = ""
    job_id: str = ""
    status: str = ""
    vertices: Dict[str, FlinkVertex] = field(default_factory=dict)