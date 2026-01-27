FROM python:3.11-slim

RUN pip install kubernetes flask

WORKDIR /app

COPY structs.py .
COPY kube_collector.py .
COPY flink_collector.py .
COPY kubeflink.py .
COPY perf_collector.py .
COPY daemon.py .


CMD ["python", "-u", "daemon.py"]