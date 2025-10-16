import argparse
import threading
from flask import Flask, request, jsonify
import time
import os

app = Flask(__name__)

# -----------------------------
# CLI arguments
# -----------------------------
parser = argparse.ArgumentParser(description="CPU core frequency controller service.")
parser.add_argument(
    "--sysfs-base",
    type=str,
    default="/sys/devices/system/cpu/",
    help="Base sysfs path for CPU frequency control (default: /sys/devices/system/cpu/)",
)
parser.add_argument(
    "--port",
    type=int,
    default=4002,
    help="Flask service port (default: 4002)",
)
parser.add_argument(
    "--host",
    type=str,
    default="0.0.0.0",
    help="Flask host to bind (default: 0.0.0.0)",
)
args, _ = parser.parse_known_args()
SYSFS_BASE = args.sysfs_base.rstrip("/") + "/"

# -----------------------------
# Locks for per-core operations
# -----------------------------
# locks = {str(c): threading.Lock() for c in range(os.cpu_count() or 1)}

# -----------------------------
# Helper functions
# -----------------------------
def cpu_path(cpu_id: str, file_name: str) -> str:
    """Build full path for a CPU's sysfs frequency file."""
    return os.path.join(SYSFS_BASE, f"cpu{cpu_id}", "cpufreq", file_name)


def read_sysfs(cpu_id: str, file_name: str) -> str:
    path = cpu_path(cpu_id, file_name)
    with open(path, "r") as f:
        return f.read().strip()


def write_sysfs(cpu_id: str, file_name: str, value: str):
    path = cpu_path(cpu_id, file_name)
    with open(path, "w") as f:
        f.write(value)


# -----------------------------
# REST Endpoints
# -----------------------------
@app.route("/api/set_frequency", methods=["POST"])
def set_frequency():
    """Set CPU frequency for specified cores."""
    data = request.get_json(force=True)
    cores = data["cores"]
    freq = str(data["freq"])
    reset = data.get("reset", "0") # used to block reduction of frequency in cases of co-location at core level.

    print(f"[INFO] Setting {freq} kHz for cores: {cores}")
    # start = time.time()

    for c in cores:
        try:
            cur_speed = read_sysfs(c, "scaling_cur_freq")
            if reset == "1" or int(cur_speed) < int(freq):
                write_sysfs(c, "scaling_setspeed", freq)
        except Exception as e:
            print(f"[WARN] Failed to set freq for core {c}: {e}")

    # print(f"[DEBUG] Frequency update took {time.time() - start:.4f}s")
    return jsonify({"status": "ok", "freq": freq, "cores": cores})


@app.route("/api/get_frequencies", methods=["GET"])
def get_frequencies():
    """Return current frequency for all available cores."""
    res = {}
    for c in range(os.cpu_count() or 1):
        try:
            res[f"cpu{c}"] = read_sysfs(str(c), "scaling_cur_freq")
        except Exception:
            res[f"cpu{c}"] = "N/A"
    return jsonify(res)


# -----------------------------
# Main entry
# -----------------------------
if __name__ == "__main__":
    print(f"[INFO] Starting Flask server on {args.host}:{args.port}")
    print(f"[INFO] Using sysfs base path: {SYSFS_BASE}")
    app.run(host=args.host, port=args.port, threaded=True)
