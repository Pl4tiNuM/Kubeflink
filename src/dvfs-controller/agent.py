import argparse
import threading
from flask import Flask, request, jsonify
import time
import os
from typing import Dict, Optional, Tuple, Any

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

# Power monitoring options
parser.add_argument(
    "--power-mode",
    type=str,
    choices=["ondemand", "continuous"],
    default="ondemand",
    help="Power monitoring mode: ondemand (sample on request) or continuous (background sampling).",
)
parser.add_argument(
    "--power-sample-interval",
    type=float,
    default=1.0,
    help="Sampling interval in seconds for continuous mode (default: 1.0).",
)
parser.add_argument(
    "--power-ondemand-window",
    type=float,
    default=1.0,
    help="Sampling window in seconds for ondemand mode (default: 1.0).",
)
parser.add_argument(
    "--rapl-base",
    type=str,
    default="/sys/class/powercap",
    help="Base path for RAPL sysfs (default: /sys/class/powercap).",
)

args, _ = parser.parse_known_args()
SYSFS_BASE = args.sysfs_base.rstrip("/") + "/"
RAPL_BASE = args.rapl_base.rstrip("/")

# -----------------------------
# Helper functions (CPU freq)
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
# RAPL power monitoring
# -----------------------------
_power_lock = threading.Lock()
_power_cache: Dict[str, Any] = {
    "ts": None,              # float epoch seconds
    "window_s": None,        # window used for last measurement
    "per_socket_w": {},      # e.g., {"package-0": 47.2, "package-1": 45.9}
    "dram_w": {},            # optional, per socket if available
    "total_w": None,         # float
    "status": "init",        # "ok" or "N/A"
    "error": None,           # error string if any
}

def _read_int(path: str) -> int:
    with open(path, "r") as f:
        return int(f.read().strip())

def _read_str(path: str) -> str:
    with open(path, "r") as f:
        return f.read().strip()

def _list_rapl_domains() -> Dict[str, Dict[str, str]]:
    """
    Discover RAPL domains.
    Returns mapping:
      {
        "intel-rapl:0": {"name": "package-0", "energy_uj": "..."},
        "intel-rapl:0:0": {"name": "dram", ...}, ...
      }
    """
    domains: Dict[str, Dict[str, str]] = {}
    if not os.path.isdir(RAPL_BASE):
        return domains

    # Prefer intel-rapl entries
    for entry in os.listdir(RAPL_BASE):
        if not entry.startswith("intel-rapl"):
            continue
        d = os.path.join(RAPL_BASE, entry)
        name_path = os.path.join(d, "name")
        energy_path = os.path.join(d, "energy_uj")
        if os.path.isfile(name_path) and os.path.isfile(energy_path):
            domains[entry] = {"dir": d, "name": _read_str(name_path), "energy_uj": energy_path}
    return domains

def _rapl_sample_energy(domains: Dict[str, Dict[str, str]]) -> Dict[str, int]:
    """Read energy_uj for each domain entry id."""
    out: Dict[str, int] = {}
    for dom_id, meta in domains.items():
        out[dom_id] = _read_int(meta["energy_uj"])
    return out

def _compute_power_w(domains: Dict[str, Dict[str, str]], e1: Dict[str, int], e2: Dict[str, int], dt: float) -> Tuple[Dict[str, float], Dict[str, float], float]:
    """
    Compute per-socket package power (W), optional dram power (W), and total package power.
    - package domains are "package-<n>" in meta["name"]
    - dram domains are typically named "dram" or "core"/"uncore" depending on platform; we only expose "dram"
    """
    per_socket_pkg_w: Dict[str, float] = {}
    per_socket_dram_w: Dict[str, float] = {}
    total_pkg = 0.0

    # Map intel-rapl:0 -> package-0, etc.
    # Subdomains intel-rapl:0:0 may be "dram" etc; we attach dram to the parent package key if we can infer it.
    for dom_id, meta in domains.items():
        name = meta["name"].lower()
        if dom_id not in e1 or dom_id not in e2:
            continue
        de_uj = e2[dom_id] - e1[dom_id]
        # Handle counter wrap/reset: ignore negative deltas
        if de_uj < 0:
            continue
        w = de_uj / (dt * 1_000_000.0)

        # Package
        if name.startswith("package"):
            per_socket_pkg_w[meta["name"]] = w
            total_pkg += w

        # DRAM domain handling (varies; common name is "dram")
        elif name == "dram":
            # Try to attach to the parent "intel-rapl:X" by trimming suffixes
            # e.g., intel-rapl:0:0 -> intel-rapl:0
            parent_id = dom_id.split(":")
            if len(parent_id) >= 2:
                parent = ":".join(parent_id[:2])  # "intel-rapl:0"
                parent_name = domains.get(parent, {}).get("name", f"{parent}")
                # parent_name is typically "package-0"
                per_socket_dram_w[parent_name] = w

    return per_socket_pkg_w, per_socket_dram_w, total_pkg

def measure_power(window_s: float) -> Dict[str, Any]:
    """
    Measure power over a sampling window (seconds) using RAPL energy_uj deltas.
    Returns a dict suitable for JSON output.
    """
    domains = _list_rapl_domains()
    if not domains:
        return {
            "status": "N/A",
            "error": f"No RAPL domains found under {RAPL_BASE}. (AMD/VM/container or missing intel_rapl?)",
            "ts": time.time(),
            "window_s": window_s,
            "per_socket_w": {},
            "dram_w": {},
            "total_w": None,
        }

    t1 = time.time()
    e1 = _rapl_sample_energy(domains)
    time.sleep(max(0.01, window_s))
    t2 = time.time()
    e2 = _rapl_sample_energy(domains)
    dt = max(1e-6, t2 - t1)

    per_socket_w, dram_w, total_w = _compute_power_w(domains, e1, e2, dt)

    if not per_socket_w:
        return {
            "status": "N/A",
            "error": "RAPL found but no package domains were readable/usable.",
            "ts": time.time(),
            "window_s": window_s,
            "per_socket_w": {},
            "dram_w": dram_w,
            "total_w": None,
        }

    return {
        "status": "ok",
        "error": None,
        "ts": time.time(),
        "window_s": dt,
        "per_socket_w": per_socket_w,
        "dram_w": dram_w,
        "total_w": total_w,
    }

def _power_sampler_loop():
    """Background sampler for continuous mode."""
    interval = max(0.05, float(args.power_sample_interval))
    while True:
        try:
            m = measure_power(window_s=interval)
            with _power_lock:
                _power_cache.update(m)
        except Exception as e:
            with _power_lock:
                _power_cache.update({
                    "status": "N/A",
                    "error": f"power sampler error: {e}",
                    "ts": time.time(),
                    "window_s": interval,
                    "per_socket_w": {},
                    "dram_w": {},
                    "total_w": None,
                })
        # In continuous mode, we already slept inside measure_power(window_s=interval),
        # so no extra sleep needed.

# Start sampler thread if requested
if args.power_mode == "continuous":
    th = threading.Thread(target=_power_sampler_loop, daemon=True)
    th.start()

# -----------------------------
# REST Endpoints
# -----------------------------
@app.route("/api/set_frequency", methods=["POST"])
def set_frequency():
    """Set CPU frequency for specified cores."""
    data = request.get_json(force=True)
    cores = data["cores"]
    freq = str(data["freq"])
    reset = data.get("reset", "0")  # used to block reduction of frequency in cases of co-location at core level.

    print(f"[INFO] Setting {freq} kHz for cores: {cores}")

    for c in cores:
        try:
            cur_speed = read_sysfs(c, "scaling_cur_freq")
            if reset == "1" or int(cur_speed) < int(freq):
                write_sysfs(c, "scaling_setspeed", freq)
        except Exception as e:
            print(f"[WARN] Failed to set freq for core {c}: {e}")

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

@app.route("/api/get_frequencies_for_cores", methods=["GET"])
def get_frequencies_for_cores():
    """Return current frequency for specific cores.
    
    Query params:
        cores: Core specification (e.g., "0-2,12-14")
    
    Returns:
        JSON with status, cores dict, and unit
    """
    cores_spec = request.args.get("cores", "")
    if not cores_spec:
        return jsonify({"status": "error", "error": "Missing 'cores' parameter"}), 400
    
    try:
        # Parse core specification
        core_list = []
        for part in cores_spec.split(','):
            part = part.strip()
            if '-' in part:
                start, end = part.split('-')
                core_list.extend(range(int(start), int(end) + 1))
            else:
                core_list.append(int(part))
        
        # Get frequencies for specified cores
        cores_data = {}
        for core in core_list:
            try:
                freq = read_sysfs(str(core), "scaling_cur_freq")
                cores_data[str(core)] = int(freq)
            except Exception as e:
                cores_data[str(core)] = None
        
        return jsonify({
            "status": "ok",
            "cores": cores_data,
            "unit": "kHz"
        })
    
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route("/api/get_frequencies_for_cores", methods=["GET"])
def get_frequencies_for_cores():
    """Return current frequency for specific cores.
    
    Query params:
        cores: Core specification (e.g., "0-2,12-14")
    
    Returns:
        JSON with status, cores dict, and unit
    """
    cores_spec = request.args.get("cores", "")
    if not cores_spec:
        return jsonify({"status": "error", "error": "Missing 'cores' parameter"}), 400
    
    try:
        # Parse core specification
        core_list = []
        for part in cores_spec.split(','):
            part = part.strip()
            if '-' in part:
                start, end = part.split('-')
                core_list.extend(range(int(start), int(end) + 1))
            else:
                core_list.append(int(part))
        
        # Get frequencies for specified cores
        cores_data = {}
        for core in core_list:
            try:
                freq = read_sysfs(str(core), "scaling_cur_freq")
                cores_data[str(core)] = int(freq)
            except Exception as e:
                cores_data[str(core)] = None
        
        return jsonify({
            "status": "ok",
            "cores": cores_data,
            "unit": "kHz"
        })
    
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route("/api/get_power", methods=["GET"])
def get_power():
    """
    Return current power consumption.
    Modes:
      - continuous: returns cached latest measurement instantly
      - ondemand: measures over --power-ondemand-window (or query param ?window_s=)
    """
    if args.power_mode == "continuous":
        with _power_lock:
            return jsonify(dict(_power_cache))

    # ondemand
    try:
        window_s = float(request.args.get("window_s", args.power_ondemand_window))
    except Exception:
        window_s = float(args.power_ondemand_window)

    m = measure_power(window_s=max(0.01, window_s))
    return jsonify(m)

# -----------------------------
# Main entry
# -----------------------------
if __name__ == "__main__":
    print(f"[INFO] Starting Flask server on {args.host}:{args.port}")
    print(f"[INFO] Using sysfs base path: {SYSFS_BASE}")
    print(f"[INFO] Power mode: {args.power_mode} (RAPL base: {RAPL_BASE})")
    app.run(host=args.host, port=args.port, threaded=True)
