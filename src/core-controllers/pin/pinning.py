#!/usr/bin/env python3
from flask import Flask, request, Response
import time
import json
import os
import glob
import threading
from collections import deque
from typing import Optional, Dict, Any, Tuple
import re
import subprocess
from typing import List
import logging


app = Flask(__name__)


def _read_cmdline(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            raw = f.read()
        parts = [p.decode(errors="ignore") for p in raw.split(b"\x00") if p]
        return " ".join(parts)
    except Exception:
        return ""

def _list_tids(pid: int) -> list[int]:
    """List thread IDs (TIDs) for a process PID."""
    try:
        return sorted(int(os.path.basename(p)) for p in glob.glob(f"/proc/{pid}/task/*"))
    except Exception:
        return []

def _read_thread_comm(pid: int, tid: int) -> str:
    """Thread name (comm). Note: truncated to 15 chars by kernel."""
    try:
        with open(f"/proc/{pid}/task/{tid}/comm", "r") as f:
            return f.read().strip()
    except Exception:
        return ""

def _read_thread_allowed_list(pid: int, tid: int) -> str:
    """Current allowed CPU list per thread (kernel view)."""
    try:
        with open(f"/proc/{pid}/task/{tid}/status", "r") as f:
            for line in f:
                if line.startswith("Cpus_allowed_list:"):
                    return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return ""

def _read_thread_last_cpu(pid: int, tid: int) -> Optional[int]:
    """Processor field (39) from /proc/.../stat."""
    st = _read_tid_stat(pid, tid)
    if not st:
        return None
    _ut, _st, proc = st
    return proc

def _pin_tid_taskset(tid: int, cores: str) -> Tuple[bool, str]:
    """
    Pin a single TID using taskset.
    Returns (ok, output_or_error).
    """
    try:
        # taskset -pc <cores> <tid>
        res = subprocess.run(
            ["taskset", "-pc", cores, str(tid)],
            capture_output=True,
            text=True,
            check=True,
        )
        return True, (res.stdout.strip() or "ok")
    except subprocess.CalledProcessError as e:
        err = (e.stderr.strip() or e.stdout.strip() or str(e))
        return False, err
    except FileNotFoundError:
        return False, "taskset not found"

# ============================================================
# Cgroup Utility Functions
# ============================================================

def get_cgroup_version(pid: int) -> int:
    """Checks /proc/<pid>/cgroup to determine if it's V1 or V2."""
    try:
        with open(f"/proc/{pid}/cgroup") as f:
            content = f.read().strip()

        # V2: single line starting with '0::'
        if "\n" not in content and content.startswith("0::"):
            return 2
        return 1
    except FileNotFoundError:
        return 0


def get_cgroup_path_from_pid(pid: int, controller: str) -> Tuple[Optional[str], int]:
    """
    Finds the host cgroup path for a given PID and controller (e.g., 'cpuset', 'cpu', 'cpuacct').
    Returns (path, detected_version).
    """
    version = get_cgroup_version(pid)
    if version == 0:
        return None, 0

    try:
        with open(f"/proc/{pid}/cgroup") as f:
            for line in f:
                line = line.strip()

                if version == 1:
                    # Cgroup v1: hierarchy:controllers:path
                    if f":{controller}:" in line or f":{controller}," in line:
                        rel_path = line.split(":")[-1]
                        return f"/sys/fs/cgroup/{controller}{rel_path}", 1

                elif version == 2:
                    # Cgroup v2: 0::/path
                    if line.startswith("0::"):
                        rel_path = line.split("0::")[1]
                        return f"/sys/fs/cgroup{rel_path}", 2

    except FileNotFoundError:
        return None, 0

    return None, version


# ============================================================
# Crictl Utility
# ============================================================

def run_crictl_command(cmd: list) -> Optional[str]:
    full_cmd = ["crictl"] + cmd
    try:
        result = subprocess.run(
            full_cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
            env={"PATH": os.environ.get("PATH", "") + ":/usr/local/bin:/usr/bin"},
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"🚨 Crictl command failed: {' '.join(full_cmd)}")
        print(f"  Stderr: {e.stderr.strip()}")
        return None
    except subprocess.TimeoutExpired:
        print(f"🚨 Crictl command timed out: {' '.join(full_cmd)}")
        return None
    except FileNotFoundError:
        print("🚨 Crictl command not found. Ensure crictl is in PATH.")
        return None


def get_pid_from_pod_container(pod_name: str, container_name: str = None) -> Optional[int]:
    """Finds the host PID of the container's main process using crictl."""
    print(f"1. Attempting to find Pod ID for name: {pod_name}")
    pod_id_output = run_crictl_command(["pods", "--name", pod_name, "-q"])
    if not pod_id_output:
        print(f"Pod '{pod_name}' not found via crictl pods.")
        return None

    pod_id = pod_id_output.split("\n")[0]
    print(f"-> Found Pod ID: {pod_id}")

    ps_cmd = ["ps", "--pod", pod_id, "-q"]
    if container_name:
        ps_cmd.extend(["--name", container_name])

    container_id = None
    max_retries = 5
    print(f"2. Searching for Container ID (Name: {container_name or 'any'}) in Pod ID {pod_id}")

    for attempt in range(max_retries):
        container_ids_output = run_crictl_command(ps_cmd)
        if container_ids_output:
            container_id = container_ids_output.split("\n")[0]
            print(f"-> Found Container ID: {container_id}")
            break
        if attempt < max_retries - 1:
            print(f"   Container not found yet. Retrying in 1s (Attempt {attempt + 1}/{max_retries})...")
            time.sleep(1)

    if not container_id:
        print(f"Container not found in Pod '{pod_name}' after {max_retries} attempts.")
        return None

    print(f"3. Inspecting Container ID {container_id} to get PID.")
    inspect_output = run_crictl_command(["inspect", "--output", "json", container_id])
    if not inspect_output:
        print(f"🚨 Failed to inspect container {container_id}.")
        return None

    try:
        inspect_data = json.loads(inspect_output)
        pid = inspect_data["info"]["pid"]
        print(f"-> Found Host PID: {pid}")
        return int(pid)
    except (json.JSONDecodeError, KeyError) as e:
        print(f"🚨 Error parsing crictl inspect output: {e}")
        return None

# ============================================================
# Thread Pinning API
# ============================================================

@app.route("/api/pin_threads_by_pattern", methods=["POST"])
def pin_threads_by_pattern():
    """
    Pin threads (TIDs) inside CRI containers using pattern matching.

    POST JSON:
      {
        "pod_pattern": "flink-query1-taskmanager",     # required (regex)
        "namespace_pattern": "default",                # optional regex, default ".*"
        "container_pattern": "flink-main-container",   # optional regex, default ".*"

        "thread_pattern": "GC Thread#|flink-pekko",    # required (regex on /proc/<pid>/task/<tid>/comm)
        "thread_exclude_pattern": "",                  # optional regex exclude
        "cores": "0-1",                                # required

        "only_if_cmdline_matches": "java",             # optional regex on main PID cmdline
        "max_pods": 25,                                # optional
        "max_containers_per_pod": 25,                  # optional
        "max_threads_per_container": 800,              # optional
        "dry_run": false,                              # optional
        "reapply_seconds": 0                           # optional; if >0, repeat once per second for this duration
      }

    Notes:
      - Thread names come from /proc/<pid>/task/<tid>/comm (15-char truncation).
      - This is per-thread sched affinity (taskset), not cgroup cpusets.
      - Effective affinity cannot exceed the container/pod cpuset restriction.
    """
    logger = logging.getLogger(__name__)
    start = time.time()
    try:
        data = request.get_json(force=True)
        logger.info(f"[THREAD_PIN] Request received: pod={data.get('pod_pattern')} thread={data.get('thread_pattern')} cores={data.get('cores')}")

        pod_pat = str(data.get("pod_pattern", "")).strip()
        ns_pat = str(data.get("namespace_pattern", ".*")).strip()
        cont_pat = str(data.get("container_pattern", ".*")).strip()

        thr_pat = str(data.get("thread_pattern", "")).strip()
        thr_excl = str(data.get("thread_exclude_pattern", "")).strip()

        cores = str(data.get("cores", "")).strip()
        if not pod_pat:
            raise ValueError("pod_pattern is required")
        if not thr_pat:
            raise ValueError("thread_pattern is required")
        if not cores:
            raise ValueError("cores is required")

        cmd_guard = str(data.get("only_if_cmdline_matches", "")).strip()

        max_pods = int(data.get("max_pods", 25))
        max_cont_per_pod = int(data.get("max_containers_per_pod", 25))
        max_thr = int(data.get("max_threads_per_container", 800))
        dry_run = bool(data.get("dry_run", False))
        reapply_seconds = float(data.get("reapply_seconds", 0))

        pod_re = re.compile(pod_pat)
        ns_re = re.compile(ns_pat)
        cont_re = re.compile(cont_pat)
        thr_re = re.compile(thr_pat)
        thr_excl_re = re.compile(thr_excl) if thr_excl else None
        cmd_re = re.compile(cmd_guard) if cmd_guard else None

    except Exception as e:
        return Response(
            response=json.dumps({"ok": False, "error": f"Invalid request data: {e}"}),
            status=400,
            mimetype="application/json",
        )

    def _list_pods_json() -> Optional[dict]:
        out = run_crictl_command(["pods", "-o", "json"])
        if not out:
            return None
        try:
            return json.loads(out)
        except Exception:
            return None

    def _list_containers_json(pod_id: str) -> Optional[dict]:
        out = run_crictl_command(["ps", "-a", "--pod", pod_id, "-o", "json"])
        if not out:
            return None
        try:
            return json.loads(out)
        except Exception:
            return None

    def _inspect_json(container_id: str) -> Optional[dict]:
        out = run_crictl_command(["inspect", "--output", "json", container_id])
        if not out:
            return None
        try:
            return json.loads(out)
        except Exception:
            return None

    def _discover_targets() -> list[dict]:
        pods = _list_pods_json()
        if not pods or "items" not in pods:
            logger.info(f"[THREAD_PIN] No pods found via crictl")
            return []

        logger.info(f"[THREAD_PIN] Found {len(pods.get('items', []))} total pods from crictl")

        targets: list[dict] = []
        for item in pods["items"]:
            meta = item.get("metadata", {}) or {}
            pod_name = meta.get("name", "") or ""
            ns = meta.get("namespace", "") or ""
            pod_id = item.get("id", "") or ""
            if not pod_id:
                continue

            if not (pod_re.search(pod_name) and ns_re.search(ns)):
                continue

            logger.info(f"[THREAD_PIN] Pod matched: {pod_name} (namespace={ns})")

            conts = _list_containers_json(pod_id)
            if not conts:
                continue

            added = 0
            for c in conts.get("containers", []) or []:
                if added >= max_cont_per_pod:
                    break
                cid = c.get("id", "") or ""
                cname = (c.get("metadata", {}) or {}).get("name", "") or ""
                if not cid or not cont_re.search(cname):
                    continue

                insp = _inspect_json(cid)
                if not insp:
                    continue
                pid = (insp.get("info", {}) or {}).get("pid", None)
                if not pid:
                    continue

                targets.append({
                    "pod_id": pod_id,
                    "pod_name": pod_name,
                    "namespace": ns,
                    "container_id": cid,
                    "container_name": cname,
                    "pid": int(pid),
                })
                added += 1

            if len(targets) >= max_pods * max_cont_per_pod:
                break

        return targets[: max_pods * max_cont_per_pod]

    targets = _discover_targets()
    if not targets:
        return Response(
            response=json.dumps({"ok": False, "error": "No matching (pod/container) targets found via crictl"}),
            status=404,
            mimetype="application/json",
        )

    def _apply_once() -> list[dict]:
        results: list[dict] = []
        for t in targets:
            pid = t["pid"]

            # Optional guard: only act if main process cmdline matches
            if cmd_re:
                cmdline = _read_cmdline(pid)
                if not cmd_re.search(cmdline):
                    results.append({
                        **t,
                        "ok": True,
                        "skipped": True,
                        "skip_reason": "cmdline_guard_no_match",
                        "pid_cmdline": cmdline[:300],
                    })
                    continue

            tids = _list_tids(pid)[:max_thr]
            logger.info(f"[THREAD_PIN] Container {t['container_name']}: PID={pid}, found {len(tids)} threads total")

            matched: list[dict] = []
            pinned_ok: list[dict] = []
            pinned_fail: list[dict] = []

            for tid in tids:
                name = _read_thread_comm(pid, tid)
                if not name:
                    continue
                if not thr_re.search(name):
                    continue
                if thr_excl_re and thr_excl_re.search(name):
                    continue

                logger.info(f"[THREAD_PIN] Matched thread: tid={tid} name='{name}' pattern='{thr_pat}'")

                if dry_run:
                    before_allowed = _read_thread_allowed_list(pid, tid)
                    before_last = _read_thread_last_cpu(pid, tid)
                    ok, msg = True, "dry_run"
                    after_allowed = _read_thread_allowed_list(pid, tid)
                    after_last = _read_thread_last_cpu(pid, tid)
                else:
                    # Pin immediately without reading before state (avoid delays)
                    import time
                    start = time.time()
                    logger.info(f"[THREAD_PIN] Calling taskset for tid={tid} name='{name}' cores={cores}")
                    ok, msg = _pin_tid_taskset(tid, cores)
                    elapsed_ms = int((time.time() - start) * 1000)

                    # Skip ephemeral threads that exited (don't count as failure)
                    if not ok and "No such process" in msg:
                        logger.info(f"[THREAD_PIN] tid={tid} name='{name}' exited before pinning (ephemeral thread)")
                        continue  # Skip this thread entirely

                    logger.info(f"[THREAD_PIN] taskset tid={tid} name='{name}' ok={ok} elapsed={elapsed_ms}ms msg='{msg}'")

                    # Read state after pinning
                    before_allowed = "unknown"
                    before_last = -1
                    after_allowed = _read_thread_allowed_list(pid, tid)
                    after_last = _read_thread_last_cpu(pid, tid)

                rec = {
                    "tid": tid,
                    "name": name,
                    "before_allowed": before_allowed,
                    "after_allowed": after_allowed,
                    "before_last_cpu": before_last,
                    "after_last_cpu": after_last,
                    "action": msg,
                }
                matched.append(rec)
                if ok:
                    pinned_ok.append(rec)
                else:
                    pinned_fail.append(rec)

            results.append({
                **t,
                "ok": True,
                "dry_run": dry_run,
                "cores": cores,
                "thread_pattern": thr_pat,
                "thread_exclude_pattern": (thr_excl if thr_excl else None),
                "matched_threads": len(matched),
                "pinned_ok": len(pinned_ok),
                "pinned_failed": len(pinned_fail),
                "matched_preview": matched[:30],
                "pinned_preview": pinned_ok[:30],
                "failed_preview": pinned_fail[:30],
                "notes": [
                    "Thread names come from /proc/<pid>/task/<tid>/comm (15-char truncation).",
                    "If container/pod cpuset is narrower than requested cores, taskset will fail or be constrained.",
                    "Use reapply_seconds to catch newly spawned JVM threads."
                ],
            })

        return results

    # Apply once or repeatedly for a duration
    if reapply_seconds and reapply_seconds > 0:
        rounds = []
        end_t = time.time() + reapply_seconds
        r = 0
        while time.time() < end_t:
            r += 1
            rounds.append({"round": r, "ts": time.time(), "results": _apply_once()})
            time.sleep(1.0)

        payload = {
            "ok": True,
            "mode": "reapply",
            "reapply_seconds": reapply_seconds,
            "rounds": rounds,
            "targets": targets,
            "elapsed_s": time.time() - start,
        }
    else:
        payload = {
            "ok": True,
            "mode": "once",
            "results": _apply_once(),
            "targets": targets,
            "elapsed_s": time.time() - start,
        }

    return Response(response=json.dumps(payload), status=200, mimetype="application/json")

# ============================================================
# Cgroup operations (quota + pin)
# ============================================================

@app.route("/api/set_cgroup_quota", methods=["POST"])
def set_cgroup_quota():
    """Sets CPU CFS quota (V1) or cpu.max (V2) for a container."""
    start = time.time()
    try:
        data = request.get_json(force=True)
        quota_pct = data.get("quota_pct", None)
        period_us = int(data.get("period_us", 100_000))
        pod_name = data["pod_name"]
        container_name = data.get("container_name")
    except Exception as e:
        return Response(
            response=json.dumps({"ok": False, "error": f"Invalid request data: {e}"}),
            status=400,
            mimetype="application/json",
        )

    if quota_pct is not None:
        try:
            quota_us = int(period_us * float(quota_pct) / 100.0)
        except ValueError:
            return Response(
                response=json.dumps({"ok": False, "error": f"Invalid quota_pct: {quota_pct}"}),
                status=400,
                mimetype="application/json",
            )
    else:
        quota_us = data.get("quota_us", None)
        if quota_us is None:
            return Response(
                response=json.dumps({"ok": False, "error": "Must provide either quota_pct or quota_us"}),
                status=400,
                mimetype="application/json",
            )
        quota_us = int(quota_us)

    pid = get_pid_from_pod_container(pod_name, container_name)
    if not pid:
        return Response(
            response=json.dumps({"ok": False, "error": f"Container not found for Pod '{pod_name}'"}),
            status=404,
            mimetype="application/json",
        )

    cpu_path, version = get_cgroup_path_from_pid(pid, "cpu")
    if not cpu_path or not os.path.exists(cpu_path):
        return Response(
            response=json.dumps({"ok": False, "error": f"Could not find cpu cgroup path for PID {pid} (V{version})"}),
            status=500,
            mimetype="application/json",
        )

    try:
        if version == 1:
            with open(os.path.join(cpu_path, "cpu.cfs_period_us"), "w") as f:
                f.write(str(period_us))
            with open(os.path.join(cpu_path, "cpu.cfs_quota_us"), "w") as f:
                f.write(str(quota_us))
            file_names = "cpu.cfs_period_us + cpu.cfs_quota_us"
        elif version == 2:
            with open(os.path.join(cpu_path, "cpu.max"), "w") as f:
                f.write(f"{quota_us} {period_us}")
            file_names = "cpu.max"
        else:
            raise RuntimeError("Unsupported cgroup version")

        elapsed = time.time() - start
        return Response(
            response=json.dumps(
                {
                    "ok": True,
                    "msg": f"Set CPU quota (cgroup v{version})",
                    "pid": pid,
                    "period_us": period_us,
                    "quota_us": quota_us,
                    "quota_pct": quota_pct,
                    "files": file_names,
                    "elapsed_s": elapsed,
                }
            ),
            status=200,
            mimetype="application/json",
        )

    except Exception as e:
        return Response(
            response=json.dumps({"ok": False, "error": f"Error setting CPU quota: {e}"}),
            status=500,
            mimetype="application/json",
        )


@app.route("/api/pin_pod_cores", methods=["POST"])
def pin_pod_cores():
    start = time.time()
    try:
        data = request.get_json(force=True)
        pod_name = data["pod_name"]
        cores = data["cores"]
        container_name = data.get("container_name")
        # NEW: allow choosing target scope
        # "pod" pins the parent of container .scope (recommended)
        # "container" pins the container cgroup itself
        pin_scope = str(data.get("pin_scope", "pod")).strip().lower()
        if pin_scope not in ("pod", "container"):
            pin_scope = "pod"
    except Exception as e:
        return Response(
            response=json.dumps({"ok": False, "error": f"Invalid request data: {e}"}),
            status=400,
            mimetype="application/json",
        )

    pid = get_pid_from_pod_container(pod_name, container_name)
    if not pid:
        return Response(
            response=json.dumps({"ok": False, "error": f"Container not found for Pod '{pod_name}'"}),
            status=404,
            mimetype="application/json",
        )

    version = get_cgroup_version(pid)
    if version != 2:
        return Response(
            response=json.dumps({"ok": False, "error": f"Expected cgroup v2 (found v{version})"}),
            status=500,
            mimetype="application/json",
        )

    try:
        pin_info = pin_cpuset_v2_from_pid(pid, cores, pin_scope=pin_scope)

        elapsed = time.time() - start
        return Response(
            response=json.dumps(
                {
                    "ok": True,
                    "msg": "Pinned cores via cgroup v2 cpuset",
                    "pod_name": pod_name,
                    "container_name": container_name,
                    "pid": pid,
                    "pin_scope": pin_scope,
                    **pin_info,
                    "elapsed_s": elapsed,
                    "note": "Verification is cpuset.cpus_effective in target cgroup",
                }
            ),
            status=200,
            mimetype="application/json",
        )
    except Exception as e:
        return Response(
            response=json.dumps({"ok": False, "error": f"Error pinning cores (v2): {e}", "pid": pid}),
            status=500,
            mimetype="application/json",
        )


# @app.route("/api/pin_pod_cores", methods=["POST"])
# def pin_pod_cores():
#     """Pins the container of a Pod to specific CPU cores via cpuset.cpus (v1 or v2)."""
#     start = time.time()
#     try:
#         data = request.get_json(force=True)
#         pod_name = data["pod_name"]
#         cores = data["cores"]  # e.g. "2-3" or "0,2,4-6"
#         container_name = data.get("container_name")
#     except Exception as e:
#         return Response(
#             response=json.dumps({"ok": False, "error": f"Invalid request data: {e}"}),
#             status=400,
#             mimetype="application/json",
#         )

#     pid = get_pid_from_pod_container(pod_name, container_name)
#     if not pid:
#         return Response(
#             response=json.dumps({"ok": False, "error": f"Container not found for Pod '{pod_name}'"}),
#             status=404,
#             mimetype="application/json",
#         )

#     cpuset_path, version = get_cgroup_path_from_pid(pid, "cpuset")
#     if not cpuset_path or not os.path.exists(cpuset_path):
#         return Response(
#             response=json.dumps({"ok": False, "error": f"Could not find cpuset cgroup path for PID {pid} (V{version})"}),
#             status=500,
#             mimetype="application/json",
#         )

#     cpuset_file = os.path.join(cpuset_path, "cpuset.cpus")
#     try:
#         with open(cpuset_file, "w") as f:
#             f.write(cores)

#         elapsed = time.time() - start
#         return Response(
#             response=json.dumps(
#                 {
#                     "ok": True,
#                     "msg": "Pinned pod cores",
#                     "pod_name": pod_name,
#                     "container_name": container_name,
#                     "pid": pid,
#                     "cgroup_version": version,
#                     "cpuset_path": cpuset_path,
#                     "cpuset_written": cores,
#                     "elapsed_s": elapsed,
#                 }
#             ),
#             status=200,
#             mimetype="application/json",
#         )
#     except Exception as e:
#         return Response(
#             response=json.dumps({"ok": False, "error": f"Error pinning cores: {e}"}),
#             status=500,
#             mimetype="application/json",
#         )


# ============================================================
# CPU utilization: helpers
# ============================================================

HZ = os.sysconf(os.sysconf_names["SC_CLK_TCK"])


def _parse_cpuset_cpus(cpuset: str) -> int:
    cpuset = (cpuset or "").strip()
    if not cpuset:
        return 0
    count = 0
    for part in cpuset.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            count += int(b) - int(a) + 1
        else:
            count += 1
    return count


# def _read_cpuset_for_pid(pid: int) -> str:
#     cpuset_path, _ = get_cgroup_path_from_pid(pid, "cpuset")
#     if not cpuset_path or not os.path.exists(cpuset_path):
#         return ""
#     fpath = os.path.join(cpuset_path, "cpuset.cpus")
#     try:
#         with open(fpath, "r") as f:
#             return f.read().strip()
#     except Exception:
#         return ""

def _read_cpuset_for_pid(pid: int) -> str:
    ver = get_cgroup_version(pid)
    try:
        if ver == 2:
            cgdir = get_cgroup2_dir_from_pid(pid)
            eff = os.path.join(cgdir, "cpuset.cpus.effective")
            if os.path.exists(eff):
                return _read(eff)
            # fallback
            fpath = os.path.join(cgdir, "cpuset.cpus")
            return _read(fpath) if os.path.exists(fpath) else ""
        else:
            cpuset_path, _ = get_cgroup_path_from_pid(pid, "cpuset")
            if not cpuset_path or not os.path.exists(cpuset_path):
                return ""
            fpath = os.path.join(cpuset_path, "cpuset.cpus")
            return _read(fpath) if os.path.exists(fpath) else ""
    except Exception:
        return ""


def _read_proc_stat_cpu_line(cpu: str) -> Optional[list]:
    try:
        with open("/proc/stat", "r") as f:
            for line in f:
                if line.startswith(cpu + " "):
                    parts = line.split()
                    return [int(x) for x in parts[1:]]
    except Exception:
        pass
    return None


def _cpu_util_host_by_cpu(cpus: set[int], window_s: float) -> Dict[int, Optional[float]]:
    """Returns per-cpu host utilization (% busy) over window_s, based on /proc/stat deltas."""
    snap0: Dict[int, list[int]] = {}
    for c in cpus:
        vals = _read_proc_stat_cpu_line(f"cpu{c}")
        if vals:
            snap0[c] = vals

    time.sleep(max(window_s, 0.05))

    out: Dict[int, Optional[float]] = {}
    for c in cpus:
        a = snap0.get(c)
        b = _read_proc_stat_cpu_line(f"cpu{c}")
        if not a or not b:
            out[c] = None
            continue

        idle_a = a[3] + (a[4] if len(a) > 4 else 0)
        idle_b = b[3] + (b[4] if len(b) > 4 else 0)
        total_a = sum(a)
        total_b = sum(b)

        dt = total_b - total_a
        didle = idle_b - idle_a
        if dt <= 0:
            out[c] = None
            continue
        out[c] = (dt - didle) / dt * 100.0

    return out


def _read_tid_stat(pid: int, tid: int) -> Optional[Tuple[int, int, int]]:
    """
    Returns (utime_ticks, stime_ticks, processor_id).
    processor_id is field 39 in /proc/[pid]/task/[tid]/stat.
    """
    try:
        with open(f"/proc/{pid}/task/{tid}/stat", "r") as f:
            s = f.read()
        rparen = s.rfind(")")
        after = s[rparen + 2 :].split()  # starts at field 3 (state)
        ut = int(after[11])  # field 14
        st = int(after[12])  # field 15
        proc = int(after[36])  # field 39
        return ut, st, proc
    except Exception:
        return None


def _container_util_by_cpu(pid: int, cpus: set[int], window_s: float) -> Dict[int, float]:
    """
    Attributes per-thread CPU time deltas to each thread's last seen CPU.
    Returns utilization per cpu as % of one core (100% == one full core).
    """
    # snapshot A
    try:
        tids = [int(os.path.basename(p)) for p in glob.glob(f"/proc/{pid}/task/*")]
    except Exception:
        tids = []

    a: Dict[int, Tuple[int, int]] = {}  # tid -> (ticks, last_cpu)
    for tid in tids:
        st = _read_tid_stat(pid, tid)
        if st:
            ut, stime, cpu = st
            a[tid] = (ut + stime, cpu)

    t0 = time.time()
    time.sleep(max(window_s, 0.05))
    t1 = time.time()
    dt = max(t1 - t0, 1e-6)

    # snapshot B
    try:
        tids2 = [int(os.path.basename(p)) for p in glob.glob(f"/proc/{pid}/task/*")]
    except Exception:
        tids2 = []

    b: Dict[int, Tuple[int, int]] = {}
    for tid in tids2:
        st = _read_tid_stat(pid, tid)
        if st:
            ut, stime, cpu = st
            b[tid] = (ut + stime, cpu)

    per_cpu_ticks: Dict[int, int] = {c: 0 for c in cpus}

    for tid, (ticks_b, cpu_b) in b.items():
        if tid not in a:
            continue
        ticks_a, _cpu_a = a[tid]
        dticks = ticks_b - ticks_a
        if dticks <= 0:
            continue
        if cpu_b in per_cpu_ticks:
            per_cpu_ticks[cpu_b] += dticks

    per_cpu_util: Dict[int, float] = {}
    for c, ticks in per_cpu_ticks.items():
        cpu_sec = ticks / HZ
        per_cpu_util[c] = (cpu_sec / dt) * 100.0

    return per_cpu_util


def _parse_cpu_list(s: str, allow_all: bool = True) -> set[int]:
    """
    Parse cpu list spec into a set of ints.
    Examples: "2", "0-3", "0-3,8,10-11", "all"
    """
    if s is None:
        raise ValueError("cpus must be provided")

    spec = str(s).strip().lower()
    if allow_all and spec in ("all", "*"):
        return set(_get_online_cpus())

    out: set[int] = set()
    if spec == "":
        return out

    for token in spec.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            lo_s, hi_s = token.split("-", 1)
            lo, hi = int(lo_s.strip()), int(hi_s.strip())
            if hi < lo:
                raise ValueError(f"Invalid range '{token}' (hi < lo)")
            out.update(range(lo, hi + 1))
        else:
            out.add(int(token))

    online = set(_get_online_cpus())
    bad = sorted(out - online)
    if bad:
        raise ValueError(f"Requested CPUs not online/valid: {bad}")
    return out


def _get_online_cpus() -> list[int]:
    """Returns online CPUs as a sorted list."""
    online_path = "/sys/devices/system/cpu/online"
    try:
        with open(online_path, "r") as f:
            s = f.read().strip()
        return sorted(_parse_cpu_list(s, allow_all=False))
    except Exception:
        n = os.cpu_count() or 1
        return list(range(n))


# ============================================================
# CPU utilization monitoring state + history
#   NEW SEMANTICS:
#     - One monitor per (scope, pod_name, container_name)
#     - The monitor always samples ALL online CPUs
#     - GET slices per requested CPU list
# ============================================================

_cpu_monitors_lock = threading.Lock()
_cpu_monitors: Dict[str, Dict[str, Any]] = {}  # key -> state dict


def _monitor_key(scope: str, pod_name: Optional[str], container_name: Optional[str]) -> str:
    # host scope uses empty pod/container -> "host::::"
    return f"{scope}::{pod_name or ''}::{container_name or ''}"


def _sample_cpu_util_all(pod_name: Optional[str],
                         container_name: Optional[str],
                         scope: str,
                         window_s: float) -> Dict[str, Any]:
    """
    Sample utilization for ALL online CPUs, returning the same output schema as before.
    """
    cpus = set(_get_online_cpus())
    return _sample_cpu_util_by_cpu(pod_name, container_name, scope, cpus, window_s)


def _cpu_monitor_loop(pod_name: Optional[str],
                      container_name: Optional[str],
                      scope: str,
                      key: str,
                      stop_evt: threading.Event,
                      interval_s: float,
                      window_s: float):
    while not stop_evt.is_set():
        obs = _sample_cpu_util_all(pod_name, container_name, scope, window_s)

        with _cpu_monitors_lock:
            st = _cpu_monitors.get(key)
            if st is not None:
                st["last"] = obs
                st["history"].append(obs)

        # responsive stop
        sleep_left = max(interval_s, 0.1)
        step = 0.1
        while sleep_left > 0 and not stop_evt.is_set():
            time.sleep(min(step, sleep_left))
            sleep_left -= step


def _sample_cpu_util_by_cpu(pod_name: Optional[str],
                            container_name: Optional[str],
                            scope: str,
                            cpus: set[int],
                            window_s: float) -> Dict[str, Any]:
    """
    Returns per-cpu utilization for either:
      scope='host'      : host /proc/stat per cpu
      scope='container' : attribute container thread CPU time to CPUs
    """
    if not cpus:
        return {"ok": False, "error": "cpus must be non-empty"}

    if scope == "host":
        util = _cpu_util_host_by_cpu(cpus, window_s)
        return {
            "ok": True,
            "scope": "host",
            "cpus": sorted(list(cpus)),
            "window_s": window_s,
            "util_pct_one_core": {str(k): util[k] for k in sorted(util.keys())},
            "ts": time.time(),
        }

    if scope != "container":
        return {"ok": False, "error": f"Unknown scope '{scope}', expected 'host' or 'container'"}

    if not pod_name:
        return {"ok": False, "error": "pod_name is required for scope=container"}

    pid = get_pid_from_pod_container(pod_name, container_name)
    if not pid:
        return {"ok": False, "error": f"Container not found for pod '{pod_name}'"}

    cpuset = _read_cpuset_for_pid(pid)
    cpuset_cnt = _parse_cpuset_cpus(cpuset)

    util = _container_util_by_cpu(pid, cpus, window_s)

    util_norm = None
    if cpuset_cnt > 0:
        util_norm = {str(c): (util.get(c, 0.0) / cpuset_cnt) for c in sorted(cpus)}

    return {
        "ok": True,
        "scope": "container",
        "pod_name": pod_name,
        "container_name": container_name,
        "pid": pid,
        "cpus": sorted(list(cpus)),
        "window_s": window_s,
        "cpuset_cpus": cpuset,
        "cpuset_cpu_count": cpuset_cnt,
        "util_pct_one_core": {str(c): util.get(c, 0.0) for c in sorted(cpus)},
        "util_pct_normalized": util_norm,
        "ts": time.time(),
        "note": "container per-cpu util is attributed by threads' last-seen CPU; best when pinned via cpuset",
    }


def _slice_util_payload(payload: Dict[str, Any], cpus: set[int]) -> Dict[str, Any]:
    """
    Given a full (all-cpu) payload, return a copy containing only requested CPUs.
    """
    if not payload or not isinstance(payload, dict):
        return payload

    out = dict(payload)
    util = payload.get("util_pct_one_core", {})
    if isinstance(util, dict):
        wanted = [str(c) for c in sorted(cpus)]
        out["cpus_requested"] = sorted(list(cpus))
        out["util_pct_one_core"] = {k: util.get(k) for k in wanted}

    # if normalized exists
    utiln = payload.get("util_pct_normalized", None)
    if isinstance(utiln, dict):
        wanted = [str(c) for c in sorted(cpus)]
        out["util_pct_normalized"] = {k: utiln.get(k) for k in wanted}

    # also set cpus field to requested for clarity
    out["cpus"] = sorted(list(cpus))
    return out


# ============================================================
# CPU utilization endpoints (NEW SEMANTICS)
# ============================================================

@app.route("/api/start_cpu_monitor", methods=["POST"])
def start_cpu_monitor():
    """
    Start continuous monitoring for ALL online CPUs.

    JSON body:
      {
        "scope": "host" | "container" (default "container"),
        "pod_name": "...",           # required if scope=container
        "container_name": "...",     # optional
        "cpus": "all" | "*"          # optional; must be 'all' if provided
        "interval_s": 1.0,
        "window_s": 0.5,
        "history_size": 60
      }

    NOTE: One monitor per (scope,pod_name,container_name). Queries can slice CPUs.
    """
    data = request.get_json(force=True)
    scope = str(data.get("scope", "container")).strip()
    pod_name = data.get("pod_name")
    container_name = data.get("container_name")
    cpus_req = str(data.get("cpus", "all")).strip().lower()
    interval_s = float(data.get("interval_s", 1.0))
    window_s = float(data.get("window_s", 0.5))
    history_size = int(data.get("history_size", 60))

    if scope == "container" and not pod_name:
        return Response(
            response=json.dumps({"ok": False, "error": "pod_name is required for scope=container"}),
            status=400,
            mimetype="application/json",
        )

    if cpus_req not in ("", "all", "*"):
        return Response(
            response=json.dumps(
                {
                    "ok": False,
                    "error": "This server runs one global monitor per (scope,pod,container). "
                             "Start with cpus='all' (or omit). Query subsets via GET cpus=...",
                }
            ),
            status=400,
            mimetype="application/json",
        )

    key = _monitor_key(scope, pod_name, container_name)

    with _cpu_monitors_lock:
        if key in _cpu_monitors and _cpu_monitors[key]["thread"].is_alive():
            return Response(
                response=json.dumps({"ok": True, "status": "already_running", "key": key}),
                status=200,
                mimetype="application/json",
            )

        stop_evt = threading.Event()
        th = threading.Thread(
            target=_cpu_monitor_loop,
            args=(pod_name, container_name, scope, key, stop_evt, interval_s, window_s),
            daemon=True,
        )
        _cpu_monitors[key] = {
            "thread": th,
            "stop_evt": stop_evt,
            "last": None,
            "history": deque(maxlen=max(1, history_size)),
            "interval_s": interval_s,
            "window_s": window_s,
            "scope": scope,
            "pod_name": pod_name,
            "container_name": container_name,
            "cpus_str": "all",
        }
        th.start()

    return Response(
        response=json.dumps(
            {
                "ok": True,
                "status": "started",
                "key": key,
                "scope": scope,
                "pod_name": pod_name,
                "container_name": container_name,
                "cpus": "all",
                "interval_s": interval_s,
                "window_s": window_s,
                "history_size": history_size,
                "note": "Monitor samples all online CPUs; GET can slice per requested cpus",
            }
        ),
        status=200,
        mimetype="application/json",
    )


@app.route("/api/stop_cpu_monitor", methods=["POST"])
def stop_cpu_monitor():
    """
    Stop a monitor.

    JSON body:
      { "key": "host::::" } OR:
      { "scope":"host" } OR:
      { "scope":"container", "pod_name":"...", "container_name":"..." }
    """
    data = request.get_json(force=True)

    key = data.get("key")
    if not key:
        scope = str(data.get("scope", "container")).strip()
        pod_name = data.get("pod_name")
        container_name = data.get("container_name")
        if scope == "container" and not pod_name:
            return Response(
                response=json.dumps({"ok": False, "error": "pod_name required for scope=container (or provide key)"}),
                status=400,
                mimetype="application/json",
            )
        key = _monitor_key(scope, pod_name, container_name)

    with _cpu_monitors_lock:
        st = _cpu_monitors.get(key)
        if not st:
            return Response(
                response=json.dumps({"ok": True, "status": "not_running", "key": key}),
                status=200,
                mimetype="application/json",
            )
        st["stop_evt"].set()

    return Response(
        response=json.dumps({"ok": True, "status": "stopping", "key": key}),
        status=200,
        mimetype="application/json",
    )

def get_cgroup2_dir_from_pid(pid: int) -> str:
    """Return /sys/fs/cgroup/<path> from /proc/<pid>/cgroup (v2 unified)."""
    with open(f"/proc/{pid}/cgroup") as f:
        for line in f:
            line = line.strip()
            if line.startswith("0::"):
                rel = line.split("0::", 1)[1]
                return "/sys/fs/cgroup" + rel
    raise RuntimeError(f"PID {pid} does not look like cgroup v2")


def _read(path: str) -> str:
    with open(path, "r") as f:
        return f.read().strip()


def _write(path: str, s: str) -> None:
    with open(path, "w") as f:
        f.write(s)


def _try_enable_cpuset_on_parent(child_cgdir: str) -> None:
    """
    In cgroup v2, controllers must be enabled in parent cgroup.subtree_control.
    We try to enable +cpuset in the nearest ancestor where it's available.
    """
    cur = os.path.dirname(child_cgdir.rstrip("/"))
    while True:
        controllers = os.path.join(cur, "cgroup.controllers")
        subtree = os.path.join(cur, "cgroup.subtree_control")
        if os.path.exists(controllers) and os.path.exists(subtree):
            ctrls = set((_read(controllers) or "").split())
            if "cpuset" in ctrls:
                enabled = set((_read(subtree) or "").split())
                if "cpuset" not in enabled:
                    # Append +cpuset (keep existing content)
                    prev = _read(subtree)
                    add = (" " if prev else "") + "+cpuset"
                    _write(subtree, prev + add)
                return
        parent = os.path.dirname(cur.rstrip("/"))
        if parent == cur or parent == "":
            break
        cur = parent
    # Not always fatal: some systems don't allow enabling here.
    # We'll rely on file existence checks later.


def _ensure_cpuset_mems(cgdir: str) -> None:
    """
    For v2 cpuset, writing cpuset.cpus can fail if mems are empty.
    Ensure cpuset.mems is set from cpuset.mems.effective when needed.
    """
    mems = os.path.join(cgdir, "cpuset.mems")
    mems_eff = os.path.join(cgdir, "cpuset.mems.effective")
    if os.path.exists(mems) and os.path.exists(mems_eff):
        cur = _read(mems)
        if cur == "":
            eff = _read(mems_eff)
            if eff != "":
                _write(mems, eff)


def _pick_target_cgdir_for_pod_pin(container_cgdir: str) -> str:
    """
    Kubernetes+systemd usually puts container in .../pod<uid>.slice/<runtime>.scope
    Pin pod-level by default: parent of the container scope.
    If container_cgdir isn't a .scope, just pin container_cgdir.
    """
    base = os.path.basename(container_cgdir.rstrip("/"))
    if base.endswith(".scope"):
        return os.path.dirname(container_cgdir.rstrip("/"))
    return container_cgdir


def pin_cpuset_v2_from_pid(pid: int, cores: str, pin_scope: str = "pod") -> dict:
    """
    Pin cpuset on cgroup v2.
    pin_scope: "container" or "pod" (pod = parent of .scope when applicable)
    Returns a dict with verification fields.
    """
    container_dir = get_cgroup2_dir_from_pid(pid)
    target_dir = container_dir
    if pin_scope == "pod":
        target_dir = _pick_target_cgdir_for_pod_pin(container_dir)

    # Try to enable cpuset controller on parent so target exposes cpuset.* knobs
    _try_enable_cpuset_on_parent(target_dir)

    cpus_file = os.path.join(target_dir, "cpuset.cpus")
    eff_file = os.path.join(target_dir, "cpuset.cpus.effective")

    if not os.path.exists(cpus_file) or not os.path.exists(eff_file):
        raise RuntimeError(
            f"cpuset files not present in {target_dir}. "
            f"(cpuset controller likely not enabled for this subtree)"
        )

    _ensure_cpuset_mems(target_dir)

    # Write + verify
    _write(cpus_file, cores)

    eff = _read(eff_file)
    conf = _read(cpus_file)

    return {
        "container_cgdir": container_dir,
        "target_cgdir": target_dir,
        "cpuset_cpus": conf,
        "cpuset_cpus_effective": eff,
    }


@app.route("/api/get_cpu_utilization_by_cpu", methods=["GET"])
def get_cpu_utilization_by_cpu():
    """
    Returns per-CPU utilization, sliced from the ALL-CPU monitor.

    Query params:
      scope=host|container (default container)
      cpus=2,3,4           (required; can be subset)
      pod_name=...         (required if scope=container)
      container_name=...   (optional)
      window_s=0.5         (used only for on-demand sampling)
      mode=last|sample     (default: last if monitor running else sample)
      history_n=0          (if >0, return last N history samples too)
    """
    scope = (request.args.get("scope", "container") or "container").strip()
    cpus_str = (request.args.get("cpus", "") or "").strip()
    pod_name = request.args.get("pod_name")
    container_name = request.args.get("container_name")
    window_s = float(request.args.get("window_s", 0.5))
    mode = (request.args.get("mode", "") or "").strip().lower()
    history_n = int(request.args.get("history_n", 0))

    try:
        cpus = _parse_cpu_list(cpus_str, allow_all=False)
    except Exception as e:
        return Response(
            response=json.dumps({"ok": False, "error": f"invalid cpus: {e}"}),
            status=400,
            mimetype="application/json",
        )

    if not cpus:
        return Response(
            response=json.dumps({"ok": False, "error": "cpus is required (e.g. '2,3' or '2-4')"}),
            status=400,
            mimetype="application/json",
        )

    if scope == "container" and not pod_name:
        return Response(
            response=json.dumps({"ok": False, "error": "pod_name is required for scope=container"}),
            status=400,
            mimetype="application/json",
        )

    key = _monitor_key(scope, pod_name, container_name)

    with _cpu_monitors_lock:
        st = _cpu_monitors.get(key)
        has_last = bool(st and st.get("thread") and st["thread"].is_alive() and st.get("last") is not None)

        if mode == "last" or (mode == "" and has_last):
            if not has_last:
                return Response(
                    response=json.dumps(
                        {
                            "ok": False,
                            "error": "No running monitor / no last sample for this (scope,pod,container). Start the monitor first.",
                            "key": key,
                        }
                    ),
                    status=404,
                    mimetype="application/json",
                )
            data_sliced = _slice_util_payload(st["last"], cpus)
            resp = {"ok": True, "mode": "last", "key": key, "data": data_sliced}
            if history_n > 0:
                hist = list(st["history"])[-history_n:]
                resp["history"] = [_slice_util_payload(h, cpus) for h in hist]
            return Response(response=json.dumps(resp), status=200, mimetype="application/json")

    # on-demand sample (all cpus), then slice
    obs_all = _sample_cpu_util_all(pod_name, container_name, scope, window_s)
    obs = _slice_util_payload(obs_all, cpus)
    status = 200 if obs.get("ok") else 500
    resp = {"ok": obs.get("ok", False), "mode": "sample", "key": key, "data": obs}
    return Response(response=json.dumps(resp), status=status, mimetype="application/json")


@app.route("/api/list_cpu_monitors", methods=["GET"])
def list_cpu_monitors():
    """List currently known monitors and whether they are alive."""
    out = []
    with _cpu_monitors_lock:
        for key, st in _cpu_monitors.items():
            out.append(
                {
                    "key": key,
                    "alive": bool(st.get("thread") and st["thread"].is_alive()),
                    "scope": st.get("scope"),
                    "pod_name": st.get("pod_name"),
                    "container_name": st.get("container_name"),
                    "cpus": st.get("cpus_str"),
                    "interval_s": st.get("interval_s"),
                    "window_s": st.get("window_s"),
                    "history_len": len(st.get("history", [])),
                    "has_last": st.get("last") is not None,
                }
            )
    return Response(response=json.dumps({"ok": True, "monitors": out}), status=200, mimetype="application/json")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=4002, threaded=True)
