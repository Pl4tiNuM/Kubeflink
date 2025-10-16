from flask import Flask, request, Response
import subprocess
import time
import json
import os
import re

app = Flask(__name__)

# --- Cgroup Utility Functions ---

def get_cgroup_version(pid: int) -> int:
    """Checks the cgroup file for the PID to determine if it's V1 or V2."""
    try:
        with open(f"/proc/{pid}/cgroup") as f:
            content = f.read().strip()
        
        # V2 is characterized by a single line starting with '0::'
        if '\n' not in content and content.startswith('0::'):
            return 2
        
        # V1 is characterized by multiple lines with numbered hierarchies (e.g., '1:name:path')
        return 1
    except FileNotFoundError:
        return 0 # Error state or PID doesn't exist

def get_cgroup_path_from_pid(pid: int, controller: str) -> tuple[str, int] or tuple[None, int]:
    """
    Finds the host cgroup path for a given PID and controller (e.g., 'cpuset' or 'cpu').
    Returns the path and the detected cgroup version (1 or 2).
    """
    version = get_cgroup_version(pid)
    
    if version == 0:
        return None, 0

    try:
        with open(f"/proc/{pid}/cgroup") as f:
            for line in f:
                line = line.strip()
                
                if version == 1:
                    # Cgroup V1 Logic: Look for the controller name in the list (e.g., :cpuset:)
                    if f":{controller}:" in line or f":{controller}," in line:
                        # Extract the relative path (last field)
                        rel_path = line.split(':')[-1]
                        # V1 paths are mounted under a specific controller name
                        return f"/sys/fs/cgroup/{controller}{rel_path}", 1

                elif version == 2:
                    # Cgroup V2 Logic: Single unified path (e.g., 0::/kubepods/...)
                    if line.startswith("0::"):
                        # Extract the relative path (everything after 0::)
                        rel_path = line.split('0::')[1]
                        # V2 is mounted under the unified root
                        return f"/sys/fs/cgroup{rel_path}", 2
                        
    except FileNotFoundError:
        return None, 0 # Should not happen if PID was found

    # V1 case: If the specific controller wasn't found in a merged hierarchy
    return None, version

# --- Crictl Utility Function (FIXED: Removed 'sudo') ---

def run_crictl_command(cmd: list) -> str or None:
    # Removed 'sudo' as the container is running in privileged mode.
    # We rely on the crictl binary being in the PATH (like /usr/bin)
    full_cmd = ['crictl'] + cmd 
    try:
        # Ensure common binary paths are in the environment for subprocess.
        result = subprocess.run(
            full_cmd, 
            capture_output=True, 
            text=True, 
            check=True, 
            timeout=10,
            env={'PATH': os.environ.get('PATH', '') + ':/usr/local/bin:/usr/bin'}
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        # Crictl command failed (e.g., pod not found)
        print(f"🚨 Crictl command failed: {' '.join(full_cmd)}")
        print(f"  Stderr: {e.stderr.strip()}")
        return None
    except subprocess.TimeoutExpired:
        print(f"🚨 Crictl command timed out: {' '.join(full_cmd)}")
        return None
    except FileNotFoundError:
        # This occurs if the operating system cannot find the 'crictl' binary in the PATH.
        print(f"🚨 Crictl command not found. Ensure crictl is accessible in the container's PATH via {os.environ.get('PATH', 'default path')}")
        return None


def get_pid_from_pod_container(pod_name: str, container_name: str = None) -> int or None:
    """Finds the host PID of the container's main process using crictl and Kubernetes metadata."""
    
    # 1. Find Pod ID (crictl pods --name <pod_name> -q)
    print(f"1. Attempting to find Pod ID for name: {pod_name}")
    pod_id_output = run_crictl_command(['pods', '--name', pod_name, '-q'])
    if not pod_id_output:
        print(f"Pod '{pod_name}' not found via crictl pods.")
        return None
    
    pod_id = pod_id_output.split('\n')[0]
    print(f"-> Found Pod ID: {pod_id}")

    # 2. Find Container ID (crictl ps --pod <pod_id> [--name <container_name>] -q)
    ps_cmd = ['ps', '--pod', pod_id, '-q']
    if container_name:
        ps_cmd.extend(['--name', container_name])
    
    container_id = None
    max_retries = 5
    print(f"2. Searching for Container ID (Name: {container_name or 'any'}) in Pod ID {pod_id}")
    
    for attempt in range(max_retries):
        container_ids_output = run_crictl_command(ps_cmd)
        
        if container_ids_output:
            container_id = container_ids_output.split('\n')[0]
            print(f"-> Found Container ID: {container_id}")
            break
        
        if attempt < max_retries - 1:
            print(f"   Container not found yet. Retrying in 1s (Attempt {attempt + 1}/{max_retries})...")
            time.sleep(1)
            
    if not container_id:
        print(f"Container not found in Pod '{pod_name}' after {max_retries} attempts.")
        return None

    # 3. Get PID from Container ID (crictl inspect --output json <container_id>)
    print(f"3. Inspecting Container ID {container_id} to get PID.")
    inspect_output = run_crictl_command(['inspect', '--output', 'json', container_id])
    if not inspect_output:
        print(f"🚨 Failed to inspect container {container_id}.")
        return None
        
    try:
        inspect_data = json.loads(inspect_output)
        pid = inspect_data['info']['pid']
        print(f"-> Found Host PID: {pid}")
        return int(pid)
    except (json.JSONDecodeError, KeyError) as e:
        print(f"🚨 Error parsing crictl inspect output for container {container_id}: {e}")
        return None

# --- Endpoints (Updated for Cgroup V1 and V2) ---

@app.route('/api/set_cgroup_quota', methods=['POST'])
def set_cgroup_quota():
    """Sets CPU CFS quota (V1) or cpu.max (V2) for a container."""
    start = time.time()
    try:
        data = request.get_json()
        quota_pct = data.get('quota_pct', None)
        period_us = data.get('period_us', 100_000)
        pod_name = data['pod_name']
        container_name = data.get('container_name')
    except Exception as e:
        return Response(response=json.dumps(f"Invalid request data: {e}"),
                        status=400, mimetype='application/json')
    
    if quota_pct is not None:
        try:
            quota_us = int(period_us * float(quota_pct) / 100.0)
        except ValueError:
            return Response(response=json.dumps(f"Invalid quota_pct: {quota_pct}"),
                            status=400, mimetype='application/json')
    else:
        quota_us = data.get('quota_us', None)
        if quota_us is None:
            return Response(response=json.dumps(
                "Must provide either 'quota_pct' or 'quota_us'"), 
                status=400, mimetype='application/json')

    # Find PID and Cgroup Path
    pid = get_pid_from_pod_container(pod_name, container_name)
    if not pid:
        return Response(response=json.dumps(f"Container not found for Pod '{pod_name}'"), status=404, mimetype='application/json')

    cpu_path, version = get_cgroup_path_from_pid(pid, 'cpu')

    if not cpu_path or not os.path.exists(cpu_path):
        res = f"Could not find cpu cgroup path for PID {pid} (V{version})"
        return Response(response=json.dumps(res), status=500, mimetype='application/json')

    try:
        if version == 1:
            # Cgroup V1: Uses cpu.cfs_period_us and cpu.cfs_quota_us
            with open(os.path.join(cpu_path, 'cpu.cfs_period_us'), 'w') as f:
                f.write(str(period_us))

            with open(os.path.join(cpu_path, 'cpu.cfs_quota_us'), 'w') as f:
                f.write(str(quota_us))
            
            file_names = "cpu.cfs_period_us and cpu.cfs_quota_us"

        elif version == 2:
            # Cgroup V2: Uses the unified cpu.max file (quota period)
            # 'max' means unlimited quota, so we write quota_us period_us
            cpu_max_value = f"{quota_us} {period_us}" 
            
            with open(os.path.join(cpu_path, 'cpu.max'), 'w') as f:
                f.write(cpu_max_value)
            
            file_names = "cpu.max"
            
        else:
            raise Exception("Unsupported Cgroup version.")

        elapsed = time.time() - start
        res = (f"Set CPU quota (Cgroup V{version}): period={period_us}µs, "
               f"quota={quota_us}µs ({quota_pct or 'custom'}%) via {file_names} "
               f"on PID {pid} in {elapsed:.2f}s")
        status = 200

    except Exception as e:
        res = f"Error setting CPU quota: {e}. Check if running with root/sudo privileges."
        status = 500

    return Response(response=json.dumps(res), status=status, mimetype='application/json')


@app.route('/api/pin_pod_cores', methods=['POST'])
def pin_pod_cores():
    """Pins the container belonging to a specific Kubernetes Pod to defined CPU cores."""
    start = time.time()
    
    try:
        data = request.get_json()
        pod_name = data['pod_name']
        cores = data['cores']
        container_name = data.get('container_name')
    except Exception as e:
        return Response(response=json.dumps(f"Invalid request data: {e}"),
                        status=400, mimetype='application/json')

    # Find PID and Cgroup Path
    pid = get_pid_from_pod_container(pod_name, container_name)
    if not pid:
        return Response(response=json.dumps(f"Container not found for Pod '{pod_name}'"), status=404, mimetype='application/json')

    cpuset_path, version = get_cgroup_path_from_pid(pid, 'cpuset')
    
    if not cpuset_path or not os.path.exists(cpuset_path):
        res = f"Could not find cpuset cgroup path for PID {pid} (V{version})"
        return Response(response=json.dumps(res), status=500, mimetype='application/json')

    # Debug print added to show the exact path found
    print(f"DEBUG: Found Cgroup V{version} CPUSet path for PID {pid}: {cpuset_path}")

    try:
        # The file name is 'cpuset.cpus' for both V1 and V2
        cpuset_file = os.path.join(cpuset_path, 'cpuset.cpus')

        with open(cpuset_file, 'w') as f:
            f.write(cores)

        elapsed = time.time() - start
        res = (f"Successfully pinned Pod '{pod_name}' (PID: {pid}) to cores {cores} via cgroup path {cpuset_path} "
               f"in {elapsed:.2f}s (Cgroup V{version})")
        status = 200

    except Exception as e:
        res = f"Error pinning cores: {e}. Check if running with root/sudo privileges."
        status = 500

    return Response(response=json.dumps(res), status=status, mimetype='application/json')


if __name__ == '__main__':
    # Start on all interfaces on port 4002, matching the DaemonSet hostPort
    app.run(host='0.0.0.0', port=4002)
