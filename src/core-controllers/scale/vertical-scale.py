import sys
import argparse
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# NOTE: The parse_resource_value function from the previous script is necessary 
# for the validation checks, but is omitted here for brevity. 
# Assume it is present and correctly handles 'm', 'Gi', and 'Mi' units for comparison.
import re
import sys

def parse_resource_value(value: str):
    """
    Converts Kubernetes resource strings (CPU/Memory) into a comparable float (for CPU) 
    or integer (for Memory in MiB). Handles 'm', 'i', and standard suffixes.
    """
    if value is None:
        return None
    
    # Use regex to split the value into number and unit (e.g., "1.5" and "Gi")
    match = re.match(r"([0-9.]+)([EPTGMK]i?|m?)", value.strip(), re.IGNORECASE)
    
    if not match:
        # If no match, try to convert the whole string to a number (CPU core or raw bytes)
        try:
            # Assume it's a raw CPU core value or raw bytes
            return float(value.strip())
        except ValueError:
            print(f"🚨 Error: Cannot parse resource value '{value}'. Unknown format.")
            return None

    number_part = float(match.group(1))
    unit_part = match.group(2).lower()
    
    # --- CPU Parsing ---
    if not unit_part or unit_part == 'm':
        # 'm' for millicores, or no unit (defaults to full cores)
        if unit_part == 'm':
            return number_part / 1000.0  # Convert millicores to full cores
        else:
            return number_part # Full cores
            
    # --- Memory Parsing (Convert everything to MiB for comparison) ---
    
    # Factors for binary units (powers of 1024)
    # 1Ki = 1024 bytes, 1Mi = 1024 Ki, 1Gi = 1024 Mi
    
    if unit_part == 'ei': return int(number_part * 1024**6 / 1024**2)
    if unit_part == 'pi': return int(number_part * 1024**5 / 1024**2)
    if unit_part == 'ti': return int(number_part * 1024**4 / 1024**2)
    if unit_part == 'gi': return int(number_part * 1024**3 / 1024**2) # Convert GiB to MiB
    if unit_part == 'mi': return int(number_part * 1024**2 / 1024**2) # Already MiB
    if unit_part == 'ki': return int(number_part * 1024 / 1024**2)  # Convert KiB to MiB
    
    # Factors for decimal units (powers of 1000) - less common for memory limits, but good practice
    if unit_part == 'e': return int(number_part * 1000**6 / 1024**2)
    if unit_part == 'p': return int(number_part * 1000**5 / 1024**2)
    if unit_part == 't': return int(number_part * 1000**4 / 1024**2)
    if unit_part == 'g': return int(number_part * 1000**3 / 1024**2)
    if unit_part == 'm': return int(number_part * 1000**2 / 1024**2)
    if unit_part == 'k': return int(number_part * 1000 / 1024**2)

    # Raw bytes (no unit) - Kubernetes default is bytes if no unit is specified
    # The regex should have caught this as a raw number, but as a safeguard:
    # Convert raw bytes to MiB for comparison.
    try:
        # If the unit part is empty, and it was a large integer (like 1728i was intended to be):
        # We assume it was read as a raw number of bytes, and convert to MiB.
        if not unit_part:
            return int(number_part / (1024 * 1024))
    except Exception:
        pass
    
    print(f"🚨 Error: Cannot determine unit for resource value '{value}'.")
    return None

def patch_pod_resources_inplace(
    pod_name: str,
    container_name: str,
    namespace: str,
    new_cpu_request: str = None,
    new_cpu_limit: str = None,
    new_memory_request: str = None,
    new_memory_limit: str = None
):
    """
    Performs an in-place vertical scaling on a specific Kubernetes Pod container 
    using the experimental 'resize' subresource patch method.
    """
    
    print("Loading Kubernetes configuration...")
    try:
        config.load_kube_config()
    except Exception:
        try:
            config.load_incluster_config()
        except Exception as e_incluster:
            print(f"🚨 Error loading Kubernetes configuration: {e_incluster}")
            sys.exit(1)
            
    api = client.CoreV1Api()
    
    try:
        # 1. Get the current Pod configuration
        pod = api.read_namespaced_pod(name=pod_name, namespace=namespace)
        target_container = next(
            (c for c in pod.spec.containers if c.name == container_name), 
            None
        )
        
        if not target_container:
            print(f"🚨 Container '{container_name}' not found in Pod '{pod_name}'. Exiting.")
            return

        # 2. Extract current and determine effective desired values
        current_resources = target_container.resources or client.V1ResourceRequirements()
        current_requests = current_resources.requests or {}
        current_limits = current_resources.limits or {}
        
        # Determine the effective values for the patch
        effective_cpu_req = new_cpu_request if new_cpu_request is not None else current_requests.get("cpu")
        effective_cpu_lim = new_cpu_limit if new_cpu_limit is not None else current_limits.get("cpu")
        effective_mem_req = new_memory_request if new_memory_request is not None else current_requests.get("memory")
        effective_mem_lim = new_memory_limit if new_memory_limit is not None else current_limits.get("memory")

        new_resources = {"requests": {}, "limits": {}}

        # 3. Perform consistency and validation checks (requests <= limits)
        
        # CPU Checks
        current_cpu_req_val = parse_resource_value(current_requests.get("cpu"))
        current_cpu_lim_val = parse_resource_value(current_limits.get("cpu"))

        # Consistency Check: requests == limits initially (forcing match if only one is updated)
        if current_cpu_req_val is not None and current_cpu_lim_val is not None and \
           current_cpu_req_val == current_cpu_lim_val:
            if new_cpu_request is None and new_cpu_limit is not None: effective_cpu_req = new_cpu_limit
            elif new_cpu_limit is None and new_cpu_request is not None: effective_cpu_lim = new_cpu_request

        # Validation Check: requests <= limits
        if effective_cpu_req is not None and effective_cpu_lim is not None:
            if parse_resource_value(effective_cpu_req) > parse_resource_value(effective_cpu_lim):
                print(f"❌ Validation failed for CPU: Request ({effective_cpu_req}) must be <= Limit ({effective_cpu_lim}). Exiting.")
                sys.exit(1)

        # Memory Checks
        current_mem_req_val = parse_resource_value(current_requests.get("memory"))
        current_mem_lim_val = parse_resource_value(current_limits.get("memory"))

        # Consistency Check: requests == limits initially (forcing match if only one is updated)
        if current_mem_req_val is not None and current_mem_lim_val is not None and \
           current_mem_req_val == current_mem_lim_val:
            if new_memory_request is None and new_memory_limit is not None: effective_mem_req = new_memory_limit
            elif new_memory_limit is None and new_memory_request is not None: effective_mem_lim = new_memory_request
        
        # Validation Check: requests <= limits
        if effective_mem_req is not None and effective_mem_lim is not None:
            if parse_resource_value(effective_mem_req) > parse_resource_value(effective_mem_lim):
                print(f"❌ Validation failed for Memory: Request ({effective_mem_req}) must be <= Limit ({effective_mem_lim}). Exiting.")
                sys.exit(1)

        # 4. Build the final patch object for the 'resize' subresource
        if effective_cpu_req:
            new_resources["requests"]["cpu"] = effective_cpu_req
            print(f"➡️ Container '{container_name}': Setting CPU Request to {effective_cpu_req}")
        if effective_cpu_lim:
            new_resources["limits"]["cpu"] = effective_cpu_lim
            print(f"➡️ Container '{container_name}': Setting CPU Limit to {effective_cpu_lim}")
        if effective_mem_req:
            new_resources["requests"]["memory"] = effective_mem_req
            print(f"➡️ Container '{container_name}': Setting Memory Request to {effective_mem_req}")
        if effective_mem_lim:
            new_resources["limits"]["memory"] = effective_mem_lim
            print(f"➡️ Container '{container_name}': Setting Memory Limit to {effective_mem_lim}")
            
        patch = {
            "spec": {
                "containers": [
                    {
                        "name": container_name,
                        "resources": new_resources
                    }
                ]
            }
        }
        
        # 5. Apply the patch to the 'resize' subresource
        print(f"\nApplying patch to Pod '{pod_name}' using 'resize' subresource...")
        
        # Note: The Kubernetes Python client does not have a native method for 
        # subresources like 'resize'. We must use the generic HTTP request method.
        api_path = f"/api/v1/namespaces/{namespace}/pods/{pod_name}/resize"
        
        # The correct content type for this patch is 'application/json-patch+json' for JSON Patch
        # but for simplicity and consistency with the kubectl --type=merge, we use 'application/merge-patch+json'
        # The CoreV1Api.patch_namespaced_pod method handles this URL path correctly with the right content type.
        
        # We simulate the HTTP request directly as the client library methods don't expose subresource patching easily
        # NOTE: If your K8s client version is older, this might fail.
        response_data, status_code, headers = api.api_client.call_api(
            api_path, 'PATCH',
            path_params={'name': pod_name, 'namespace': namespace},
            header_params={'Content-Type': 'application/merge-patch+json'},
            body=patch,
            auth_settings=['BearerToken'],
            _preload_content=False
        )
        
        # Check for success (HTTP 200/202)
        if status_code not in [200, 202]:
            raise ApiException(
                            status=status_code, 
                            reason=f"Failed to patch pod resize subresource. Status: {status_code}",
                            headers=headers,
                            body=response_data.read().decode('utf-8') # Read the body for logging
             ) 
        print(f"✅ Successfully submitted resize patch for Pod '{pod_name}'. Monitor the Pod for resource change.")

    except ApiException as e:
        print(f"🚨 Error patching pod (Status: {e.status}): {e.reason}")
        print("💡 Ensure your Kubernetes cluster version (v1.28+) supports In-Place Vertical Scaling.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

# --- Argument Parsing ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Perform **in-place** vertical scaling on a specific container in a Kubernetes **Pod** using the 'resize' subresource.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    # Required Arguments
    parser.add_argument(
        "-p", "--pod-name", 
        required=True, 
        help="The name of the Kubernetes Pod (e.g., 'flink-taskmanager-864c4c89-abcde')."
    )
    parser.add_argument(
        "-c", "--container-name", 
        required=True, 
        help="The name of the container within the Pod to scale (e.g., 'flink-main-container')."
    )
    parser.add_argument(
        "-n", "--namespace", 
        default="default", 
        help="The Kubernetes namespace (defaults to 'default')."
    )
    
    # Optional Resource Arguments (same as before)
    parser.add_argument("--cpu-request", default=None, help="New CPU request (e.g., '1.5' or '500m').")
    parser.add_argument("--cpu-limit", default=None, help="New CPU limit (e.g., '3').")
    parser.add_argument("--memory-request", default=None, help="New Memory request (e.g., '3Gi' or '3072Mi').")
    parser.add_argument("--memory-limit", default=None, help="New Memory limit (e.g., '6Gi').")

    args = parser.parse_args()

    # Call the main function with parsed arguments
    patch_pod_resources_inplace(
        pod_name=args.pod_name,
        container_name=args.container_name,
        namespace=args.namespace,
        new_cpu_request=args.cpu_request,
        new_cpu_limit=args.cpu_limit,
        new_memory_request=args.memory_request,
        new_memory_limit=args.memory_limit
    )