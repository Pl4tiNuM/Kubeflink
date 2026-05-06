"""
ExperimentOrchestrator: Main state machine for experiment execution.

States:
0. Initialize Run
1. Submit Flink Job (workload ramp starts automatically)
2. Wait for Cluster + Job Readiness
3. Apply Pod→VM Core Pinning
4. Optional DVFS Apply
5. Settle Window (4-5s after pinning/DVFS)
6. Run Experiment (Start Metric Collection - workload already running)
7. Teardown / Finalize
"""

import time
from enum import Enum, auto
from typing import Optional
from datetime import datetime

from .run_context import RunContext
from .topology import TopologyResolver
from .ticker import Ticker
from .workload_driver import WorkloadDriver
from .port_forward import PortForwardManager


class OrchestratorState(Enum):
    """Orchestrator state machine states"""
    INIT = auto()
    SUBMIT_JOB = auto()
    WAIT_READY = auto()
    APPLY_PINNING = auto()
    APPLY_DVFS = auto()
    SETTLE = auto()
    RUN_EXPERIMENT = auto()
    TEARDOWN = auto()
    COMPLETED = auto()
    ABORTED = auto()


class ExperimentOrchestrator:
    """
    Main orchestrator for Flink experiment execution.

    Manages state transitions and coordinates:
    - Job submission
    - Topology resolution
    - Pinning/DVFS application
    - Workload execution
    - Metric collection
    """

    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        self.state = OrchestratorState.INIT

        # Components (to be injected)
        self.topology_resolver: Optional[TopologyResolver] = None
        self.pinner_client = None
        self.dvfs_client = None
        self.workload_driver = None
        self.ticker: Optional[Ticker] = None

        # State tracking
        self.abort_reason: Optional[str] = None

    def execute(self):
        """
        Execute full experiment workflow.

        Returns:
            True if completed successfully, False if aborted
        """
        try:
            self._transition(OrchestratorState.INIT)
            self._state_init()

            self._transition(OrchestratorState.SUBMIT_JOB)
            if not self._state_submit_job():
                return False

            self._transition(OrchestratorState.WAIT_READY)
            if not self._state_wait_ready():
                return False

            # Apply pod-level pinning if enabled
            if self.ctx.config.pinning.enabled:
                self._transition(OrchestratorState.APPLY_PINNING)
                if not self._state_apply_pinning():
                    return False

            # Apply thread-level pinning if enabled (independent of pod pinning)
            if self.ctx.config.thread_pinning and self.ctx.config.thread_pinning.enabled:
                print("\n" + "="*80)
                print("Applying thread-level pinning...")
                print("="*80)
                print("Waiting 10s for workload to start and operator threads to spawn...")
                time.sleep(15)
                if not self._apply_thread_pinning():
                    return False

            # Apply CPU governor settings if enabled (preamble, alongside pinning)
            if self.ctx.config.governor and self.ctx.config.governor.enabled:
                print("\n" + "="*80)
                print("Applying CPU governor settings...")
                print("="*80)
                if not self._apply_governor():
                    return False

            if self.ctx.config.dvfs.enabled:
                self._transition(OrchestratorState.APPLY_DVFS)
                if not self._state_apply_dvfs():
                    return False

            self._transition(OrchestratorState.SETTLE)
            self._state_settle()

            self._transition(OrchestratorState.RUN_EXPERIMENT)
            if not self._state_run_experiment():
                return False

            self._transition(OrchestratorState.TEARDOWN)
            self._state_teardown()

            self._transition(OrchestratorState.COMPLETED)
            return True

        except KeyboardInterrupt:
            self._abort("User interrupt (Ctrl+C)")
            return False
        except Exception as e:
            self._abort(f"Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            self._finalize()

    def _transition(self, new_state: OrchestratorState):
        """Transition to new state and log"""
        self.state = new_state
        self.ctx.log_event("STATE_TRANSITION", state=new_state.name)
        print(f"\n=== State: {new_state.name} ===")

    def _abort(self, reason: str):
        """Abort experiment"""
        self.abort_reason = reason
        self.state = OrchestratorState.ABORTED
        self.ctx.log_event("RUN_ABORTED", reason=reason)
        print(f"\n!!! ABORTED: {reason} !!!")

    # ========== State Implementations ==========

    def _state_init(self):
        """State 0: Initialize Run - Setup directories, metadata, and components"""
        self.ctx.log_event("RUN_INIT", run_id=self.ctx.config.run_id)

        # Write initial metadata
        self.ctx.write_meta()

        # Initialize topology resolver
        self.topology_resolver = TopologyResolver(self.ctx)

        # Initialize workload driver
        self.workload_driver = WorkloadDriver(self.ctx.config.query_name)

        # Initialize port-forward manager (will be started after job submission)
        self.port_forward: Optional[PortForwardManager] = None
        self.prometheus_port_forward: Optional[PortForwardManager] = None

        if self.ctx.config.flink_rest_url.startswith("http://localhost"):
            # Extract port from URL
            port = 8081  # default
            if ":" in self.ctx.config.flink_rest_url.split("//")[1]:
                port_str = self.ctx.config.flink_rest_url.split(":")[-1].split("/")[0]
                try:
                    port = int(port_str)
                except:
                    pass
            self.port_forward = PortForwardManager(
                namespace=self.ctx.config.namespace,
                local_port=port
            )

        # Prometheus port-forward if URL is localhost
        if self.ctx.config.prometheus_url and self.ctx.config.prometheus_url.startswith("http://localhost"):
            # Extract port from URL
            prom_port = 9090  # default
            if ":" in self.ctx.config.prometheus_url.split("//")[1]:
                port_str = self.ctx.config.prometheus_url.split(":")[-1].split("/")[0]
                try:
                    prom_port = int(port_str)
                except:
                    pass
            self.prometheus_port_forward = PortForwardManager(
                namespace=self.ctx.config.namespace,
                local_port=prom_port
            )

        print(f"Run ID: {self.ctx.config.run_id}")
        print(f"Run dir: {self.ctx.run_dir}")

    def _state_submit_job(self) -> bool:
        """State 1: Submit Flink Job"""
        self.ctx.log_event("JOB_SUBMIT_START")

        # Build ratelist from ramp steps
        ratelist = self.workload_driver.build_ratelist(
            [(step.target_rps, step.duration_s) for step in self.ctx.config.workload.ramp_steps]
        )

        print(f"Submitting job: {self.ctx.config.query_name}")
        print(f"Ratelist: {ratelist}")

        # Submit job with extra parameters
        result = self.workload_driver.submit_job(
            ratelist=ratelist,
            extra_args=self.ctx.config.workload.extra_params,
            capture_output=True
        )

        if result["ok"]:
            self.ctx.log_event("JOB_SUBMIT_OK",
                             cluster_id=result["cluster_id"],
                             ratelist=ratelist)
            print(f"✓ Job submitted: {result['cluster_id']}")

            # Store cluster_id for later use
            self.cluster_id = result["cluster_id"]

            return True
        else:
            self.ctx.log_event("JOB_SUBMIT_FAIL",
                             error=result.get("error", "Unknown"),
                             stderr=result.get("stderr", ""))
            print(f"✗ Job submission failed: {result.get('error', 'Unknown')}")
            if "stderr" in result and result["stderr"]:
                print(f"stderr: {result['stderr']}")
            return False

    def _state_wait_ready(self) -> bool:
        """State 2: Wait for Cluster + Job Readiness"""
        self.ctx.log_event("WAIT_READY_START")

        # Get expected TM count from config
        expected_tm_count = self.ctx.config.expected_tm_count

        # Construct label selector using cluster_id from workload driver
        cluster_id = self.workload_driver.get_cluster_id()
        label_selector = f"app={cluster_id},component=taskmanager"

        print(f"Waiting for {expected_tm_count} TaskManager pods to be running...")
        print(f"Label selector: {label_selector}")

        # Step 1: Wait for TM pods to exist and be running (not necessarily ready)
        if not self._wait_for_tm_pods_running(label_selector, expected_tm_count):
            self._abort("Timeout waiting for TM pods to start")
            return False

        # Step 2: Start port-forward now that pods/service exist
        if self.port_forward and not hasattr(self, '_port_forward_started'):
            service_name = self.workload_driver.get_rest_service_name()
            print(f"Starting port-forward to {service_name}...")

            if self.port_forward.start(service_name, timeout_seconds=30):
                print(f"✓ Port-forward ready: localhost:{self.port_forward.local_port}")
                self._port_forward_started = True
            else:
                print("⚠ Warning: Port-forward failed")
                print(f"  Run manually: kubectl port-forward service/{service_name} {self.port_forward.local_port}:8081 -n {self.ctx.config.namespace}")
                # Don't abort - user might have manual port-forward running

        # Start Prometheus port-forward if configured
        if self.prometheus_port_forward and not hasattr(self, '_prometheus_port_forward_started'):
            print(f"Starting port-forward to prometheus service...")

            if self.prometheus_port_forward.start("prometheus", timeout_seconds=30, remote_port=9090):
                print(f"✓ Prometheus port-forward ready: localhost:{self.prometheus_port_forward.local_port}")
                self._prometheus_port_forward_started = True
            else:
                print("⚠ Warning: Prometheus port-forward failed")
                print(f"  Run manually: kubectl port-forward svc/prometheus {self.prometheus_port_forward.local_port}:9090 -n {self.ctx.config.namespace}")
                # Don't abort - Prometheus metrics are optional

        # Step 3: Wait for cluster and job to be fully ready
        print(f"Waiting for Flink cluster and job to be ready...")
        ready = self.topology_resolver.wait_for_ready(
            expected_tm_count=expected_tm_count,
            label_selector=label_selector,
            timeout_seconds=120
        )

        if not ready:
            self._abort("Timeout waiting for cluster readiness")
            return False

        # Resolve topology
        self.topology_resolver.resolve(label_selector=label_selector)

        self.ctx.log_event(
            "WAIT_READY_OK",
            job_id=self.ctx.job_id,
            vertex_count=len(self.ctx.vertices),
            tm_count=len(self.ctx.tm_pods)
        )

        print(f"✓ Job ID: {self.ctx.job_id}")
        print(f"✓ Vertices: {len(self.ctx.vertices)}")
        print(f"✓ TM Pods: {len(self.ctx.tm_pods)}")

        return True

    def _wait_for_tm_pods_running(self, label_selector: str, expected_count: int, timeout_seconds: int = 120) -> bool:
        """Wait for TM pods to exist and be in Running phase (not necessarily ready)"""
        from kubernetes import client, config as k8s_config

        try:
            k8s_config.load_kube_config()
        except:
            k8s_config.load_incluster_config()

        v1 = client.CoreV1Api()
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            try:
                pods = v1.list_namespaced_pod(
                    namespace=self.ctx.config.namespace,
                    label_selector=label_selector
                )

                running_count = sum(1 for pod in pods.items if pod.status.phase == "Running")

                print(f"TM pods running: {running_count}/{expected_count}")

                if running_count >= expected_count:
                    return True

            except Exception as e:
                print(f"Error checking pods: {e}")

            time.sleep(3)

        return False

    def _state_apply_pinning(self) -> bool:
        """State 3: Apply Pod→VM Core Pinning"""
        self.ctx.log_event("PIN_START")

        if not self.pinner_client:
            print("WARNING: No pinner client configured - skipping pod-level pinning")
            self.ctx.log_event("PIN_SKIPPED", reason="no_client")
            return True

        print("Applying pod→VM core pinning...")

        # Load mapping from config
        pinning_config = None

        if self.ctx.config.pinning.mapping_file:
            # Load from file (new format: {"taskmanagers": [...]})
            import json
            try:
                with open(self.ctx.config.pinning.mapping_file, 'r') as f:
                    pinning_config = json.load(f)
                print(f"Loaded pinning map from {self.ctx.config.pinning.mapping_file}")
            except Exception as e:
                self.ctx.log_event("PIN_FAILED", reason=f"Failed to load mapping file: {e}")
                print(f"✗ Failed to load pinning map: {e}")
                return False
        elif self.ctx.config.pinning.pod_core_map:
            # Use inline config (old format compatibility)
            pinning_config = {"taskmanagers": [
                {"pod_name": pod, "cores": cores}
                for pod, cores in self.ctx.config.pinning.pod_core_map.items()
            ]}

        if not pinning_config or "taskmanagers" not in pinning_config:
            print("WARNING: No pinning configuration provided - skipping pod-level pinning")
            self.ctx.log_event("PIN_SKIPPED", reason="no_mapping")
            return True

        self._apply_pod_pinning(pinning_config)
        return True

    def _apply_pod_pinning(self, pinning_config: dict) -> None:
        """Apply pod-level core pinning"""
        # Store pinning map for CPU monitoring
        self.ctx.tm_pinning_map = {}

        # Apply pinning for each TM
        success_count = 0
        for tm_config in pinning_config["taskmanagers"]:
            pod_name = tm_config["pod_name"]
            node_ip = tm_config.get("node_ip")
            cores = tm_config["cores"]

            print(f"  Pinning {pod_name} to cores {cores} on node {node_ip}...")

            try:
                # Convert cores list to string format (e.g., "0,1,2,3")
                cores_str = ",".join(map(str, cores))

                result = self.pinner_client.pin_pod_cores(
                    node_ip=node_ip,
                    pod_name=pod_name,
                    cores=cores_str
                )

                if not result.get("message", {}).get("ok", False):
                    error_msg = result.get("message", {}).get("error", "Unknown error")
                    print(f"    ✗ Failed: {error_msg}")
                    self.ctx.log_event("PIN_POD_FAILED", pod=pod_name, error=error_msg)
                else:
                    print(f"    ✓ Pinned to cores {cores_str}")
                    success_count += 1

                    # Store for CPU monitoring
                    self.ctx.tm_pinning_map[pod_name] = {
                        "node_ip": node_ip,
                        "cores": cores
                    }
            except Exception as e:
                print(f"    ✗ Exception: {e}")
                self.ctx.log_event("PIN_POD_FAILED", pod=pod_name, error=str(e))

        self.ctx.log_event("PIN_APPLIED", success_count=success_count, total_pods=len(pinning_config["taskmanagers"]))
        print(f"✓ Pinned {success_count}/{len(pinning_config['taskmanagers'])} pods")

    def _apply_thread_pinning(self) -> bool:
        """Apply thread-level pinning (called after pod pinning if enabled)"""
        self.ctx.log_event("THREAD_PIN_START")

        if not self.pinner_client:
            print("WARNING: No pinner client configured - skipping thread pinning")
            self.ctx.log_event("THREAD_PIN_SKIPPED", reason="no_client")
            return True

        tp_config = self.ctx.config.thread_pinning

        if not tp_config.policies:
            print("WARNING: No thread pinning policies defined - skipping thread pinning")
            self.ctx.log_event("THREAD_PIN_SKIPPED", reason="no_policies")
            return True

        print(f"Applying {len(tp_config.policies)} thread pinning policies...")

        # Apply each policy to all VM nodes
        total_success = 0
        total_policies_applied = 0

        for policy_idx, policy in enumerate(tp_config.policies):
            print(f"\n  Policy {policy_idx + 1}/{len(tp_config.policies)}:")
            print(f"    Pod: {policy.pod_pattern}, Thread: {policy.thread_pattern}, Cores: {policy.cores}")
            print(f"    [DEBUG] namespace_pattern: {repr(policy.namespace_pattern)}, container_pattern: {repr(policy.container_pattern)}")
            print(f"    [DEBUG] only_if_cmdline_matches: {repr(policy.only_if_cmdline_matches)}, reapply_seconds: {policy.reapply_seconds}")

            policy_success = 0
            for node_ip in self.ctx.config.vm_ips:
                try:
                    result = self.pinner_client.pin_threads_by_pattern(
                        node_ip=node_ip,
                        pod_pattern=policy.pod_pattern,
                        namespace_pattern=policy.namespace_pattern,
                        container_pattern=policy.container_pattern,
                        thread_pattern=policy.thread_pattern,
                        cores=policy.cores,
                        only_if_cmdline_matches=policy.only_if_cmdline_matches,
                        reapply_seconds=policy.reapply_seconds
                    )

                    msg = result.get("message", {})
                    if not msg.get("ok", False):
                        error_msg = msg.get("error", "Unknown error")
                        print(f"    ✗ Node {node_ip}: {error_msg}")
                        self.ctx.log_event("THREAD_PIN_POLICY_FAILED",
                                         policy_idx=policy_idx,
                                         pod_pattern=policy.pod_pattern,
                                         thread_pattern=policy.thread_pattern,
                                         node_ip=node_ip,
                                         error=error_msg)
                    else:
                        # Get detailed results from response
                        # Handle both "once" mode (results) and "reapply" mode (rounds[].results)
                        mode = msg.get("mode", "once")
                        if mode == "reapply":
                            rounds = msg.get("rounds", [])
                            results_list = rounds[-1].get("results", []) if rounds else []
                        else:
                            results_list = msg.get("results", [])

                        total_matched = sum(r.get("matched_threads", 0) for r in results_list)
                        total_pinned = sum(r.get("pinned_ok", 0) for r in results_list)
                        total_failed = sum(r.get("pinned_failed", 0) for r in results_list)
                        containers_found = len(results_list)

                        print(f"    ✓ Node {node_ip}: {containers_found} containers, {total_matched} threads matched, {total_pinned} pinned, {total_failed} failed")

                        if total_pinned == 0 and total_matched > 0:
                            print(f"      WARNING: {total_matched} threads matched but 0 pinned - check for errors")

                        policy_success += 1
                        total_success += 1
                except Exception as e:
                    print(f"    ✗ Node {node_ip}: Exception: {e}")
                    self.ctx.log_event("THREAD_PIN_POLICY_FAILED",
                                     policy_idx=policy_idx,
                                     pod_pattern=policy.pod_pattern,
                                     thread_pattern=policy.thread_pattern,
                                     node_ip=node_ip,
                                     error=str(e))

            print(f"    Policy result: {policy_success}/{len(self.ctx.config.vm_ips)} nodes")
            total_policies_applied += 1

        self.ctx.log_event("THREAD_PIN_APPLIED",
                         policies_applied=total_policies_applied,
                         total_policies=len(tp_config.policies),
                         successful_node_operations=total_success,
                         total_operations=len(tp_config.policies) * len(self.ctx.config.vm_ips))
        print(f"\n✓ Applied {total_policies_applied} thread pinning policies ({total_success} successful operations)")
        return True

    def _state_apply_dvfs(self) -> bool:
        """State 4: Apply DVFS (immediately after pinning)

        Reads vm_to_physical_cpu.json which contains mapping and per-core target frequencies.
        Expected format:
        {
          "vm_core_0": {"physical_cpu": 0, "target_freq_ghz": 2.4},
          "vm_core_1": {"physical_cpu": 1, "target_freq_ghz": 2.0},
          ...
        }
        """
        self.ctx.log_event("DVFS_START")

        if not self.dvfs_client:
            print("WARNING: No DVFS client configured - skipping DVFS")
            self.ctx.log_event("DVFS_SKIPPED", reason="no_client")
            return True

        print("Applying DVFS settings...")

        # Load per-core frequencies from mapping file
        per_core_freq = {}

        if self.ctx.config.dvfs.mapping_file:
            import json
            try:
                with open(self.ctx.config.dvfs.mapping_file, 'r') as f:
                    mapping = json.load(f)

                # Extract target frequencies for each physical CPU
                for vm_core, info in mapping.items():
                    physical_cpu = info.get('physical_cpu')
                    target_freq = info.get('target_freq_ghz')

                    if physical_cpu is not None and target_freq is not None:
                        per_core_freq[str(physical_cpu)] = target_freq

                print(f"Loaded DVFS config from {self.ctx.config.dvfs.mapping_file}")
                print(f"  Setting frequencies for {len(per_core_freq)} cores")
            except Exception as e:
                self.ctx.log_event("DVFS_FAILED", reason=f"Failed to load mapping file: {e}")
                print(f"✗ Failed to load DVFS mapping: {e}")
                return False
        elif self.ctx.config.dvfs.per_core_freq:
            # Use per_core_freq from config directly
            per_core_freq = self.ctx.config.dvfs.per_core_freq
        elif self.ctx.config.dvfs.target_freq_ghz:
            # Fallback: apply same frequency to all cores
            # Note: This requires knowing how many cores exist
            print(f"  Using uniform frequency: {self.ctx.config.dvfs.target_freq_ghz} GHz")
            # Will be applied as a batch setting
        else:
            print("WARNING: No DVFS frequency configuration provided")
            self.ctx.log_event("DVFS_SKIPPED", reason="no_config")
            return True

        # Apply frequencies
        if per_core_freq:
            # Set per-core frequencies
            for core_id, freq_ghz in per_core_freq.items():
                print(f"  Setting CPU {core_id} to {freq_ghz} GHz...")
                try:
                    result = self.dvfs_client.set_frequency(core_id=int(core_id), freq_ghz=freq_ghz)

                    if "error" in result:
                        print(f"    ✗ Failed: {result['error']}")
                        self.ctx.log_event("DVFS_CORE_FAILED", core=core_id, error=result['error'])
                    else:
                        print(f"    ✓ Set to {freq_ghz} GHz")
                except Exception as e:
                    print(f"    ✗ Exception: {e}")
                    self.ctx.log_event("DVFS_CORE_FAILED", core=core_id, error=str(e))
        elif self.ctx.config.dvfs.target_freq_ghz:
            # Apply uniform frequency (implementation depends on DvfsClient API)
            print(f"  Applying uniform frequency to all cores...")
            # This would need a set_all_frequencies or similar method

        self.ctx.log_event("DVFS_APPLIED", core_count=len(per_core_freq))
        print(f"✓ DVFS applied to {len(per_core_freq)} cores")
        return True

    def _apply_governor(self) -> bool:
        """Apply CPU governor settings (preamble phase, alongside pinning)"""
        self.ctx.log_event("GOVERNOR_START")

        if not self.dvfs_client:
            print("WARNING: No DVFS client configured - skipping governor settings")
            self.ctx.log_event("GOVERNOR_SKIPPED", reason="no_client")
            return True

        gov_cfg = self.ctx.config.governor
        if not gov_cfg or not gov_cfg.entries:
            print("WARNING: No governor entries defined - skipping")
            self.ctx.log_event("GOVERNOR_SKIPPED", reason="no_entries")
            return True

        print(f"Applying {len(gov_cfg.entries)} governor setting(s)...")
        success = 0
        for entry in gov_cfg.entries:
            print(f"  Node {entry.node_ip} cores [{entry.cores}] → '{entry.governor}'...")
            try:
                result = self.dvfs_client.set_governor(
                    node_ip=entry.node_ip, cores=entry.cores, governor=entry.governor
                )
                if not result.get("error"):
                    print(f"    ✓ Done")
                    success += 1
                else:
                    err = result.get('error', 'Unknown')
                    print(f"    ✗ Failed: {err}")
                    self.ctx.log_event("GOVERNOR_ENTRY_FAILED", node_ip=entry.node_ip,
                                      cores=entry.cores, governor=entry.governor, error=err)
            except Exception as e:
                print(f"    ✗ Exception: {e}")
                self.ctx.log_event("GOVERNOR_ENTRY_FAILED", node_ip=entry.node_ip,
                                  cores=entry.cores, governor=entry.governor, error=str(e))

        self.ctx.log_event("GOVERNOR_APPLIED", success=success, total=len(gov_cfg.entries))
        print(f"✓ Governors applied: {success}/{len(gov_cfg.entries)}")
        return True

    def _state_settle(self):
        """State 5: Settle Window (brief wait after pinning/DVFS)"""
        settle_sec = self.ctx.config.settle_seconds

        self.ctx.log_event("SETTLE_START", duration_seconds=settle_sec)

        # Start CPU monitoring before settle period
        # If pinning was applied, configure monitors for specific TM core groups
        if self.ticker:
            for scraper in self.ticker.scrapers:
                if hasattr(scraper, 'start_monitoring') and scraper.name == 'cpu_util':
                    print("Starting CPU utilization monitoring...")

                    # If we have TM pinning info, update scraper's core map to monitor only pinned cores
                    if hasattr(self.ctx, 'tm_pinning_map') and self.ctx.tm_pinning_map:
                        print("  Configuring monitors for pinned TM cores...")
                        # Group cores by node
                        node_cores = {}
                        for pod_name, pin_info in self.ctx.tm_pinning_map.items():
                            node_ip = pin_info["node_ip"]
                            cores = pin_info["cores"]
                            if node_ip not in node_cores:
                                node_cores[node_ip] = set()
                            node_cores[node_ip].update(cores)

                        # Update scraper's cpu_cores_map with union of all TM cores per node
                        scraper.cpu_cores_map = {
                            node_ip: {"cores": sorted(list(cores))}
                            for node_ip, cores in node_cores.items()
                        }

                        # Pass TM pinning map to scraper for TM-grouped output
                        scraper.tm_pinning_map = self.ctx.tm_pinning_map

                        for node_ip, cores_set in node_cores.items():
                            print(f"    {node_ip}: monitoring cores {sorted(list(cores_set))}")

                    scraper.start_monitoring()
                    # Wait 1 second for monitor to stabilize
                    time.sleep(1)
                    print("✓ CPU monitoring started")
                    break

        print(f"Settling for {settle_sec}s (allow configs to take effect)...")
        time.sleep(settle_sec)

        self.ctx.log_event("SETTLE_DONE")
        print("✓ Ready to start metric collection")

    def _check_job_status(self) -> Optional[str]:
        """
        Check Flink job status via REST API.
        
        Returns:
            Job status string (RUNNING, FINISHED, FAILED, CANCELED, etc.) or None if cannot determine
        """
        # Skip if Flink REST URL not configured
        if not self.ctx.config.flink_rest_url or not self.ctx.config.flink_rest_url.strip():
            return None
            
        try:
            import requests
            
            # Get jobs overview
            resp = requests.get(
                f"{self.ctx.config.flink_rest_url}/jobs/overview",
                timeout=5
            )
            
            if resp.status_code != 200:
                print(f"⚠ Failed to check job status: HTTP {resp.status_code}")
                return None
                
            jobs = resp.json().get("jobs", [])
            if not jobs:
                print("⚠ No jobs found in Flink cluster")
                return None
            
            # Get the most recent job (should be ours)
            latest_job = jobs[0]
            status = latest_job.get("state")
            print(f"Job status check: {status}")
            return status
            
        except Exception as e:
            print(f"⚠ Error checking job status: {e}")
            # Return None instead of RUNNING so we don't assume job is running on errors
            return None

    def _state_run_experiment(self) -> bool:
        """State 6: Run Experiment (Start Metric Collection)

        At this point:
        - Job is running with workload ramp already in progress (started at submission)
        - Pinning and DVFS have been applied
        - We now start collecting metrics and wait for ramp completion
        """
        self.ctx.log_event("EXPERIMENT_START")
        self.ctx.started_at = datetime.utcnow().isoformat() + "Z"

        if not self.ticker:
            self._abort("No ticker configured")
            return False

        # Start ticker for metric collection
        print("Starting metric collection...")
        print("(Note: Workload ramp started automatically with job submission)")
        self.ticker.start()

        # Calculate total ramp duration
        if self.ctx.config.workload.ramp_steps:
            total_duration = sum(step.duration_s for step in self.ctx.config.workload.ramp_steps)
            print(f"Collecting metrics for up to {total_duration}s (ramp duration)...")
            print("Monitoring job status (will stop early if job finishes or fails)...")
            
            # Monitor job status and stop early if job completes or fails
            start_time = time.time()
            check_interval = 10  # Check every 10 seconds
            consecutive_failures = 0
            max_consecutive_failures = 3  # Stop after 3 failed checks in a row
            
            while time.time() - start_time < total_duration:
                # Check job status (only if Flink REST URL configured)
                if self.ctx.config.flink_rest_url and self.ctx.config.flink_rest_url.strip():
                    job_status = self._check_job_status()
                    
                    if job_status and job_status in ["FINISHED", "FAILED", "CANCELED"]:
                        elapsed = int(time.time() - start_time)
                        print(f"✓ Job status: {job_status} after {elapsed}s (stopped monitoring early)")
                        self.ctx.log_event("JOB_STATUS_CHANGE", status=job_status, elapsed_s=elapsed)
                        break
                    elif job_status == "RUNNING":
                        consecutive_failures = 0  # Reset counter on successful check
                    elif job_status is None:
                        consecutive_failures += 1
                        print(f"⚠ Failed to check job status ({consecutive_failures}/{max_consecutive_failures})")
                        if consecutive_failures >= max_consecutive_failures:
                            elapsed = int(time.time() - start_time)
                            print(f"✓ Stopping monitoring after {consecutive_failures} failed status checks (job likely finished)")
                            self.ctx.log_event("JOB_STATUS_UNKNOWN", elapsed_s=elapsed, reason="consecutive_failures")
                            break
                
                # Sleep until next check or end of duration
                remaining = total_duration - (time.time() - start_time)
                sleep_time = min(check_interval, remaining)
                if sleep_time > 0:
                    time.sleep(sleep_time)
        else:
            # No workload config - run ticker for a fixed duration
            duration = 60  # Default 60s
            print(f"Running ticker for {duration}s...")
            time.sleep(duration)

        self.ctx.log_event("EXPERIMENT_END")
        return True

    def _state_teardown(self):
        """State 8: Teardown / Finalize"""
        self.ctx.log_event("TEARDOWN_START")

        # Stop ticker FIRST before stopping port-forwards
        # This prevents scrapers from failing when port-forwards are terminated
        if self.ticker:
            print("Stopping metric collection...")
            self.ticker.stop()

        # Stop CPU monitoring if active
        if self.ticker:
            for scraper in self.ticker.scrapers:
                if hasattr(scraper, 'stop_monitoring') and scraper.name == 'cpu_util':
                    print("Stopping CPU utilization monitoring...")
                    scraper.stop_monitoring()
                    break

        # Stop workload driver
        if self.workload_driver:
            print("Stopping workload driver...")
            # TODO: Stop workload driver

        # Delete Flink deployment
        if self.workload_driver:
            print("Deleting Flink deployment...")
            cluster_id = self.workload_driver.get_cluster_id()
            try:
                import subprocess
                result = subprocess.run(
                    ["kubectl", "delete", "deployment", cluster_id, "-n", self.ctx.config.namespace],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.returncode == 0:
                    print(f"✓ Deleted deployment: {cluster_id}")
                else:
                    print(f"⚠ Failed to delete deployment: {result.stderr.strip()}")
            except Exception as e:
                print(f"⚠ Error deleting deployment: {e}")

        # Stop port-forward
        if self.port_forward:
            print("Stopping Flink port-forward...")
            self.port_forward.stop()

        if self.prometheus_port_forward:
            print("Stopping Prometheus port-forward...")
            self.prometheus_port_forward.stop()

        self.ctx.log_event("TEARDOWN_DONE")

    def _finalize(self):
        """Final cleanup and summary"""
        self.ctx.completed_at = datetime.utcnow().isoformat() + "Z"

        # Write final metadata
        self.ctx.write_meta()

        # Log completion
        if self.state == OrchestratorState.COMPLETED:
            self.ctx.log_event("RUN_COMPLETE", status="success")
            print(f"\n✓ Run completed: {self.ctx.config.run_id}")
        elif self.state == OrchestratorState.ABORTED:
            self.ctx.log_event("RUN_COMPLETE", status="aborted", reason=self.abort_reason)
            print(f"\n✗ Run aborted: {self.abort_reason}")

        # Close context
        self.ctx.close()

        print(f"Results: {self.ctx.run_dir}")
