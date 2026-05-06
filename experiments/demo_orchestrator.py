"""
End-to-End Orchestrator Demo

This script demonstrates a complete experiment run using the orchestrator.

Prerequisites:
1. Flink cluster accessible via port-forward or NodePort
2. DVFS and Pinner agents running on cluster nodes
3. Configuration files prepared (see config_examples/)

Usage:
    python demo_orchestrator.py --config config_examples/experiment1.json
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

# Add orchestrator to path
sys.path.insert(0, str(Path(__file__).parent))

from orchestrator import (
    RunContext, RunConfig,
    RampStep, PinningConfig, ThreadPinningConfig, ThreadPinningPolicy,
    GovernorEntry, GovernorConfig, DvfsConfig, WorkloadConfig,
    ExperimentOrchestrator,
    FlinkRestScraper, PowerScraper, CpuUtilScraper, PrometheusScraper, FrequencyScraper,
    Ticker, KubeTopologyResolver,
    PinnerClient, DvfsClient, WorkloadDriver,
    generate_run_id
)


def load_config(config_path: str) -> RunConfig:
    """
    Load experiment configuration from JSON file.

    Expected format: see config_examples/experiment_template.json
    """
    with open(config_path, 'r') as f:
        config_dict = json.load(f)

    # Parse ramp steps
    ramp_steps = [
        RampStep(
            step_idx=step['step_idx'],
            target_rps=step['target_rps'],
            duration_s=step['duration_s']
        )
        for step in config_dict.get('workload', {}).get('ramp_steps', [])
    ]

    # Parse pinning config
    pinning = PinningConfig(
        enabled=config_dict.get('pinning', {}).get('enabled', False),
        mapping_source=config_dict.get('pinning', {}).get('mapping_source', 'file'),
        mapping_file=config_dict.get('pinning', {}).get('mapping_file')
    )

    # Parse thread pinning config
    tp_policies = [
        ThreadPinningPolicy(
            thread_pattern=policy['thread_pattern'],
            cores=policy['cores'],
            pod_pattern=policy.get('pod_pattern'),
            namespace_pattern=policy.get('namespace_pattern'),
            container_pattern=policy.get('container_pattern'),
            only_if_cmdline_matches=policy.get('only_if_cmdline_matches'),
            reapply_seconds=policy.get('reapply_seconds')
        )
        for policy in config_dict.get('thread_pinning', {}).get('policies', [])
    ]

    thread_pinning = ThreadPinningConfig(
        enabled=config_dict.get('thread_pinning', {}).get('enabled', False),
        policies=tp_policies
    )

    # Parse governor config (independent of frequency DVFS)
    governor_entries = [
        GovernorEntry(
            node_ip=e['node_ip'],
            cores=e['cores'],
            governor=e['governor']
        )
        for e in config_dict.get('governor', {}).get('entries', [])
    ]
    governor = GovernorConfig(
        enabled=config_dict.get('governor', {}).get('enabled', False),
        entries=governor_entries
    )

    # Parse DVFS frequency config
    dvfs = DvfsConfig(
        enabled=config_dict.get('dvfs', {}).get('enabled', False),
        mapping_file=config_dict.get('dvfs', {}).get('mapping_file'),
        target_freq_ghz=config_dict.get('dvfs', {}).get('target_freq_ghz')
    )

    # Parse workload config
    workload = WorkloadConfig(
        generator_type=config_dict.get('workload', {}).get('generator_type', 'nexmark'),
        endpoint=config_dict.get('workload', {}).get('endpoint'),
        ramp_steps=ramp_steps,
        extra_params=config_dict.get('workload', {}).get('extra_params', {})
    )

    # Generate run_id if not provided
    run_id = config_dict.get('run_id')
    if not run_id:
        run_id = generate_run_id(config_dict['query_name'])

    # Create RunConfig
    config = RunConfig(
        run_id=run_id,
        query_name=config_dict['query_name'],
        namespace=config_dict.get('namespace', 'default'),
        flink_rest_url=config_dict['flink_rest_url'],
        expected_tm_count=config_dict.get('expected_tm_count', 3),
        vm_ips=config_dict.get('vm_ips', []),
        cpu_cores_file=config_dict.get('cpu_cores_file'),
        physical_node_ips=config_dict.get('physical_node_ips', []),
        power_socket=config_dict.get('power_socket'),
        frequency_configs=config_dict.get('frequency_configs'),
        prometheus_url=config_dict.get('prometheus_url'),
        tick_seconds=config_dict.get('tick_seconds', 5),
        settle_seconds=config_dict.get('settle_seconds', 5),
        pinning=pinning,
        thread_pinning=thread_pinning,
        governor=governor,
        dvfs=dvfs,
        workload=workload,
        git_commit=config_dict.get('git_commit'),
        image_tag=config_dict.get('image_tag')
    )

    return config


def setup_scrapers(ctx: RunContext) -> list:
    """
    Create and configure scrapers based on configuration.
    """
    scrapers = []

    # Add Flink REST scraper if URL provided
    if ctx.config.flink_rest_url and ctx.config.flink_rest_url.strip():
        flink_scraper = FlinkRestScraper(
            flink_rest_url=ctx.config.flink_rest_url,
            collect_task_metrics=True,
            collect_vertex_metrics=True,
            read_timeout=30
        )
        scrapers.append(flink_scraper)
        print(f"✓ Configured FlinkRestScraper: {ctx.config.flink_rest_url}")
    else:
        print("⊘ FlinkRestScraper disabled (no URL configured)")

    # Add Power scraper if physical node IPs provided
    if ctx.config.physical_node_ips:
        power_scraper = PowerScraper(
            node_ips=ctx.config.physical_node_ips,
            port=4002,
            socket=ctx.config.power_socket
        )
        scrapers.append(power_scraper)
        socket_info = f" (socket: {ctx.config.power_socket})" if ctx.config.power_socket else " (total)"
        print(f"✓ Configured PowerScraper: {len(ctx.config.physical_node_ips)} physical nodes{socket_info}")

    # Add Frequency scraper if frequency_configs provided
    if ctx.config.frequency_configs:
        freq_scraper = FrequencyScraper(
            node_configs=ctx.config.frequency_configs,
            port=4002
        )
        scrapers.append(freq_scraper)
        print(f"✓ Configured FrequencyScraper: {len(ctx.config.frequency_configs)} nodes")

    # Add CPU util scraper if VM IPs provided
    if ctx.config.vm_ips:
        # Load CPU cores configuration if provided
        cpu_cores_map = None
        if ctx.config.cpu_cores_file:
            cores_path = Path(ctx.config.cpu_cores_file)
            if not cores_path.is_absolute():
                # Resolve relative to config file location if possible, or current directory
                cores_path = Path.cwd() / cores_path

            if cores_path.exists():
                with open(cores_path) as f:
                    cpu_cores_map = json.load(f)
                print(f"✓ Loaded CPU cores config from {ctx.config.cpu_cores_file}")
            else:
                print(f"⚠ CPU cores file not found: {cores_path}, monitoring all CPUs")

        cpu_scraper = CpuUtilScraper(
            node_ips=ctx.config.vm_ips,
            port=4002,
            scope='host',
            mode='last',
            cpu_cores_map=cpu_cores_map
        )
        scrapers.append(cpu_scraper)
        print(f"✓ Configured CpuUtilScraper: {len(ctx.config.vm_ips)} VMs")

    # Add Prometheus scraper if URL provided
    if ctx.config.prometheus_url:
        prom_scraper = PrometheusScraper(
            prometheus_url=ctx.config.prometheus_url,
            lookback_seconds=ctx.config.tick_seconds
        )
        scrapers.append(prom_scraper)
        print(f"✓ Configured PrometheusScraper: {ctx.config.prometheus_url}")

    return scrapers


def main():
    parser = argparse.ArgumentParser(
        description="Run end-to-end Flink experiment with orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--config',
        required=True,
        help='Path to experiment configuration JSON file'
    )

    parser.add_argument(
        '--runs-dir',
        default='runs',
        help='Base directory for run outputs (default: runs/)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate configuration without running experiment'
    )

    args = parser.parse_args()

    # Load configuration
    print("="*60)
    print("Loading configuration...")
    print("="*60)

    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"✗ Failed to load config: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print(f"Run ID: {config.run_id}")
    print(f"Query: {config.query_name}")
    print(f"Namespace: {config.namespace}")
    print(f"Expected TM count: {config.expected_tm_count}")
    print(f"Tick interval: {config.tick_seconds}s")
    print(f"Settle time: {config.settle_seconds}s")
    print(f"Pinning enabled: {config.pinning.enabled}")
    print(f"Thread pinning enabled: {config.thread_pinning.enabled}")
    print(f"Governor enabled: {config.governor.enabled} ({len(config.governor.entries)} entries)")
    print(f"DVFS (freq) enabled: {config.dvfs.enabled}")
    print(f"VM IPs (Pinner): {config.vm_ips}")
    print(f"Physical Node IPs (DVFS): {config.physical_node_ips}")
    print(f"Ramp steps: {len(config.workload.ramp_steps)}")

    if args.dry_run:
        print("\n✓ Configuration valid (dry-run mode)")
        sys.exit(0)

    # Create run context
    print("\n" + "="*60)
    print("Initializing run context...")
    print("="*60)

    ctx = RunContext(config, base_runs_dir=Path(args.runs_dir))
    print(f"Run directory: {ctx.run_dir}")

    # Setup scrapers
    print("\n" + "="*60)
    print("Configuring scrapers...")
    print("="*60)

    scrapers = setup_scrapers(ctx)

    # Create ticker
    ticker = Ticker(
        ctx=ctx,
        scrapers=scrapers,
        tick_seconds=ctx.config.tick_seconds
    )

    # Create orchestrator
    print("\n" + "="*60)
    print("Creating orchestrator...")
    print("="*60)

    orchestrator = ExperimentOrchestrator(ctx)

    # Inject components
    orchestrator.topology_resolver = KubeTopologyResolver(ctx)
    orchestrator.workload_driver = WorkloadDriver(config.query_name)
    orchestrator.ticker = ticker

    # Setup Pinner client if pod-level or thread-level pinning is enabled
    if config.pinning.enabled or (config.thread_pinning and config.thread_pinning.enabled):
        if not config.vm_ips:
            print("✗ Pinning enabled but no vm_ips provided")
            sys.exit(1)
        # Create pinner client (node IPs passed per method call)
        orchestrator.pinner_client = PinnerClient(timeout=60)  # Must be > max reapply_seconds
        print(f"✓ Configured PinnerClient for {len(config.vm_ips)} nodes")

    # Setup DVFS client if frequency DVFS or governor settings are enabled
    needs_dvfs_client = (
        (config.dvfs and config.dvfs.enabled) or
        (config.governor and config.governor.enabled)
    )
    if needs_dvfs_client:
        if config.dvfs.enabled and not config.physical_node_ips:
            print("✗ DVFS (freq) enabled but no physical_node_ips provided")
            sys.exit(1)
        orchestrator.dvfs_client = DvfsClient(timeout=5)
        print(f"✓ Configured DvfsClient (dvfs={config.dvfs.enabled}, governor={config.governor.enabled})")

    # Execute experiment
    print("\n" + "="*60)
    print("Starting experiment execution...")
    print("="*60)
    print()

    success = orchestrator.execute()

    # Print summary
    print("\n" + "="*60)
    print("Experiment Summary")
    print("="*60)

    if success:
        print("✓ Status: COMPLETED")
    else:
        print(f"✗ Status: FAILED")
        if orchestrator.abort_reason:
            print(f"  Reason: {orchestrator.abort_reason}")

    print(f"\nResults directory: {ctx.run_dir}")
    print(f"  - Metadata: {ctx.meta_path}")
    print(f"  - Events: {ctx.events_path}")
    print(f"  - Metrics: {ctx.metrics_dir}/")

    if scrapers:
        print(f"\nMetric files:")
        for scraper in scrapers:
            metric_file = ctx.metrics_dir / f"{scraper.name}.jsonl"
            if metric_file.exists():
                line_count = sum(1 for _ in open(metric_file))
                print(f"  - {scraper.name}.jsonl ({line_count} records)")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
