#!/usr/bin/env python3
"""
Compare multiple Query5 experiment runs with side-by-side metrics.

Usage:
    python3 compare_query5_experiments.py <run_dir1> <run_dir2> [run_dir3] ...

Example:
    python3 compare_query5_experiments.py runs/query5mod_run1 runs/query5mod_run2
"""

import json
import sys
from pathlib import Path
from typing import List, Dict, Any

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

# Set seaborn style
sns.set_theme(style="whitegrid")
sns.set_palette("husl")


def load_jsonl(filepath: Path) -> List[Dict[str, Any]]:
    """Load JSONL file and return list of records."""
    records = []
    with open(filepath, 'r') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def extract_flink_metrics_query5(records: List[Dict]) -> pd.DataFrame:
    """Extract Flink metrics for Query5 vertices: Source, Q1_Transform, Q1_SlidingWindow, Sink."""
    data = []

    for record in records:
        if not record['ok'] or 'data' not in record:
            continue

        t = record['t_unix_ms'] / 1000.0
        vertices = record['data'].get('vertices', {})

        row = {'time': t}

        # Extract metrics for each vertex
        for vertex_id, vertex_data in vertices.items():
            vertex_name = vertex_data.get('name', '')
            aggregated = vertex_data.get('aggregated', {})

            if 'Source' in vertex_name or 'Q1_Source' in vertex_name:
                for key, value in aggregated.items():
                    if 'numRecordsOutPerSecond' in key:
                        row['source_records_out_per_sec'] = float(value) if value != 'NaN' else 0
                    if 'backPressuredTimeMsPerSecond' in key:
                        row['source_backpressure_ms'] = float(value) if value != 'NaN' else 0
                    if 'busyTimeMsPerSecond' in key:
                        row['source_busy_ms'] = float(value) if value != 'NaN' else 0

            elif 'Q1_Transform' in vertex_name:
                for key, value in aggregated.items():
                    if 'numRecordsOutPerSecond' in key:
                        row['transform_records_out_per_sec'] = float(value) if value != 'NaN' else 0
                    if 'backPressuredTimeMsPerSecond' in key:
                        row['transform_backpressure_ms'] = float(value) if value != 'NaN' else 0
                    if 'numRecordsInPerSecond' in key:
                        row['transform_records_in_per_sec'] = float(value) if value != 'NaN' else 0
                    if 'busyTimeMsPerSecond' in key:
                        row['transform_busy_ms'] = float(value) if value != 'NaN' else 0

            elif 'Q1_SlidingWindow' in vertex_name:
                for key, value in aggregated.items():
                    if 'numRecordsOutPerSecond' in key:
                        row['window_records_out_per_sec'] = float(value) if value != 'NaN' else 0
                    if 'backPressuredTimeMsPerSecond' in key:
                        row['window_backpressure_ms'] = float(value) if value != 'NaN' else 0
                    if 'numRecordsInPerSecond' in key:
                        row['window_records_in_per_sec'] = float(value) if value != 'NaN' else 0
                    if 'busyTimeMsPerSecond' in key:
                        row['window_busy_ms'] = float(value) if value != 'NaN' else 0

            elif 'Sink' in vertex_name or 'Q1_Sink' in vertex_name:
                for key, value in aggregated.items():
                    if 'numRecordsInPerSecond' in key:
                        row['sink_records_in_per_sec'] = float(value) if value != 'NaN' else 0
                    if 'busyTimeMsPerSecond' in key:
                        row['sink_busy_ms'] = float(value) if value != 'NaN' else 0

        data.append(row)

    df = pd.DataFrame(data)
    if not df.empty:
        start_time = df['time'].iloc[0]
        df['time'] = df['time'] - start_time
    return df


def extract_power(records: List[Dict]) -> pd.DataFrame:
    """Extract power consumption data per socket."""
    data = []
    for record in records:
        if record['ok'] and 'data' in record:
            t = record['t_unix_ms'] / 1000.0

            row = {'time': t}
            nodes = record['data'].get('nodes', {})

            socket_idx = 0
            for node_ip, node_data in nodes.items():
                # Get per_socket power
                per_socket = node_data.get('per_socket_w', {})
                for socket_id, power in per_socket.items():
                    row[f'socket_{socket_idx}'] = float(power)
                    socket_idx += 1

            data.append(row)

    df = pd.DataFrame(data)
    if not df.empty:
        start_time = df['time'].iloc[0]
        df['time'] = df['time'] - start_time
    return df


def extract_frequency(records: List[Dict]) -> pd.DataFrame:
    """Extract CPU frequency data for cores."""
    data = []

    for record in records:
        if not record['ok'] or 'data' not in record:
            continue

        t = record['t_unix_ms'] / 1000.0
        row = {'time': t}

        nodes = record['data'].get('nodes', {})
        for node_ip, node_data in nodes.items():
            cores = node_data.get('cores', {})
            for core_id, freq_khz in cores.items():
                if freq_khz is not None:
                    # Convert kHz to MHz for readability
                    row[f'core_{core_id}_mhz'] = freq_khz / 1000.0

        data.append(row)

    df = pd.DataFrame(data)
    if not df.empty:
        start_time = df['time'].iloc[0]
        df['time'] = df['time'] - start_time
    return df


def load_run_data(run_dir: Path) -> Dict[str, pd.DataFrame]:
    """Load all metrics for a single run directory."""
    metrics_dir = run_dir / 'metrics'

    if not metrics_dir.exists():
        print(f"Warning: {metrics_dir} does not exist")
        return {}

    data = {}

    # Load Flink metrics
    flink_file = metrics_dir / 'flink_rest.jsonl'
    if flink_file.exists():
        data['flink'] = extract_flink_metrics_query5(load_jsonl(flink_file))

    # Load power metrics
    power_file = metrics_dir / 'power.jsonl'
    if power_file.exists():
        data['power'] = extract_power(load_jsonl(power_file))

    # Load frequency metrics
    freq_file = metrics_dir / 'frequency.jsonl'
    if freq_file.exists():
        data['frequency'] = extract_frequency(load_jsonl(freq_file))

    return data


def get_run_label(run_name: str) -> str:
    """Convert run directory name to short label based on governor pattern."""
    label_parts = []

    if 'ondemand-powersave' in run_name:
        label_parts.append('ondemand+powersave')
    elif 'ondemand' in run_name:
        label_parts.append('ondemand')

    if 'uncore' in run_name:
        label_parts.append('uncore')

    if label_parts:
        return '+'.join(label_parts)

    return run_name


def compare_query5_experiments(run_dirs: List[Path]):
    """Create comparison plot for multiple Query5 experiment runs."""

    # Load data for all runs
    all_data = {}
    run_labels = []

    for run_dir in run_dirs:
        run_name = run_dir.name
        run_labels.append(run_name)
        all_data[run_name] = load_run_data(run_dir)
        print(f"✓ Loaded data for: {run_name}")

    # Create mapping from run names to short labels
    run_label_map = {run_name: get_run_label(run_name) for run_name in run_labels}

    # Define colors for each run - using highly distinguishable colors (blue/red theme)
    colors = ['#2E86DE', '#EE5A24', '#0FB9B1', '#D63031', '#5F27CD', '#F79F1F']
    line_styles = ['-', '--', '-.', ':']

    # Create figure with subplots (5 rows)
    fig, axes = plt.subplots(5, 1, figsize=(7, 10))
    fig.suptitle('Query5 Experiment Comparison', fontsize=8, fontweight='bold')

    # 1. Busy Time: Transform and SlidingWindow per file
    ax1 = axes[0]
    has_data = False
    legend_handles = []
    
    for idx, (run_name, data) in enumerate(all_data.items()):
        if 'flink' in data and not data['flink'].empty:
            flink_df = data['flink']
            color = colors[idx % len(colors)]
            
            if 'transform_busy_ms' in flink_df.columns:
                ax1.plot(flink_df['time'], flink_df['transform_busy_ms'] / 10.0,
                        linewidth=1.5, color=color, linestyle='-', alpha=0.8)
                has_data = True
            
            if 'window_busy_ms' in flink_df.columns:
                ax1.plot(flink_df['time'], flink_df['window_busy_ms'] / 10.0,
                        linewidth=1.5, color=color, linestyle='--', alpha=0.8)
                has_data = True
            
            # Add to legend
            from matplotlib.lines import Line2D
            legend_handles.append(Line2D([0], [0], color=color, lw=2, linestyle='-', label=f'Run: {run_label_map[run_name]}'))
    
    # Add vertex line styles to legend
    if legend_handles:
        legend_handles.append(Line2D([0], [0], color='white', lw=0))  # Spacer
    legend_handles.append(Line2D([0], [0], color='gray', lw=2, linestyle='-', label='Transform'))
    legend_handles.append(Line2D([0], [0], color='gray', lw=2, linestyle='--', label='SlidingWindow'))

    if has_data:
        ax1.set_ylabel('Busy Time (%)', fontsize=8, fontweight='bold')
        ax1.set_xlabel('Time (seconds)', fontsize=8)
        ax1.set_ylim(0, 100)
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(labelsize=8)
        ax1.set_title('Busy Time by Run (Colors) and Vertex (LineStyles)', fontsize=8, fontweight='bold')
        ax1.legend(handles=legend_handles, loc='best', fontsize=8, frameon=True)
    else:
        ax1.text(0.5, 0.5, 'No busy time data available', ha='center', va='center', transform=ax1.transAxes)

    # 2. Power Consumption per file
    ax2 = axes[1]
    has_data = False
    legend_handles_power = []
    linestyles_for_sockets = ['-', '--', '-.', ':']

    # Collect all socket indices first
    all_socket_indices = set()
    for run_name, data in all_data.items():
        if 'power' in data and not data['power'].empty:
            power_df = data['power']
            socket_cols = [col for col in power_df.columns if col.startswith('socket_')]
            for socket_col in socket_cols:
                socket_idx = int(socket_col.split('_')[1])
                all_socket_indices.add(socket_idx)

    # Create legend for runs (colors)
    for idx, run_name in enumerate(run_labels):
        color = colors[idx % len(colors)]
        from matplotlib.lines import Line2D
        legend_handles_power.append(Line2D([0], [0], color=color, lw=2, label=f'Run: {run_label_map[run_name]}'))

    # Add line style explanations for sockets
    legend_handles_power.append(Line2D([0], [0], color='white', lw=0))  # Spacer
    for socket_idx in sorted(all_socket_indices):
        linestyle = linestyles_for_sockets[socket_idx % len(linestyles_for_sockets)]
        legend_handles_power.append(Line2D([0], [0], color='gray', lw=2, linestyle=linestyle, label=f'Socket {socket_idx}'))

    for idx, (run_name, data) in enumerate(all_data.items()):
        if 'power' in data and not data['power'].empty:
            power_df = data['power']
            color = colors[idx % len(colors)]

            # Plot each socket with different line style and run with different color
            socket_cols = [col for col in power_df.columns if col.startswith('socket_')]
            for socket_idx, socket_col in enumerate(socket_cols):
                linestyle = linestyles_for_sockets[socket_idx % len(linestyles_for_sockets)]
                ax2.plot(power_df['time'], power_df[socket_col],
                        linewidth=1.5, color=color, linestyle=linestyle, alpha=0.8)
                has_data = True

            # Print average power per socket
            for socket_col in socket_cols:
                avg_power = power_df[socket_col].mean()
                print(f"  {run_name} - {socket_col}: {avg_power:.2f}W")

    if has_data:
        ax2.set_ylabel('Power (Watts)', fontsize=8, fontweight='bold')
        ax2.set_xlabel('Time (seconds)', fontsize=8)
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(labelsize=8)
        ax2.set_title('Power Consumption per Socket (Colors=Runs, LineStyles=Sockets)', fontsize=8, fontweight='bold')
        ax2.legend(handles=legend_handles_power, loc='best', fontsize=8, frameon=True, ncol=2)
    else:
        ax2.text(0.5, 0.5, 'No power data available', ha='center', va='center', transform=ax2.transAxes)

    # 3. Throughput: Transform and SlidingWindow output per file
    ax3 = axes[2]
    has_data = False
    legend_handles = []

    for idx, (run_name, data) in enumerate(all_data.items()):
        if 'flink' in data and not data['flink'].empty:
            flink_df = data['flink']
            color = colors[idx % len(colors)]

            if 'transform_records_out_per_sec' in flink_df.columns:
                ax3.plot(flink_df['time'], flink_df['transform_records_out_per_sec'],
                        linewidth=1.5, color=color, linestyle='-', alpha=0.8)
                has_data = True

            if 'window_records_out_per_sec' in flink_df.columns:
                ax3.plot(flink_df['time'], flink_df['window_records_out_per_sec'],
                        linewidth=1.5, color=color, linestyle='--', alpha=0.8)
                has_data = True

            # Add to legend
            from matplotlib.lines import Line2D
            legend_handles.append(Line2D([0], [0], color=color, lw=2, linestyle='-', label=f'Run: {run_label_map[run_name]}'))

    # Add vertex line styles to legend
    if legend_handles:
        legend_handles.append(Line2D([0], [0], color='white', lw=0))  # Spacer
    legend_handles.append(Line2D([0], [0], color='gray', lw=2, linestyle='-', label='Transform'))
    legend_handles.append(Line2D([0], [0], color='gray', lw=2, linestyle='--', label='SlidingWindow'))

    if has_data:
        ax3.set_ylabel('Throughput (records/s)', fontsize=8, fontweight='bold')
        ax3.set_xlabel('Time (seconds)', fontsize=8)
        ax3.grid(True, alpha=0.3)
        ax3.tick_params(labelsize=8)
        ax3.set_title('Throughput by Run (Colors) and Vertex (LineStyles)', fontsize=8, fontweight='bold')
        ax3.legend(handles=legend_handles, loc='best', fontsize=8, frameon=True)
    else:
        ax3.text(0.5, 0.5, 'No throughput data available', ha='center', va='center', transform=ax3.transAxes)

    # 4. Backpressure: Transform and SlidingWindow per file
    ax4 = axes[3]
    has_data = False
    legend_handles = []

    for idx, (run_name, data) in enumerate(all_data.items()):
        if 'flink' in data and not data['flink'].empty:
            flink_df = data['flink']
            color = colors[idx % len(colors)]

            if 'transform_backpressure_ms' in flink_df.columns:
                ax4.plot(flink_df['time'], flink_df['transform_backpressure_ms'],
                        linewidth=1.5, color=color, linestyle='-', alpha=0.8)
                has_data = True

            if 'window_backpressure_ms' in flink_df.columns:
                ax4.plot(flink_df['time'], flink_df['window_backpressure_ms'],
                        linewidth=1.5, color=color, linestyle='--', alpha=0.8)
                has_data = True

            # Add to legend
            from matplotlib.lines import Line2D
            legend_handles.append(Line2D([0], [0], color=color, lw=2, linestyle='-', label=f'Run: {run_label_map[run_name]}'))

    # Add vertex line styles to legend
    if legend_handles:
        legend_handles.append(Line2D([0], [0], color='white', lw=0))  # Spacer
    legend_handles.append(Line2D([0], [0], color='gray', lw=2, linestyle='-', label='Transform'))
    legend_handles.append(Line2D([0], [0], color='gray', lw=2, linestyle='--', label='SlidingWindow'))

    if has_data:
        ax4.set_ylabel('Backpressure (ms/s)', fontsize=8, fontweight='bold')
        ax4.set_xlabel('Time (seconds)', fontsize=8)
        ax4.grid(True, alpha=0.3)
        ax4.tick_params(labelsize=8)
        ax4.set_title('Backpressure by Run (Colors) and Vertex (LineStyles)', fontsize=8, fontweight='bold')
        ax4.legend(handles=legend_handles, loc='best', fontsize=8, frameon=True)
    else:
        ax4.text(0.5, 0.5, 'No backpressure data available', ha='center', va='center', transform=ax4.transAxes)

    # 5. Frequency: cores 2 and 12 per file
    ax5 = axes[4]
    has_data = False
    legend_handles = []

    for idx, (run_name, data) in enumerate(all_data.items()):
        if 'frequency' in data and not data['frequency'].empty:
            freq_df = data['frequency']
            color = colors[idx % len(colors)]

            # Plot core 2
            core_2_col = 'core_2_mhz'
            if core_2_col in freq_df.columns:
                ax5.plot(freq_df['time'], freq_df[core_2_col],
                        linewidth=1.5, color=color, linestyle='-', alpha=0.8)
                has_data = True

            # Plot core 12
            core_12_col = 'core_12_mhz'
            if core_12_col in freq_df.columns:
                ax5.plot(freq_df['time'], freq_df[core_12_col],
                        linewidth=1.5, color=color, linestyle='--', alpha=0.8)
                has_data = True

            # Add to legend
            from matplotlib.lines import Line2D
            legend_handles.append(Line2D([0], [0], color=color, lw=2, linestyle='-', label=f'Run: {run_label_map[run_name]}'))

    # Add core line styles to legend
    if legend_handles:
        legend_handles.append(Line2D([0], [0], color='white', lw=0))  # Spacer
    legend_handles.append(Line2D([0], [0], color='gray', lw=2, linestyle='-', label='Core 2'))
    legend_handles.append(Line2D([0], [0], color='gray', lw=2, linestyle='--', label='Core 12'))

    if has_data:
        ax5.set_ylabel('Frequency (MHz)', fontsize=8, fontweight='bold')
        ax5.set_xlabel('Time (seconds)', fontsize=8)
        ax5.grid(True, alpha=0.3)
        ax5.tick_params(labelsize=8)
        ax5.set_title('CPU Frequency by Run (Colors) and Core (LineStyles)', fontsize=8, fontweight='bold')
        ax5.legend(handles=legend_handles, loc='best', fontsize=8, frameon=True)
    else:
        ax5.text(0.5, 0.5, 'No frequency data available', ha='center', va='center', transform=ax5.transAxes)

    plt.subplots_adjust(top=0.943,
bottom=0.057,
left=0.107,
right=0.729,
hspace=0.621,
wspace=0.2)
    plt.tight_layout()

    # Save figure
    output_file = Path('comparison_plot.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✓ Plot saved to: {output_file}")

    # Also save as PDF
    output_pdf = Path('comparison_plot.pdf')
    plt.savefig(output_pdf, bbox_inches='tight')
    print(f"✓ PDF saved to: {output_pdf}")

    plt.show()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    run_dirs = [Path(arg) for arg in sys.argv[1:]]

    # Validate all directories exist
    for run_dir in run_dirs:
        if not run_dir.exists():
            print(f"Error: Directory {run_dir} does not exist")
            sys.exit(1)

    compare_query5_experiments(run_dirs)


if __name__ == '__main__':
    main()
