# Core Frequency Scaling

This module provides a simple HTTP server that allows you to set the CPU frequency of specific cores on a Linux system using the DVFS (Dynamic Voltage and Frequency Scaling) mechanism. It uses Flask to create a RESTful API for setting the frequency.

## Requirements
- Governor set to "userspace" for the cores you want to control.
- acpi-cpufreq kernel module loaded.

## Usage
1. Install the required Python packages:
   ```bash
   pip install flask
   ```
2. Run the server:
   ```bash
   python agent.py
   ```
3. Send a POST request to set the frequency:
   ```bash
   curl -X POST http://localhost:4002/api/set_frequency -H "Content-Type: application/json" -d '{"cores": [0,1], "freq": 1200000, "reset": "1"}'
   ```  This sets the frequency of cores 0 and 1 to 1.2 GHz. The `reset` parameter allows frequency reduction if set to "1".
    - `cores`: List of CPU core indices to set the frequency for.
    - `freq`: Target frequency in kHz.
    - `reset`: If set to "1", allows frequency reduction.

## Note
- Ensure you have the necessary permissions to start the server (it requires root access).
- Be cautious when setting frequencies, as improper settings can lead to system instability.

<!-- # Example Request
```json
{
    "cores": [0, 1],
    "freq": 1200000,
    "reset": "1"
}
```

# Example Response
```json
{
    "status": "ok",
    "freq": 1200000,
    "cores": [0, 1]
}

# Concurrency -->