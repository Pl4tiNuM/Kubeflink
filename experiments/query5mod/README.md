# Query5mod Experiment

Modified sliding window aggregation query from the Nexmark benchmark.

## Description

Query5mod performs sliding window aggregation on bid events. It:
1. Transforms bids with extra computation (configurable workload)
2. Applies sliding window aggregation to count bids per auction
3. Tracks the auction with the highest bid count

## Parameters

- `--ratelist`: Event generation rate and duration (format: `rate_duration`)
  - Example: `5000_7200000` means 5000 events/sec for 7200 seconds (2 hours)
- `--topsize`: Number of top auctions to track (default: 5)
- `--swl_min`: Sliding window length in minutes (default: 60)
- `--sws_min`: Sliding window slide in minutes (default: 1)
- `--wtm_ms`: Watermark interval in milliseconds (default: 1000)
- `--extsize`: Extra computation size for transformation (default: 1000)
- `--psrc`: Source parallelism (default: 1)
- `--ptrans`: Transform operator parallelism (default: 1)
- `--pwindow`: Window operator parallelism (default: 2)
- `--psink`: Sink parallelism (default: 1)

## Running

```bash
./run-query5mod.sh
```

To customize parameters, edit the variables in the script before running.

## JAR Location

The script expects the compiled JAR at: `../target/Query5mod-jar-with-dependencies.jar`
