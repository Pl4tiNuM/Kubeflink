# Query1 Experiment

Currency conversion query from the Nexmark benchmark.

## Description

Query1 converts bid prices from dollars to euros using a configurable exchange rate. It demonstrates basic map operations in Flink.

## Parameters

- `--ratelist`: Event generation rate and duration (format: `rate1_duration1_rate2_duration2_...`)
  - Example: `250_300000_11000_300000` means 250 events/sec for 300 seconds, then 11000 events/sec for 300 seconds
- `--exchange-rate`: Dollar to Euro conversion rate (default: 0.82)
- `--psrc`: Source parallelism (default: 1)
- `--pmap`: Map operator parallelism (default: 1)
- `--psink`: Sink parallelism (default: 1)

## Running

```bash
./run-query1.sh
```

To customize parameters, edit the variables in the script before running.

## JAR Location

The script expects the compiled JAR at: `../target/Query1-jar-with-dependencies.jar`
