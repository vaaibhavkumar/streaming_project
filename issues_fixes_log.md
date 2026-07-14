# Spark + Kafka Streaming Project — Issues, Fixes, and Learnings Log

A record of every issue hit while setting up a local Kafka + PySpark Structured
Streaming pipeline on Windows, the fix applied for each, and key learnings or
improvements discovered along the way.

## Environment setup issues

| # | Issue | Fix |
|---|-------|-----|
| 1 | No Docker installed | Installed Docker Desktop; confirmed WSL2 was already set up correctly |
| 2 | `bitnami/kafka:3.7` image not found | Bitnami removed most free images from Docker Hub (Aug 2025) — switched `docker-compose.yml` to the official `apache/kafka:latest` image |
| 3 | `curl -L` failed in PowerShell | PowerShell aliases `curl` to `Invoke-WebRequest`, which doesn't support curl flags — used `curl.exe` explicitly instead |

## Kafka/Python client issues

| # | Issue | Fix |
|---|-------|-----|
| 4 | `kafka-python` library broken (`ModuleNotFoundError: kafka.vendor.six.moves`) | Known bug on modern Python — swapped to the maintained fork `kafka-python-ng` |

## Java/Spark environment issues

| # | Issue | Fix |
|---|-------|-----|
| 5 | No Java installed | Installed Java (initially got Java 25 by default) |
| 6 | Java 25 incompatible with Spark 3.5.1 | Installed Java 17 alongside; used `$env:JAVA_HOME` to point only the Spark terminal at 17 without changing the system default |
| 7 | `cmd` vs PowerShell syntax mismatches (`set` vs `$env:`) | Standardized on PowerShell syntax throughout |
| 8 | `spark-submit`: "Python was not found" (Windows Store alias interception) | Set `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` explicitly to the venv's `python.exe` |
| 9 | `Failed to find Spark jars directory` | Set `SPARK_HOME` explicitly to the pip-installed pyspark folder |
| 10 | Missing `winutils.exe`/`hadoop.dll` (Windows Hadoop compatibility shim) | Downloaded from `cdarlint/winutils` GitHub repo, set `HADOOP_HOME` |
| 11 | First `winutils.exe` download was corrupted (wrong version path, got an HTML error page instead) | Re-downloaded from a version (`hadoop-3.3.5`) confirmed to exist in the repo; verified via byte-header check (`MZ` signature) |
| 12 | `NativeIO$Windows.access0` UnsatisfiedLinkError (stale checkpoint state hitting a native-library gap) | Deleted leftover checkpoint folders for a clean start |

## Python/package version issues

| # | Issue | Fix |
|---|-------|-----|
| 13 | `ModuleNotFoundError: distutils` | Removed in Python 3.12+; installed `setuptools<81` to restore the compatibility shim |
| 14 | `ModuleNotFoundError: pyarrow` | Simply wasn't installed; added it |
| 15 | Python worker crashed silently (`EOFException`) | Root cause: pandas 3.0 + pyarrow 25 far too new for PySpark 3.5.1 — pinned to `pandas==2.2.3` / `pyarrow==18.1.0` |
| 16 | Same crash persisted even after pinning | Root cause was actually **Python 3.13 itself** — too new for PySpark 3.5.1. Rebuilt the entire venv on **Python 3.11** instead |
| 17 | Venv folder locked/couldn't delete | A leftover producer/Spark process was holding the file lock — stopped it first |
| 18 | Stale `requirements.txt` on disk didn't match latest fixes | Manually reinstalled correct pinned versions directly via pip |

## Networking issue

| # | Issue | Fix |
|---|-------|-----|
| 19 | Executor heartbeat failures via `host.docker.internal` | Forced Spark to bind to `127.0.0.1` explicitly via `--conf spark.driver.host` / `--conf spark.driver.bindAddress` |

## Code bugs (mine, not environment)

| # | Issue | Fix |
|---|-------|-----|
| 20 | `state.setTimeoutDuration("30 minutes")` — expected int milliseconds, not a string | Changed to `30 * 60 * 1000` |
| 21 | `applyInPandasWithState` function used `return pd.DataFrame(...)` instead of `yield` | Changed `return` → `yield` |
| 22 | `state.get` expected 2 values but the stored state tuple had 3 values | Removed `account_id` from the persisted state, storing only `running_count` and `running_mean` |
| 23 | `StateSchemaNotCompatible` on restart after schema change | Restored the original persisted state schema (`account_id`, `running_count`, `running_mean`) and added backward-compatible deserialization for both 2- and 3-field stored states |
| 24 | `CHECKPOINT_ROOT` was pointed to `/tmp/...` so the checkpoint folder was not visible inside the project | Changed `CHECKPOINT_ROOT` to `streaming_checkpoints` so checkpoint folders are created inside the project workspace and are visible locally |
| 25 | `BATCH_METADATA_NOT_FOUND` (`_spark_metadata/0` missing) caused the query to abort on write | Cleared stale output metadata under `streaming_output/windowed_aggregates/_spark_metadata` and deleted prior checkpoints under `streaming_checkpoints/*`; restart the job so Spark can rebuild fresh batch metadata. Stale output metadata from interrupted writes must not be reused |
| 26 | Stale local checkpoint and Parquet output caused repeated restart failures even after `--force-fresh-start` was expected to clear state | Added stronger cleanup in `src/streaming_job.py`: `--force-fresh-start` now removes `streaming_checkpoints/<version>` and stale sink output directories; also added `--clear-output` and startup diagnostics to make recovery explicit |

## Final working environment

- **OS**: Windows 11
- **Docker**: Docker Desktop, `apache/kafka:latest` image
- **Java**: Temurin 17 (`jdk-17.0.19.10-hotspot`), set via `JAVA_HOME` per-session
- **Python**: 3.11.9 (venv rebuilt from initial 3.13 install)
- **Key package versions**: `pyspark==3.5.1`, `pandas==2.2.3`, `pyarrow==18.1.0`,
  `kafka-python-ng==2.2.2`, `setuptools<81`
- **Hadoop compatibility**: `winutils.exe` + `hadoop.dll` from `cdarlint/winutils`
  (`hadoop-3.3.5`), `HADOOP_HOME` set accordingly
- **Spark network config**: `spark.driver.host=127.0.0.1`,
  `spark.driver.bindAddress=127.0.0.1`

## Outcome

Windowed revenue aggregation sink confirmed fully working (multiple batches
processed, watermarking functioning, injected anomalies visible in output).
Stateful anomaly-detection sink required one final code fix (`yield` instead
of `return`) before both sinks ran cleanly together.