import argparse

from pyspark.sql import SparkSession

from spark_service import ExperimentArgs, ExperimentConfig, run_experiment


DEFAULT_INPUT_PATH = "hdfs://namenode:9000/data/nyc_taxi_parquet/yellow_tripdata_2016-01"
DEFAULT_OUTPUT_DIR = "/opt/app/artifacts"
DEFAULT_APP_NAME = "NYC Taxi Trips Baseline"
DEFAULT_EXECUTOR_MEMORY = "1g"
DEFAULT_DRIVER_MEMORY = "1g"
DEFAULT_SHUFFLE_PARTITIONS = 4
DEFAULT_LOG_LEVEL = "ERROR"


def parse_args() -> ExperimentArgs:
    parser = argparse.ArgumentParser(description="Spark application for NYC Taxi Trips experiments.")
    parser.add_argument("--input", default=DEFAULT_INPUT_PATH, help="Path to the input dataset in HDFS.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Local directory for experiment logs.")
    parser.add_argument("--app-name", default=DEFAULT_APP_NAME, help="Spark application name.")
    parser.add_argument("--executor-memory", default=DEFAULT_EXECUTOR_MEMORY, help="Spark executor memory limit.")
    parser.add_argument("--driver-memory", default=DEFAULT_DRIVER_MEMORY, help="Spark driver memory limit.")
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=DEFAULT_SHUFFLE_PARTITIONS,
        help="Number of shuffle partitions for baseline run.",
    )
    parser.add_argument(
        "--log-level",
        default=DEFAULT_LOG_LEVEL,
        choices=["ALL", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF"],
        help="Spark log level for the driver.",
    )
    namespace = parser.parse_args()
    return ExperimentArgs(**vars(namespace))

def configure_baseline_builder(builder: SparkSession.Builder, args: ExperimentArgs) -> SparkSession.Builder:
    return builder.config("spark.sql.adaptive.enabled", "false")

def main() -> None:
    args = parse_args()
    config = ExperimentConfig(
        logger_name="nyc-taxi-spark",
        finished_message="Spark experiment finished",
        output_prefix="experiment",
        spark_builder=configure_baseline_builder,
    )
    run_experiment(args, config)

if __name__ == "__main__":
    main()
