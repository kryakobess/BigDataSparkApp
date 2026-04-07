import argparse

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from spark_service import ExperimentArgs, ExperimentConfig, resolve_location_column, run_experiment

DEFAULT_INPUT_PATH = "hdfs://namenode:9000/data/nyc_taxi_parquet/yellow_tripdata_2016-01"
DEFAULT_OUTPUT_DIR = "/opt/app/artifacts"
DEFAULT_APP_NAME = "NYC Taxi Trips Optimized"
DEFAULT_EXECUTOR_MEMORY = "1g"
DEFAULT_DRIVER_MEMORY = "1g"
DEFAULT_SHUFFLE_PARTITIONS = 16
DEFAULT_TARGET_PARTITIONS = 8
DEFAULT_LOG_LEVEL = "ERROR"


def parse_args() -> ExperimentArgs:
    parser = argparse.ArgumentParser(description="Optimized Spark application for NYC Taxi Trips experiments.")
    parser.add_argument("--input", default=DEFAULT_INPUT_PATH, help="Path to the input dataset in HDFS.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Local directory for experiment logs.")
    parser.add_argument("--app-name", default=DEFAULT_APP_NAME, help="Spark application name.")
    parser.add_argument("--executor-memory", default=DEFAULT_EXECUTOR_MEMORY, help="Spark executor memory limit.")
    parser.add_argument("--driver-memory", default=DEFAULT_DRIVER_MEMORY, help="Spark driver memory limit.")
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=DEFAULT_SHUFFLE_PARTITIONS,
        help="Number of shuffle partitions for optimized run.",
    )
    parser.add_argument(
        "--target-partitions",
        type=int,
        default=DEFAULT_TARGET_PARTITIONS,
        help="Target partition count after data preparation.",
    )
    parser.add_argument(
        "--log-level",
        default=DEFAULT_LOG_LEVEL,
        choices=["ALL", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF"],
        help="Spark log level for the driver.",
    )
    namespace = parser.parse_args()
    return ExperimentArgs(**vars(namespace))


def configure_optimized_builder(builder: SparkSession.Builder, args: ExperimentArgs) -> SparkSession.Builder:
    return (
        builder.config("spark.default.parallelism", args.target_partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
    )

def optimize_dataset(df: DataFrame, args: ExperimentArgs) -> DataFrame:
    location_column = resolve_location_column(df)
    optimized_df = (
        df.select(
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "pickup_hour",
            location_column,
            "payment_type",
            "VendorID",
            "total_amount",
            "tip_amount",
            "tpep_pickup_datetime_ts",
        )
        .coalesce(args.target_partitions)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    optimized_df.count() # прогрев (гоев) кэша
    return optimized_df

def extra_spark_config(args: ExperimentArgs) -> dict:
    return {
        "target_partitions": args.target_partitions,
        "adaptive_query_execution": True,
    }

def main() -> None:
    args = parse_args()
    config = ExperimentConfig(
        logger_name="nyc-taxi-spark-opt",
        finished_message="Optimized Spark experiment finished",
        output_prefix="experiment_opt",
        spark_builder=configure_optimized_builder,
        transform_after_prepare=optimize_dataset,
        extra_spark_config=extra_spark_config,
    )
    run_experiment(args, config)


if __name__ == "__main__":
    main()
