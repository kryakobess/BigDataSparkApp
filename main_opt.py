import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, hour, max as spark_max, min as spark_min, round as spark_round, to_timestamp

try:
    import psutil
except ImportError:
    psutil = None


LOGGER = logging.getLogger("nyc-taxi-spark-opt")
SPARK_EVENT_LOG_DIR = "/tmp/spark-events"

# Performance-related defaults for optimized experiment.
DEFAULT_INPUT_PATH = "hdfs://namenode:9000/data/nyc_taxi/yellow_tripdata_2016-01.csv"
DEFAULT_OUTPUT_DIR = "artifacts"
DEFAULT_APP_NAME = "NYC Taxi Trips Optimized"
DEFAULT_EXECUTOR_MEMORY = "1g"
DEFAULT_DRIVER_MEMORY = "1g"
DEFAULT_SHUFFLE_PARTITIONS = 16
DEFAULT_TARGET_PARTITIONS = 12
DEFAULT_LOG_LEVEL = "ERROR"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Optimized Spark application for NYC Taxi Trips experiments."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_PATH,
        help="Path to the input CSV file in HDFS.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Local directory for experiment logs.",
    )
    parser.add_argument(
        "--app-name",
        default=DEFAULT_APP_NAME,
        help="Spark application name.",
    )
    parser.add_argument(
        "--executor-memory",
        default=DEFAULT_EXECUTOR_MEMORY,
        help="Spark executor memory limit.",
    )
    parser.add_argument(
        "--driver-memory",
        default=DEFAULT_DRIVER_MEMORY,
        help="Spark driver memory limit.",
    )
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
    return parser.parse_args()


def configure_python_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def build_spark_session(args: argparse.Namespace) -> SparkSession:
    os.makedirs(SPARK_EVENT_LOG_DIR, exist_ok=True)

    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.executor.memory", args.executor_memory)
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
        .config("spark.default.parallelism", args.target_partitions)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{SPARK_EVENT_LOG_DIR}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(args.log_level)
    return spark


def current_driver_ram_mb() -> float:
    if psutil is None:
        return -1.0
    process = psutil.Process(os.getpid())
    return round(process.memory_info().rss / (1024 * 1024), 2)


def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def log_step_start(step_name: str) -> float:
    LOGGER.info("START step=%s | driver_ram_mb=%.2f", step_name, current_driver_ram_mb())
    return time.perf_counter()


def log_step_end(step_name: str, start_ts: float, step_metrics: List[Dict[str, Any]]) -> None:
    duration_sec = round(time.perf_counter() - start_ts, 3)
    ram_mb = current_driver_ram_mb()
    LOGGER.info(
        "END step=%s | duration_sec=%.3f | driver_ram_mb=%.2f",
        step_name,
        duration_sec,
        ram_mb,
    )
    step_metrics.append(
        {
            "step": step_name,
            "duration_sec": duration_sec,
            "driver_ram_mb": ram_mb,
            "finished_at": datetime.now().isoformat(timespec="seconds"),
        }
    )


def read_dataset(spark: SparkSession, input_path: str) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )


def prepare_dataset(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("tpep_pickup_datetime_ts", to_timestamp("tpep_pickup_datetime"))
        .withColumn("tpep_dropoff_datetime_ts", to_timestamp("tpep_dropoff_datetime"))
        .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
        .filter(col("trip_distance").isNotNull())
        .filter(col("fare_amount").isNotNull())
        .filter(col("passenger_count").isNotNull())
        .filter(col("trip_distance") > 0)
        .filter(col("fare_amount") > 0)
        .filter(col("passenger_count") > 0)
    )


def resolve_location_column(df: DataFrame) -> str:
    if "PULocationID" in df.columns:
        return "PULocationID"
    if "pickup_longitude" in df.columns and "pickup_latitude" in df.columns:
        return "pickup_zone_approx"
    raise ValueError("Could not find a supported pickup location representation in dataset.")


def add_location_features(df: DataFrame) -> DataFrame:
    if "PULocationID" in df.columns:
        return df
    if "pickup_longitude" in df.columns and "pickup_latitude" in df.columns:
        return df.withColumn(
            "pickup_zone_approx",
            spark_round(col("pickup_longitude"), 2).cast("string"),
        )
    return df


def optimize_dataset(df: DataFrame, target_partitions: int) -> DataFrame:
    location_column = resolve_location_column(df)
    optimized_df = df.repartition(target_partitions, location_column).persist(StorageLevel.MEMORY_AND_DISK)
    optimized_df.count() # for cache
    return optimized_df


def collect_metrics(df: DataFrame) -> Dict[str, Any]:
    location_column = resolve_location_column(df)
    total_rows = df.count()

    distance_stats = (
        df.select(
            avg("trip_distance").alias("avg_trip_distance"),
            spark_min("trip_distance").alias("min_trip_distance"),
            spark_max("trip_distance").alias("max_trip_distance"),
            avg("fare_amount").alias("avg_fare_amount"),
        )
        .first()
        .asDict()
    )

    rides_by_passenger_count = [
        row.asDict()
        for row in df.groupBy("passenger_count")
        .agg(count("*").alias("trip_count"))
        .orderBy(col("passenger_count").asc())
        .collect()
    ]

    rides_by_pickup_hour = [
        row.asDict()
        for row in df.groupBy("pickup_hour")
        .agg(count("*").alias("trip_count"))
        .orderBy(col("trip_count").desc())
        .limit(24)
        .collect()
    ]

    top_pickup_locations = [
        row.asDict()
        for row in df.groupBy(location_column)
        .agg(count("*").alias("trip_count"))
        .orderBy(col("trip_count").desc())
        .limit(10)
        .collect()
    ]

    return {
        "total_rows_after_cleanup": total_rows,
        "pickup_location_dimension": location_column,
        "distance_and_fare_stats": distance_stats,
        "rides_by_passenger_count": rides_by_passenger_count,
        "rides_by_pickup_hour": rides_by_pickup_hour,
        "top_pickup_locations": top_pickup_locations,
        "storage_level": "MEMORY_AND_DISK",
        "target_partitions": df.rdd.getNumPartitions(),
    }


def write_experiment_log(
    args: argparse.Namespace,
    step_metrics: List[Dict[str, Any]],
    result_metrics: Dict[str, Any],
    total_duration_sec: float,
    peak_driver_ram_mb: float,
) -> str:
    ensure_output_dir(args.output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(args.output_dir, f"experiment_opt_{timestamp}.json")

    payload = {
        "experiment_timestamp": datetime.now().isoformat(timespec="seconds"),
        "app_name": args.app_name,
        "input_path": args.input,
        "spark_config": {
            "executor_memory": args.executor_memory,
            "driver_memory": args.driver_memory,
            "shuffle_partitions": args.shuffle_partitions,
            "target_partitions": args.target_partitions,
            "log_level": args.log_level,
            "adaptive_query_execution": True,
        },
        "total_duration_sec": round(total_duration_sec, 3),
        "peak_driver_ram_mb": peak_driver_ram_mb,
        "steps": step_metrics,
        "result_metrics": result_metrics,
    }

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, ensure_ascii=False)

    return output_path


def main() -> None:
    configure_python_logging()
    args = parse_args()

    step_metrics: List[Dict[str, Any]] = []
    peak_driver_ram_mb = current_driver_ram_mb()
    total_start = time.perf_counter()

    LOGGER.info("Optimized Spark experiment started")
    LOGGER.info("Input path: %s", args.input)
    LOGGER.info(
        "Requested Spark memory limits | driver=%s | executor=%s",
        args.driver_memory,
        args.executor_memory,
    )

    spark = build_spark_session(args)

    try:
        step_start = log_step_start("read_dataset")
        raw_df = read_dataset(spark, args.input)
        raw_columns = raw_df.columns
        raw_count = raw_df.count()
        LOGGER.info("Dataset loaded | columns=%d | rows=%d", len(raw_columns), raw_count)
        log_step_end("read_dataset", step_start, step_metrics)

        step_start = log_step_start("prepare_dataset")
        prepared_df = add_location_features(prepare_dataset(raw_df))
        prepared_count = prepared_df.count()
        LOGGER.info("Dataset prepared | rows_after_cleanup=%d", prepared_count)
        log_step_end("prepare_dataset", step_start, step_metrics)

        step_start = log_step_start("optimize_dataset")
        optimized_df = optimize_dataset(prepared_df, args.target_partitions)
        LOGGER.info(
            "Dataset cached and repartitioned | partitions=%d",
            optimized_df.rdd.getNumPartitions(),
        )
        log_step_end("optimize_dataset", step_start, step_metrics)

        step_start = log_step_start("collect_metrics")
        result_metrics = collect_metrics(optimized_df)
        LOGGER.info(
            "Metrics collected | total_rows_after_cleanup=%d",
            result_metrics["total_rows_after_cleanup"],
        )
        log_step_end("collect_metrics", step_start, step_metrics)

        peak_driver_ram_mb = max(
            [peak_driver_ram_mb]
            + [metric["driver_ram_mb"] for metric in step_metrics if metric["driver_ram_mb"] >= 0]
        )
        total_duration_sec = time.perf_counter() - total_start

        step_start = log_step_start("write_experiment_log")
        output_path = write_experiment_log(
            args=args,
            step_metrics=step_metrics,
            result_metrics=result_metrics,
            total_duration_sec=total_duration_sec,
            peak_driver_ram_mb=peak_driver_ram_mb,
        )
        LOGGER.info("Experiment log saved to %s", output_path)
        log_step_end("write_experiment_log", step_start, step_metrics)

        LOGGER.info(
            "Optimized Spark experiment finished | total_duration_sec=%.3f | peak_driver_ram_mb=%.2f",
            total_duration_sec,
            peak_driver_ram_mb,
        )
    finally:
        if "optimized_df" in locals():
            optimized_df.unpersist()
        spark.stop()
        LOGGER.info("Spark session stopped")


if __name__ == "__main__":
    main()
