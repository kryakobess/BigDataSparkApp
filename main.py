import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    expr,
    col,
    count,
    hour,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    stddev,
    sum as spark_sum,
    to_timestamp,
)

from dataset_schema import NYC_TAXI_SCHEMA

try:
    import psutil
except ImportError:
    psutil = None


LOGGER = logging.getLogger("nyc-taxi-spark")
SPARK_EVENT_LOG_DIR = "/tmp/spark-events"

# Performance-related defaults for baseline experiment.
DEFAULT_INPUT_PATH = "hdfs://namenode:9000/data/nyc_taxi_parquet/yellow_tripdata_2016-01"
DEFAULT_OUTPUT_DIR = "/opt/app/artifacts"
DEFAULT_APP_NAME = "NYC Taxi Trips Baseline"
DEFAULT_EXECUTOR_MEMORY = "1g"
DEFAULT_DRIVER_MEMORY = "1g"
DEFAULT_SHUFFLE_PARTITIONS = 8
DEFAULT_LOG_LEVEL = "ERROR"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Spark application for NYC Taxi Trips experiments."
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
        help="Number of shuffle partitions for baseline run.",
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
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{SPARK_EVENT_LOG_DIR}")
        .config("spark.sql.adaptive.enabled", "false")
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
    if input_path.endswith(".csv"):
        return spark.read.option("header", "true").schema(NYC_TAXI_SCHEMA).csv(input_path)
    return spark.read.parquet(input_path)


def prepare_dataset(df: DataFrame) -> DataFrame:
    prepared = (
        df.withColumn("tpep_pickup_datetime_ts", to_timestamp("tpep_pickup_datetime"))
        .withColumn("tpep_dropoff_datetime_ts", to_timestamp("tpep_dropoff_datetime"))
        .filter(col("trip_distance").isNotNull())
        .filter(col("fare_amount").isNotNull())
        .filter(col("passenger_count").isNotNull())
        .filter(col("trip_distance") > 0)
        .filter(col("fare_amount") > 0)
        .filter(col("passenger_count") > 0)
    )
    return prepared


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


def collect_metrics(df: DataFrame) -> Dict[str, Any]:
    location_column = resolve_location_column(df)
    total_rows = df.count()

    distance_stats = (
        df.select(
            avg("trip_distance").alias("avg_trip_distance"),
            spark_min("trip_distance").alias("min_trip_distance"),
            spark_max("trip_distance").alias("max_trip_distance"),
            avg("fare_amount").alias("avg_fare_amount"),
            stddev("trip_distance").alias("std_trip_distance"),
            spark_sum("total_amount").alias("sum_total_amount"),
            avg("tip_amount").alias("avg_tip_amount"),
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
        for row in df.withColumn("pickup_hour", hour("tpep_pickup_datetime_ts"))
        .groupBy("pickup_hour")
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

    revenue_by_payment_type = [
        row.asDict()
        for row in df.groupBy("payment_type")
        .agg(
            count("*").alias("trip_count"),
            avg("fare_amount").alias("avg_fare_amount"),
            spark_sum("total_amount").alias("total_revenue"),
        )
        .orderBy(col("total_revenue").desc())
        .collect()
    ]

    distance_by_vendor = [
        row.asDict()
        for row in df.groupBy("VendorID")
        .agg(
            count("*").alias("trip_count"),
            avg("trip_distance").alias("avg_trip_distance"),
            avg("total_amount").alias("avg_total_amount"),
        )
        .orderBy(col("trip_count").desc())
        .collect()
    ]

    hourly_revenue = [
        row.asDict()
        for row in df.withColumn("pickup_hour", hour("tpep_pickup_datetime_ts"))
        .groupBy("pickup_hour")
        .agg(
            spark_sum("total_amount").alias("total_revenue"),
            avg("trip_distance").alias("avg_trip_distance"),
            avg("tip_amount").alias("avg_tip_amount"),
        )
        .orderBy(col("total_revenue").desc())
        .collect()
    ]

    vendor_payment_stats = [
        row.asDict()
        for row in df.groupBy("VendorID", "payment_type")
        .agg(
            count("*").alias("trip_count"),
            avg("fare_amount").alias("avg_fare_amount"),
            avg("trip_distance").alias("avg_trip_distance"),
            spark_sum("total_amount").alias("total_revenue"),
        )
        .orderBy(col("total_revenue").desc())
        .collect()
    ]

    from pyspark.sql.functions import expr

    distribution_stats = (
        df.agg(
            expr("percentile_approx(trip_distance, 0.5)").alias("trip_distance_p50"),
            expr("percentile_approx(trip_distance, 0.9)").alias("trip_distance_p90"),
            expr("percentile_approx(fare_amount, 0.5)").alias("fare_amount_p50"),
            expr("percentile_approx(fare_amount, 0.95)").alias("fare_amount_p95"),
            expr("percentile_approx(tip_amount, 0.5)").alias("tip_amount_p50"),
            expr("percentile_approx(tip_amount, 0.95)").alias("tip_amount_p95"),
        )
        .first()
        .asDict()
    )


    return {
        "total_rows_after_cleanup": total_rows,
        "pickup_location_dimension": location_column,
        "distance_and_fare_stats": distance_stats,
        "rides_by_passenger_count": rides_by_passenger_count,
        "rides_by_pickup_hour": rides_by_pickup_hour,
        "top_pickup_locations": top_pickup_locations,
        "revenue_by_payment_type": revenue_by_payment_type,
        "distance_by_vendor": distance_by_vendor,
        "hourly_revenue": hourly_revenue,
        "vendor_payment_stats": vendor_payment_stats,
        "distribution_stats": distribution_stats,
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
    output_path = os.path.join(args.output_dir, f"experiment_{timestamp}.json")

    payload = {
        "experiment_timestamp": datetime.now().isoformat(timespec="seconds"),
        "app_name": args.app_name,
        "input_path": args.input,
        "spark_config": {
            "executor_memory": args.executor_memory,
            "driver_memory": args.driver_memory,
            "shuffle_partitions": args.shuffle_partitions,
            "log_level": args.log_level,
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

    LOGGER.info("Spark experiment started")
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

        step_start = log_step_start("collect_metrics")
        result_metrics = collect_metrics(prepared_df)
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
            "Spark experiment finished | total_duration_sec=%.3f | peak_driver_ram_mb=%.2f",
            total_duration_sec,
            peak_driver_ram_mb,
        )
    finally:
        spark.stop()
        LOGGER.info("Spark session stopped")


if __name__ == "__main__":
    main()
