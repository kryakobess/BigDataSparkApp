import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession

from dataset_schema import NYC_TAXI_SCHEMA


LOGGER = logging.getLogger("nyc-taxi-parquet-prep")

DEFAULT_INPUT_PATH = "hdfs://namenode:9000/data/nyc_taxi/yellow_tripdata_2016-01.csv"
DEFAULT_OUTPUT_PATH = "hdfs://namenode:9000/data/nyc_taxi_parquet/yellow_tripdata_2016-01"
DEFAULT_APP_NAME = "NYC Taxi CSV to Parquet"
DEFAULT_LOG_LEVEL = "ERROR"
SPARK_EVENT_LOG_DIR = "/tmp/spark-events"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert NYC Taxi CSV dataset in HDFS to Parquet.")
    parser.add_argument("--input", default=DEFAULT_INPUT_PATH, help="Source CSV path in HDFS.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Target Parquet path in HDFS.")
    parser.add_argument("--app-name", default=DEFAULT_APP_NAME, help="Spark application name.")
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


def build_spark_session(app_name: str, log_level: str) -> SparkSession:
    os.makedirs(SPARK_EVENT_LOG_DIR, exist_ok=True)
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{SPARK_EVENT_LOG_DIR}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)
    return spark


def main() -> None:
    configure_python_logging()
    args = parse_args()
    spark = build_spark_session(args.app_name, args.log_level)

    try:
        LOGGER.info("Reading CSV dataset from %s", args.input)
        df = (
            spark.read.option("header", "true")
            .schema(NYC_TAXI_SCHEMA)
            .csv(args.input)
        )

        LOGGER.info("Writing dataset to Parquet at %s", args.output)
        df.write.mode("overwrite").parquet(args.output)
        LOGGER.info("Parquet dataset preparation completed")
    finally:
        spark.stop()
        LOGGER.info("Spark session stopped")


if __name__ == "__main__":
    main()
