#!/bin/zsh

set -euo pipefail

COMPOSE_FILE="docker-compose.hadoop-3dn.yml"
DATASET_LOCAL_PATH="/opt/app/yellow_tripdata_2016-01.csv"
HDFS_CSV_DIR="/data/nyc_taxi"
HDFS_CSV_PATH="${HDFS_CSV_DIR}/yellow_tripdata_2016-01.csv"
HDFS_PARQUET_PATH="/data/nyc_taxi_parquet/yellow_tripdata_2016-01"

echo "[INFO] Waiting for HDFS to become available..."
until docker exec hadoop-namenode hdfs dfsadmin -report >/dev/null 2>&1; do
  sleep 5
done

echo "[INFO] Ensuring HDFS directory exists..."
docker exec hadoop-namenode hdfs dfs -mkdir -p "${HDFS_CSV_DIR}"
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/nyc_taxi_parquet
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /data/nyc_taxi_parquet

echo "[INFO] Uploading dataset to HDFS..."
docker exec hadoop-namenode hdfs dfs -put -f "${DATASET_LOCAL_PATH}" "${HDFS_CSV_DIR}/"

echo "[INFO] Verifying HDFS dataset path..."
docker exec hadoop-namenode hdfs dfs -ls "${HDFS_CSV_DIR}"

echo "[INFO] Waiting for Spark master to respond..."
until docker exec spark-master /opt/bitnami/spark/bin/spark-submit --version >/dev/null 2>&1; do
  sleep 5
done

echo "[INFO] Preparing Parquet dataset in HDFS if needed..."
if ! docker exec hadoop-namenode hdfs dfs -test -d "${HDFS_PARQUET_PATH}"; then
  docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/app/prepare_parquet.py \
    --input "hdfs://namenode:9000${HDFS_CSV_PATH}" \
    --output "hdfs://namenode:9000${HDFS_PARQUET_PATH}"
fi

echo "[INFO] Running baseline experiment for 3 DataNodes..."
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  /opt/app/main.py \
  --input "hdfs://namenode:9000${HDFS_PARQUET_PATH}" \
  --app-name "NYC Taxi Baseline 3DN" \
  --output-dir /opt/app/artifacts

echo "[INFO] Experiment completed."
