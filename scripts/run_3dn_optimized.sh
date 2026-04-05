#!/bin/zsh

set -euo pipefail

COMPOSE_FILE="docker-compose.hadoop-3dn.yml"
DATASET_LOCAL_PATH="/opt/app/yellow_tripdata_2016-01.csv"
HDFS_DATASET_DIR="/data/nyc_taxi"
HDFS_DATASET_PATH="${HDFS_DATASET_DIR}/yellow_tripdata_2016-01.csv"

echo "[INFO] Waiting for HDFS to become available..."
until docker exec hadoop-namenode hdfs dfsadmin -report >/dev/null 2>&1; do
  sleep 5
done

echo "[INFO] Ensuring HDFS directory exists..."
docker exec hadoop-namenode hdfs dfs -mkdir -p "${HDFS_DATASET_DIR}"

echo "[INFO] Uploading dataset to HDFS..."
docker exec hadoop-namenode hdfs dfs -put -f "${DATASET_LOCAL_PATH}" "${HDFS_DATASET_DIR}/"

echo "[INFO] Verifying HDFS dataset path..."
docker exec hadoop-namenode hdfs dfs -ls "${HDFS_DATASET_DIR}"

echo "[INFO] Waiting for Spark master to respond..."
until docker exec spark-master /opt/bitnami/spark/bin/spark-submit --version >/dev/null 2>&1; do
  sleep 5
done

echo "[INFO] Running optimized experiment for 3 DataNodes..."
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  /opt/app/main_opt.py \
  --input "hdfs://namenode:9000${HDFS_DATASET_PATH}" \
  --app-name "NYC Taxi Optimized 3DN" \
  --output-dir /opt/app/artifacts

echo "[INFO] Experiment completed."
