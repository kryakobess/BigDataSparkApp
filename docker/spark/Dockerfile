FROM bitnamilegacy/spark:3.5.1

USER root

RUN python3 -m pip install --no-cache-dir psutil \
    && mkdir -p /tmp/spark-events \
    && chown -R 1001:0 /tmp/spark-events

USER 1001
