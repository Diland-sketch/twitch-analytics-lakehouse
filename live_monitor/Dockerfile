FROM apache/spark:3.5.8-scala2.12-java17-python3-ubuntu
USER root
WORKDIR /app
COPY live_monitor/requirements.txt /tmp/i4-requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/i4-requirements.txt
# Resolver el conector en build: el broker no necesita acceso público.
RUN printf 'from pyspark.sql import SparkSession\ns=SparkSession.builder.master("local[1]").getOrCreate()\ns.stop()\n' > /tmp/resolve.py \
 && /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 /tmp/resolve.py
COPY live_monitor /app/live_monitor
COPY twitch_lakehouse /app/twitch_lakehouse
ENV PYTHONPATH=/app:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip PYTHONUNBUFFERED=1 STATE_DIR=/state SPARK_LOCAL_IP=127.0.0.1
ENTRYPOINT []
CMD ["python3", "-m", "live_monitor.consumer"]
