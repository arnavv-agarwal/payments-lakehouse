# Spark with everything the lakehouse jobs need, resolved at build time so a
# job never downloads jars when it starts:
#   spark-sql-kafka   read Kafka topics as a stream
#   spark-avro        decode Avro (from_avro)
#   iceberg runtime   Iceberg tables + SQL extensions (MERGE, CALL procedures)
#   iceberg-aws       S3FileIO: Iceberg's own S3 client (works with any S3 API)
#   postgresql        JDBC driver for the Iceberg catalog database
ARG SPARK_VERSION=4.1.4
FROM apache/spark:${SPARK_VERSION}-java21-python3

ARG SPARK_VERSION=4.1.4
ARG ICEBERG_VERSION=1.12.0
ARG POSTGRES_JDBC_VERSION=42.7.13

USER root
# Let Spark's own dependency resolver (Ivy) fetch the packages and their
# transitive dependencies once, then keep only the resulting jars.
RUN set -eu; \
    echo 'pass' > /tmp/noop.py; \
    /opt/spark/bin/spark-submit \
        --conf spark.jars.ivy=/tmp/ivy \
        --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION},org.apache.spark:spark-avro_2.13:${SPARK_VERSION},org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION},org.postgresql:postgresql:${POSTGRES_JDBC_VERSION}" \
        /tmp/noop.py; \
    mkdir -p /opt/lakehouse/jars; \
    # Keep only jars Spark doesn't already ship. Two versions of one library on
    # the classpath cause conflicts that are hard to debug.
    # Ivy names files <group>_<artifact>-<version>.jar.
    for jar in /tmp/ivy/jars/*.jar; do \
        artifact="$(basename "$jar" | sed -E 's/^[^_]+_//; s/-[0-9][0-9A-Za-z.]*\.jar$//')"; \
        if ls /opt/spark/jars/"$artifact"-[0-9]*.jar > /dev/null 2>&1; then \
            echo "skip $artifact (Spark ships it)"; \
        else \
            cp "$jar" /opt/lakehouse/jars/; \
        fi; \
    done; \
    rm -rf /tmp/ivy /tmp/noop.py; \
    # Named volumes copy this ownership on first use, so the job can write checkpoints.
    mkdir -p /checkpoints && chown spark:spark /checkpoints

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
USER spark
# Writable by the spark user. The spark-sql shell creates a small local Hive
# metastore (metastore_db, derby.log) in the working directory even though our
# tables live in the Iceberg catalog.
WORKDIR /opt/spark/work-dir
