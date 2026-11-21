# Kafka Connect worker with the Debezium Postgres connector installed.
#
# Confluent's image already ships the Avro converter. It has no curl or tar,
# so the connector is downloaded and checked in a throwaway build stage and
# only the extracted plugin is copied into the final image.
ARG CP_VERSION=8.4.0

FROM alpine:3.22 AS plugins
ARG DEBEZIUM_VERSION=3.7.1.Final
WORKDIR /plugins
RUN set -eu; \
    url="https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz"; \
    wget -q -O plugin.tar.gz "$url"; \
    wget -q -O plugin.tar.gz.sha1 "$url.sha1"; \
    echo "$(cat plugin.tar.gz.sha1)  plugin.tar.gz" | sha1sum -c -; \
    tar -xzf plugin.tar.gz; \
    rm plugin.tar.gz plugin.tar.gz.sha1

FROM confluentinc/cp-kafka-connect:${CP_VERSION}
# Kafka Connect loads each subfolder of plugin.path in its own classloader,
# so Debezium's dependencies can't clash with the worker's own jars.
COPY --from=plugins /plugins/ /usr/share/confluent-hub-components/
