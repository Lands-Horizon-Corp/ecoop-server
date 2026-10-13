#!/bin/sh
# Registers (or updates) the Debezium Postgres connector. PUT on
# /connectors/<name>/config is idempotent, so this is safe on every deploy.
# Everything comes from the environment; nothing is hard coded here.
set -eu

: "${CONNECT_URL:?}" "${CONNECTOR_NAME:?}" "${DB_HOST:?}" "${DB_PORT:?}"
: "${DB_USER:?}" "${DB_PASSWORD:?}" "${DB_NAME:?}" "${TOPIC_PREFIX:?}"
: "${TABLE_INCLUDE_LIST:?}" "${SLOT_NAME:?}" "${PUBLICATION_NAME:?}" "${SNAPSHOT_MODE:?}"

until curl -fs "$CONNECT_URL/connectors" >/dev/null; do
  echo "waiting for Kafka Connect at $CONNECT_URL"; sleep 2
done

body=$(cat <<JSON
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "plugin.name": "pgoutput",
  "database.hostname": "$DB_HOST",
  "database.port": "$DB_PORT",
  "database.user": "$DB_USER",
  "database.password": "$DB_PASSWORD",
  "database.dbname": "$DB_NAME",
  "topic.prefix": "$TOPIC_PREFIX",
  "table.include.list": "$TABLE_INCLUDE_LIST",
  "slot.name": "$SLOT_NAME",
  "publication.name": "$PUBLICATION_NAME",
  "publication.autocreate.mode": "filtered",
  "snapshot.mode": "$SNAPSHOT_MODE",
  "tombstones.on.delete": "true",
  "decimal.handling.mode": "string",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "topic.creation.default.replication.factor": "${TOPIC_REPLICATION_FACTOR:-1}",
  "topic.creation.default.partitions": "${TOPIC_PARTITIONS:-1}"
}
JSON
)

curl -fsS -X PUT -H 'Content-Type: application/json' \
  --data "$body" "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" >/dev/null
echo "connector $CONNECTOR_NAME registered"
