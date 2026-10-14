#!/bin/sh
# Registers (or updates) the Debezium Postgres connector. PUT on
# /connectors/<name>/config is idempotent, so this is safe on every deploy.
# Everything comes from the environment; nothing is hard coded here.
#
# The CQRS read model copies rows from these topics, so two settings matter:
#  - REPLICA IDENTITY FULL on every captured table (replica.identity.autoset.values
#    below sets it). Without it an UPDATE leaves out TOAST columns that did not
#    change, and the read model would be handed a placeholder instead of the value.
#    The app also refuses to start replicating a table that is not FULL.
#  - time-like columns are mapped from Debezium's own types, so models must use
#    timestamptz (not timestamp, time or interval); the app checks this too.
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
  "topic.creation.default.partitions": "${TOPIC_PARTITIONS:-3}",
  "replica.identity.autoset.values": "${REPLICA_IDENTITY_AUTOSET:-.*:FULL}"
}
JSON
)

curl -fsS -X PUT -H 'Content-Type: application/json' \
  --data "$body" "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" >/dev/null
echo "connector $CONNECTOR_NAME registered"
