#!/usr/bin/env bash

set -euo pipefail

: "${CONNECT_URL:?CONNECT_URL is required}"
: "${INVENTORY_DB_HOST:?INVENTORY_DB_HOST is required}"
: "${INVENTORY_DB_PORT:?INVENTORY_DB_PORT is required}"
: "${INVENTORY_DB_NAME:?INVENTORY_DB_NAME is required}"
: "${INVENTORY_DB_USER:?INVENTORY_DB_USER is required}"
: "${INVENTORY_DB_PASSWORD:?INVENTORY_DB_PASSWORD is required}"

connector_url="${CONNECT_URL%/}/connectors/eshop-inventory-cdc/config"

payload="$(jq -n \
  --arg host "$INVENTORY_DB_HOST" \
  --arg port "$INVENTORY_DB_PORT" \
  --arg database "$INVENTORY_DB_NAME" \
  --arg user "$INVENTORY_DB_USER" \
  --arg password "$INVENTORY_DB_PASSWORD" \
  '{
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": $host,
    "database.port": $port,
    "database.user": $user,
    "database.password": $password,
    "database.dbname": $database,
    "topic.prefix": "eshop",
    "plugin.name": "pgoutput",
    "slot.name": "eshop_inventory_cdc",
    "publication.name": "eshop_inventory_publication",
    "publication.autocreate.mode": "filtered",
    "table.include.list": "inventory.inventory_movements,inventory.inventory_balances",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "snapshot.mode": "when_needed",
    "tombstones.on.delete": "true",
    "heartbeat.interval.ms": "10000"
  }')"

curl --fail --silent --show-error \
  --request PUT \
  --header 'Content-Type: application/json' \
  --data-binary "$payload" \
  "$connector_url"

printf '\nInventory CDC connector registered at %s\n' "$connector_url"
