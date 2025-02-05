#!/bin/bash

CONFIG_FILE="/etc/datahub/actions/system/conf/executor.yaml"

# Copy the original template file to the actual configuration file
cp /etc/datahub/actions/system/conf/executor.yaml.template $CONFIG_FILE
CONSUMER_CONFIG=""
for var in $(env | grep ^KAFKA_PROPERTIES_ | awk -F= '{print $1}'); do
  key=$(echo $var | sed 's/^KAFKA_PROPERTIES_//g' | tr '[:upper:]' '[:lower:]' | tr '_' '.')
  value=${!var}
  CONSUMER_CONFIG="${CONSUMER_CONFIG}\n        ${key}: ${value}"
done

CONSUMER_CONFIG=$(echo "$CONSUMER_CONFIG" | cut -c4-)

# If consumer_config is not empty, insert it *before* topic_routes
if [ ! -z "$CONSUMER_CONFIG" ]; then
  sed -i "/topic_routes:/i\      consumer_config:" $CONFIG_FILE
  sed -i "/consumer_config:/a\ ${CONSUMER_CONFIG}" $CONFIG_FILE
fi


# Execute the provided command
exec "$@"