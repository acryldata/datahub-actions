#!/bin/bash

CONFIG_FILE="/etc/datahub/actions/system/conf/executor.yaml"

# Copy the original template file to the actual configuration file
cp /etc/datahub/actions/system/conf/executor.yaml.template $CONFIG_FILE

# Convert consumer_config environment variables to YAML format
CONSUMER_CONFIG=""
for var in $(env | grep ^KAFKA_PROPERTIES_ | awk -F= '{print $1}'); do
  key=$(echo $var | sed 's/^KAFKA_PROPERTIES_//g' | tr '[:upper:]' '[:lower:]' | tr '_' '.')
  value=${!var}
  CONSUMER_CONFIG="${CONSUMER_CONFIG}\n      ${key}: \"${value}\""
done

# If consumer_config is not empty, insert it into the YAML file
if [ ! -z "$CONSUMER_CONFIG" ]; then
  sed -i "/topic_routes:/a\ \ \ consumer_config: ${CONSUMER_CONFIG}" $CONFIG_FILE
fi

# Execute the provided command
exec "$@"
