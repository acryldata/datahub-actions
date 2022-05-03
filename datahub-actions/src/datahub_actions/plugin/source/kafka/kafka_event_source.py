# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

# Confluent important
import confluent_kafka
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from datahub.configuration import ConfigModel
from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.emitter.serialization_helper import post_json_transform

# DataHub imports.
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EntityChangeEventClass,
    GenericPayloadClass,
    MetadataChangeLogClass,
)

from datahub_actions.event.event import EventEnvelope
from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    METADATA_CHANGE_LOG_EVENT_V1_TYPE,
    EntityChangeEvent,
    MetadataChangeLogEvent,
)

# May or may not need these.
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.source.event_source import EventSource

logger = logging.getLogger(__name__)


ENTITY_CHANGE_EVENT_NAME = "entityChangeEvent"
DEFAULT_TOPIC_ROUTES = {
    "mcl": "MetadataChangeLog_Versioned_v1",
    "pe": "PlatformEvent_v1",
}


# Converts a Kafka Message to a Kafka Metadata Dictionary.
def build_kafka_meta(msg: Any) -> dict:
    return {
        "kafka": {
            "topic": msg.topic(),
            "offset": msg.offset(),
            "partition": msg.partition(),
        }
    }


# Converts a Kafka Message to a MetadataChangeLogEvent
def build_metadata_change_log_event(msg: Any) -> MetadataChangeLogEvent:
    value: dict = msg.value()
    return MetadataChangeLogEvent.from_class(
        MetadataChangeLogClass.from_obj(value, True)
    )


# Converts a Kafka Message to an EntityChangeEventClass.
# If this platform event were to contain unions, this type of conversion would not be
# easy, since the underlying
def build_entity_change_event(payload: GenericPayloadClass) -> EntityChangeEvent:
    json_payload = json.loads(payload.get("value"))
    event = EntityChangeEvent.from_class(
        EntityChangeEventClass(
            json_payload["entityType"],
            json_payload["entityUrn"],
            json_payload["category"],
            json_payload["operation"],
            AuditStampClass.from_obj(json_payload["auditStamp"]),
            json_payload["version"],
            json_payload["modifier"] if "modifier" in json_payload else None,
            None,
        )
    )
    # Hack: Since parameters is an "AnyRecord" (arbitrary json) we have to insert into the underlying map directly
    # to avoid validation at object creation time. This means the reader is responsible to understand the serialized JSON format, which
    # is simply PDL serialized to JSON.
    if "parameters" in json_payload:
        event._inner_dict["__parameters_json"] = json_payload["parameters"]
    return event


class KafkaEventSourceConfig(ConfigModel):
    connection: KafkaConsumerConnectionConfig = KafkaConsumerConnectionConfig()
    topic_routes: Optional[Dict[str, str]]


# This is the default Kafka-based Event Source.
@dataclass
class KafkaEventSource(EventSource):

    running = False
    source_config: KafkaEventSourceConfig

    def __init__(self, config: KafkaEventSourceConfig, ctx: PipelineContext):
        self.source_config = config
        self.schema_registry_client = SchemaRegistryClient(
            {"url": self.source_config.connection.schema_registry_url}
        )
        self.consumer: confluent_kafka.Consumer = confluent_kafka.DeserializingConsumer(
            {
                # Provide a custom group id to subcribe to multiple partitions via separate actions pods.
                "group.id": ctx.pipeline_name,
                "bootstrap.servers": self.source_config.connection.bootstrap,
                "enable.auto.commit": False,  # We manually commit offsets.
                "auto.offset.reset": "latest",  # Latest by default, unless overwritten.
                "value.deserializer": AvroDeserializer(
                    schema_registry_client=self.schema_registry_client,
                    return_record_name=True,
                ),
                "session.timeout.ms": "10000",  # 10s timeout.
                "max.poll.interval.ms": "10000",  # 10s poll max.
                **self.source_config.connection.consumer_config,
            }
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "EventSource":
        config = KafkaEventSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def events(self) -> Iterable[EventEnvelope]:
        topic_routes = self.source_config.topic_routes or DEFAULT_TOPIC_ROUTES
        topics_to_subscribe = list(topic_routes.values())
        logger.debug(f"Subscribing to the following topics: {topics_to_subscribe}")
        self.consumer.subscribe(topics_to_subscribe)
        self.running = True
        while self.running:
            msg = self.consumer.poll(timeout=2.0)
            if msg is None:
                continue
            else:
                logger.debug(
                    f"Kafka msg received: {msg.topic()}, {msg.partition()}, {msg.offset()}"
                )
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.debug(
                        "%% %s [%d] reached end at offset %d\n"
                        % (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                if "mcl" in topic_routes and msg.topic() == topic_routes["mcl"]:
                    yield from self.handle_mcl(msg)
                elif "pe" in topic_routes and msg.topic() == topic_routes["pe"]:
                    yield from self.handle_pe(msg)

    @staticmethod
    def handle_mcl(msg: Any) -> Iterable[EventEnvelope]:
        metadata_change_log_event = build_metadata_change_log_event(msg)
        kafka_meta = build_kafka_meta(msg)
        yield EventEnvelope(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE, metadata_change_log_event, kafka_meta
        )

    @staticmethod
    def handle_pe(msg: Any) -> Iterable[EventEnvelope]:
        value: dict = msg.value()
        payload: GenericPayloadClass = GenericPayloadClass.from_obj(
            post_json_transform(value["payload"])
        )
        if ENTITY_CHANGE_EVENT_NAME == value["name"]:
            event = build_entity_change_event(payload)
            kafka_meta = build_kafka_meta(msg)
            yield EventEnvelope(ENTITY_CHANGE_EVENT_V1_TYPE, event, kafka_meta)

    def close(self) -> None:
        if self.consumer:
            self.running = False
            self.consumer.close()

    def ack(self, event: EventEnvelope) -> None:
        self.consumer.commit(
            offsets=[
                TopicPartition(
                    event.meta["kafka"]["topic"],
                    event.meta["kafka"]["partition"],
                    event.meta["kafka"]["offset"] + 1,
                )
            ]
        )
        logger.debug(
            f"Successfully committed offsets at message: topic: {event.meta['kafka']['topic']}, partition: {event.meta['kafka']['partition']}, offset: {event.meta['kafka']['offset']}"
        )
