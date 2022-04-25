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

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable

# Confluent important
import confluent_kafka
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from datahub.configuration import ConfigModel
from datahub.configuration.kafka import KafkaConsumerConnectionConfig

# DataHub imports.
from datahub.metadata.schema_classes import (
    GenericAspectClass,
    MetadataChangeProposalClass,
)

from datahub_actions.events.event import EnvelopedEvent, EventType

# May or may not need these.
from datahub_actions.pipeline.context import ActionContext
from datahub_actions.source.event_source import EventSource

logger = logging.getLogger(__name__)


ENTITY_CHANGE_EVENT_NAME = "entityChangeEvent"


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
def build_metadata_change_log_event(msg: Any) -> MetadataChangeProposalClass:
    # TODO: Map MCL to MetadataChangeLogClass
    value: dict = msg.value()
    return MetadataChangeProposalClass(
        value["entityType"],
        value["changeType"],
        None,  # TODO
        value["entityUrn"],
        None,  # TODO
        value["aspectName"],
        GenericAspectClass(
            value["aspect"][1]["contentType"], value["aspect"][1]["value"]
        )
        if value["aspect"] is not None
        else None,
        None,  # TODO
    )


# Converts a Kafka Message to a MetadataChangeLogEvent
def build_entity_change_event(msg: Any) -> MetadataChangeProposalClass:
    # TODO: Fix to return Entity Change Event type.
    return MetadataChangeProposalClass(
        "test",
        "UPSERT",
        None,  # TODO
        "urn:li:dataset:1",
        None,  # TODO
        "aspect",
        None,
        None,  # TODO
    )


class KafkaEventSourceConfig(ConfigModel):
    connection: KafkaConsumerConnectionConfig = KafkaConsumerConnectionConfig()
    topic_routes: Dict[str, str]


@dataclass
class KafkaEventSource(EventSource):

    running = False
    source_config: KafkaEventSourceConfig

    def __init__(self, config: KafkaEventSourceConfig, ctx: ActionContext):
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
    def create(cls, config_dict: dict, ctx: ActionContext) -> "EventSource":
        config = KafkaEventSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def events(self) -> Iterable[EnvelopedEvent]:
        topic_routes = self.source_config.topic_routes
        topics_to_subscribe = list(topic_routes.values())
        logger.debug(f"Subscribing to the following topics: {topics_to_subscribe}")
        self.consumer.subscribe(topics_to_subscribe)
        self.running = True
        while self.running:
            msg = self.consumer.poll(timeout=2.0)
            if msg is None:
                continue
            else:
                # TODO: Make this debug.
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
                    yield from self._handle_mcl(msg)
                elif "pe" in topic_routes and msg.topic() == topic_routes["pe"]:
                    yield from self._handle_pe(msg)

    def _handle_mcl(self, msg: Any) -> Iterable[EnvelopedEvent]:
        metadata_change_log_event = build_metadata_change_log_event(msg)
        kafka_meta = build_kafka_meta(msg)
        yield EnvelopedEvent(
            EventType.METADATA_CHANGE_LOG, metadata_change_log_event, kafka_meta
        )

    def _handle_pe(self, msg: Any) -> Iterable[EnvelopedEvent]:
        value: dict = msg.value()
        if ENTITY_CHANGE_EVENT_NAME == value["name"]:
            event = build_entity_change_event(msg)
            kafka_meta = build_kafka_meta(msg)
            yield EnvelopedEvent(EventType.ENTITY_CHANGE_EVENT, event, kafka_meta)

    def close(self) -> None:
        if self.consumer:
            self.running = False
            self.consumer.close()

    def ack(self, event: EnvelopedEvent) -> None:
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
