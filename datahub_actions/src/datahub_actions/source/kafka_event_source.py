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
from typing import Callable, Dict, Iterable, List

# Confluent important
import confluent_kafka
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from datahub.configuration import ConfigModel
from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.emitter.mce_builder import DEFAULT_ENV

# DataHub imports.
from datahub.metadata.schema_classes import MetadataChangeProposalClass

from datahub_actions.events.event import EnvelopedEvent, EventType

# May or may not need these.
from datahub_actions.pipeline.context import ActionContext
from datahub_actions.source.event_source import Event, EventSource

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Converts a Kafka Message to a Kafka Metadata Dictionary.
def build_kafka_meta(msg) -> dict:
    return {
        "kafka": {
            "topic": msg.topic(),
            "offset": msg.offset(),
            "partition": msg.partition(),
        }
    }


# Converts a Kafka Message to a MetadataChangeLogEvent
def build_metadata_change_log_event(msg) -> MetadataChangeProposalClass:
    pass


# Converts a Kafka Message to a MetadataChangeLogEvent
def build_platform_event(msg):
    pass


class KafkaEventSourceConfig(ConfigModel):
    env: str = DEFAULT_ENV
    connection: KafkaConsumerConnectionConfig = KafkaConsumerConnectionConfig()
    auto_offset_reset: str = "latest"
    topic_routes: Dict[str, str] = {"mae": "Failure"}


@dataclass
class KafkaEventSource(EventSource):

    source_config: KafkaEventSourceConfig
    callbacks: List[Callable[Event]]

    def __init__(self, config: KafkaEventSourceConfig):
        self.source_config = config
        self.schema_registry_client = SchemaRegistryClient(
            {"url": self.source_config.connection.schema_registry_url}
        )
        self.consumer: confluent_kafka.Consumer = confluent_kafka.DeserializingConsumer(
            {
                "group.id": "test",  # TODO: Figure out if this is causing the issue.
                "bootstrap.servers": self.source_config.connection.bootstrap,
                "enable.auto.commit": False,
                "auto.offset.reset": self.source_config.auto_offset_reset,
                "value.deserializer": AvroDeserializer(
                    schema_registry_client=self.schema_registry_client,
                    return_record_name=True,
                ),
                "session.timeout.ms": "10000",
                "max.poll.interval.ms": "10000",
                **self.source_config.connection.consumer_config,
            }
        )

    @classmethod
    def create(cls, config_dict, ctx: ActionContext):
        config = KafkaEventSourceConfig.parse_obj(config_dict)
        assert (
            "mcl" in config.topic_routes
        ), "topic_routes must contain an entry for mae (the metadata log event topic)"
        assert (
            "pe" in config.topic_routes
        ), "topic_routes must contain an entry for pe (the platform event topic)"
        return cls(config)

    def events(self) -> Iterable[EnvelopedEvent]:
        topic_routes = self.source_config.topic_routes
        assert "mae" in topic_routes
        assert "mcl" in topic_routes
        assert "pe" in topic_routes
        logger.info(
            f"Will subscribe to {topic_routes['mae']}, {topic_routes['mcl']}, {topic_routes['pe']}"
        )
        self.consumer.subscribe(
            [topic_routes["mae"], topic_routes["mcl"], topic_routes["pe"]]
        )
        running = True
        while running:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            else:
                logger.info(
                    f"Msg received: {msg.topic()}, {msg.partition()}, {msg.offset()}"
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
                # if msg.topic() == topic_routes["mae"]:
                # pass
                # yield from self._handle_mae(msg.value())
                # Create a record envelope from MAE.
                if msg.topic() == topic_routes["mcl"]:
                    yield from self._handle_mcl(msg)
                elif msg.topic() == topic_routes["pe"]:
                    yield from self._handle_pe(msg)
                    # yield from self._handle_mcl(msg.value())
                    # Create a record envelope from PE.

    def _handle_mcl(self, msg):
        metadata_change_log_event = build_metadata_change_log_event(msg.value())
        kafka_meta = build_kafka_meta(msg)
        yield EnvelopedEvent(
            EventType.METADATA_CHANGE_LOG, metadata_change_log_event, kafka_meta
        )

    def _handle_pe(self, msg):
        # TODO
        pass
        # platform_event = build_platform_event(msg.value())
        # kafka_meta = build_kafka_meta(msg)
        # yield EnvelopedEvent(EventType.METADATA_CHANGE_LOG, metadata_change_log_event, kafka_meta)
        # yield EnvelopedEvent(platform_event, kafka_meta)

    def close(self):
        if self.consumer:
            self.consumer.close()

    def ack(self, event: EnvelopedEvent):
        # Somehow we need to ack this particular event.
        # TODO: Commit offsets to kafka explicitly.
        pass
