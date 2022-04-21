from datahub.ingestion.api.registry import PluginRegistry

from datahub_actions.source.event_source import EventSource
from datahub_actions.source.kafka_event_source import KafkaEventSource

event_source_registry = PluginRegistry[EventSource]()
event_source_registry.register("kafka", KafkaEventSource)
