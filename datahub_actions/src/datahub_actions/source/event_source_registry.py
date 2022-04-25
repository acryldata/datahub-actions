from datahub.ingestion.api.registry import PluginRegistry

from datahub_actions.plugin.source.kafka_event_source import KafkaEventSource
from datahub_actions.source.event_source import EventSource

event_source_registry = PluginRegistry[EventSource]()
event_source_registry.register_from_entrypoint("datahub_actions.source.plugins")
event_source_registry.register("kafka", KafkaEventSource)
