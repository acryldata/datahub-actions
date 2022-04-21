from datahub.ingestion.api.registry import PluginRegistry

from datahub_actions.transform.event_transformer import EventTransformer
from datahub_actions.transform.filter.filter_transformer import FilterTransformer

event_transformer_registry = PluginRegistry[EventTransformer]()
event_transformer_registry.register("__filter", FilterTransformer)
