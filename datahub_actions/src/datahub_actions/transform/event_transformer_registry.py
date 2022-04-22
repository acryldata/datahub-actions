from datahub.ingestion.api.registry import PluginRegistry

from datahub_actions.transform.event_transformer import Transformer
from datahub_actions.transform.filter.filter_transformer import FilterTransformer

event_transformer_registry = PluginRegistry[Transformer]()
event_transformer_registry.register("__filter", FilterTransformer)
