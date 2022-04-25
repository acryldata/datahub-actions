from datahub.ingestion.api.registry import PluginRegistry

from datahub_actions.plugin.transform.filter.filter_transformer import FilterTransformer
from datahub_actions.transform.event_transformer import Transformer

event_transformer_registry = PluginRegistry[Transformer]()
event_transformer_registry.register_from_entrypoint(
    "datahub_actions.transformer.plugins"
)
event_transformer_registry.register("__filter", FilterTransformer)
