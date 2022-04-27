from datahub.ingestion.api.registry import PluginRegistry

from datahub_actions.plugin.transform.filter.filter_transformer import FilterTransformer
from datahub_actions.transform.transformer import Transformer

transformer_registry = PluginRegistry[Transformer]()
transformer_registry.register_from_entrypoint("datahub_actions.transformer.plugins")
transformer_registry.register("__filter", FilterTransformer)
