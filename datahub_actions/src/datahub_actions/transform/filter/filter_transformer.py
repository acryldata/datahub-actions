import logging
from typing import Any, Dict, Optional

from datahub.configuration import ConfigModel

from datahub_actions.events.event import EnvelopedEvent
from datahub_actions.pipeline.context import ActionContext
from datahub_actions.transform.event_transformer import Transformer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FilterTransformerConfig(ConfigModel):
    event_type: str
    fields: Dict[str, Any]


class FilterTransformer(Transformer):

    event_type: str
    fields: Dict[str, Any]

    def __init__(self, config: FilterTransformerConfig):
        self.event_type = config.event_type
        self.fields = config.fields

    @classmethod
    def create(cls, config_dict: dict, ctx: ActionContext) -> "Transformer":
        config = FilterTransformerConfig.parse_obj(config_dict)
        return cls(config)

    def transform(self, event: EnvelopedEvent) -> Optional[EnvelopedEvent]:
        # TODO: Implement the events filter!
        return event
