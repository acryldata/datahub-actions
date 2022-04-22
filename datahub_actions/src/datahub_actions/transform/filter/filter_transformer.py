import logging
from typing import Any, Dict, Optional

import pydantic
from datahub.configuration import ConfigModel

from datahub_actions.events.event import EnvelopedEvent, EventType
from datahub_actions.pipeline.context import ActionContext
from datahub_actions.transform.event_transformer import Transformer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FilterTransformerConfig(ConfigModel):
    event_type: str
    fields: Dict[str, Any]

    @pydantic.validator("event_type", always=True)
    def event_type_is_valid(cls, v, values, **kwargs):
        valid_events = [e.value for e in EventType]
        if v not in valid_events:
            raise pydantic.ConfigError(
                f"Valid event_type are {valid_events} - {v} is invalid"
            )


class FilterTransformer(Transformer):
    def __init__(self, config: FilterTransformerConfig):
        self.config: FilterTransformerConfig = config

    @classmethod
    def create(cls, config_dict: dict, ctx: ActionContext) -> "Transformer":
        config = FilterTransformerConfig.parse_obj(config_dict)
        return cls(config)

    def transform(self, event: EnvelopedEvent) -> Optional[EnvelopedEvent]:
        # TODO: Implement the events filter!
        return event
