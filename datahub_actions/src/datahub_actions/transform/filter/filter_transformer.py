import logging
from typing import Any, Dict, Optional

from datahub.configuration import ConfigModel
from datahub.ingestion.api.common import RecordEnvelope

from datahub_actions.events.event import Event
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
        self.type = config.type
        self.fields = config.fields

    @classmethod
    def create(cls, config: dict, ctx: ActionContext) -> "FilterTransformer":
        config = FilterTransformerConfig.parse_obj(config)
        return cls(config)

    def transform(
        self, event: RecordEnvelope[Event]
    ) -> Optional[RecordEnvelope[Event]]:
        return event
