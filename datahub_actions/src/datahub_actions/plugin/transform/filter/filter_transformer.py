import logging
from typing import Any, Dict, List, Optional

import pydantic
from datahub.configuration import ConfigModel

from datahub_actions.event.event import EventEnvelope, EventType
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.transform.transformer import Transformer

logger = logging.getLogger(__name__)


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
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        config = FilterTransformerConfig.parse_obj(config_dict)
        return cls(config)

    def transform(self, env_event: EventEnvelope) -> Optional[EventEnvelope]:

        logger.info(f"Preparing to filter event {env_event}")

        for key, val in self.config.fields.items():
            if not self._matches(val, env_event.event.get(key)):
                return None
        return env_event

    def _matches(self, match_val: Any, match_val_to: Any) -> bool:
        if isinstance(match_val, dict):
            return self._matches_dict(match_val, match_val_to)
        if isinstance(match_val, list):
            return self._matches_list(match_val, match_val_to)
        return match_val == match_val_to

    def _matches_list(self, match_filters: List, match_with: Any) -> bool:
        """When matching lists we do ANY not ALL match"""
        if not isinstance(match_with, str):
            return False
        for filter in match_filters:
            if filter == match_with:
                return True
        return False

    def _matches_dict(self, match_filters: Dict[str, Any], match_with: Any) -> bool:
        if not isinstance(match_with, dict):
            return False
        for key, val in match_filters.items():
            curr = match_with.get(key)
            if not self._matches(val, curr):
                return False
        return True
