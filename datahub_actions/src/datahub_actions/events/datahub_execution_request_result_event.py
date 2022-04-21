import logging
from dataclasses import dataclass
from typing import Any, List, Optional

from datahub_actions.api.action_core import (
    EntityType,
    RawMetadataChange,
    SemanticChange,
    Subscription,
)
from datahub_actions.utils.delta_extractor_mcl import (
    ASPECT_DATAHUB_EXECUTION_REQUEST_RESULT,
)

logger = logging.getLogger(__name__)


@dataclass
class DataHubExecutionRequestResultEvent:
    execution_id: str
    entity_type: EntityType
    status: str
    original_event: Any


class DataHubExecutionRequestResult:
    @staticmethod
    def get_subscriptions() -> List[Subscription]:
        return [
            Subscription(event_name=DataHubExecutionRequestResultEvent.__name__),
        ]

    @staticmethod
    def get_urn_key() -> str:
        return ""

    @staticmethod
    def get_aspect_names() -> List[str]:
        return [ASPECT_DATAHUB_EXECUTION_REQUEST_RESULT]

    @staticmethod
    def _from_semantic_change(
        semantic_change: SemanticChange, raw_change: RawMetadataChange
    ) -> "DataHubExecutionRequestResultEvent":
        return DataHubExecutionRequestResultEvent(
            execution_id=str(semantic_change.entity_key.get("id")),
            entity_type=EntityType.from_string(semantic_change.entity_type),
            status=str(semantic_change.attrs.get("status")),
            original_event=raw_change.original_event,
        )

    @staticmethod
    def from_semantic_changes(
        subscription: Optional[Subscription],
        raw_change: RawMetadataChange,
        semantic_changes: Optional[List[SemanticChange]],
    ) -> List["DataHubExecutionRequestResultEvent"]:
        result: List[DataHubExecutionRequestResultEvent] = []
        if semantic_changes is None:
            return result
        for semantic_change in semantic_changes:
            if (
                semantic_change.change_type
                != DataHubExecutionRequestResultEvent.__name__
            ):
                continue
            try:
                result.append(
                    DataHubExecutionRequestResult._from_semantic_change(
                        semantic_change, raw_change
                    )
                )
            except (KeyError, AttributeError):
                pass
        return result

    @staticmethod
    def get_semantic_change(
        subscription: Optional[Subscription], raw_change: RawMetadataChange
    ) -> List[DataHubExecutionRequestResultEvent]:
        return []
