import logging
from dataclasses import dataclass
from typing import Any, List, Optional

from datahub_actions.api.action_core import (
    ChangeType,
    EntityType,
    RawMetadataChange,
    SemanticChange,
    Subscription,
)
from datahub_actions.utils.change_processor import apply_path_spec_to_delta
from datahub_actions.utils.delta_extractor_mcl import ASPECT_OWNERSHIP

logger = logging.getLogger(__name__)


@dataclass
class RemoveOwnerEvent:
    owner_id: str
    entity_id: str
    entity_type: EntityType
    original_event: Any


class RemoveOwner:
    @staticmethod
    def get_subscriptions() -> List[Subscription]:
        return [
            Subscription(
                change_type=ChangeType.UPDATE,
                path_spec="com.linkedin.pegasus2avro.common.Ownership/owners/*/owner",
            ),
            Subscription(event_name=RemoveOwnerEvent.__name__),
        ]

    @staticmethod
    def get_urn_key() -> str:
        return "owner"

    @staticmethod
    def get_aspect_names() -> List[str]:
        return [ASPECT_OWNERSHIP]

    @staticmethod
    def _from_semantic_change(
        semantic_change: SemanticChange, raw_change: RawMetadataChange
    ) -> "RemoveOwnerEvent":
        return RemoveOwnerEvent(
            owner_id=semantic_change.attrs[RemoveOwner.get_urn_key()],
            entity_id=semantic_change.entity_id,
            entity_type=EntityType.from_string(semantic_change.entity_type),
            original_event=raw_change.original_event,
        )

    @staticmethod
    def from_semantic_changes(
        subscription: Optional[Subscription],
        raw_change: RawMetadataChange,
        semantic_changes: Optional[List[SemanticChange]],
    ) -> List["RemoveOwnerEvent"]:
        if semantic_changes is None:
            return RemoveOwner.get_semantic_change(subscription, raw_change)

        remove_owner_events: List[RemoveOwnerEvent] = []
        if raw_change.aspect_name not in RemoveOwner.get_aspect_names():
            return remove_owner_events
        for semantic_change in semantic_changes:
            if semantic_change.change_type != RemoveOwnerEvent.__name__:
                continue
            remove_owner_events.append(
                RemoveOwner._from_semantic_change(semantic_change, raw_change)
            )
        return remove_owner_events

    @staticmethod
    def get_semantic_change(
        subscription: Optional[Subscription], raw_change: RawMetadataChange
    ) -> List[RemoveOwnerEvent]:
        dataset_owner_path_spec = (
            "com.linkedin.pegasus2avro.common.Ownership/owners/*/owner".split("/")
        )

        # we are not applying proposals to non-dataset entities at the moment
        if raw_change.entity_type != EntityType.DATASET:
            return []

        removed_owners = []

        for node in raw_change.removed:
            removed_owners.extend(
                apply_path_spec_to_delta(node, dataset_owner_path_spec)
            )

        for node in raw_change.changed:
            old_owners = apply_path_spec_to_delta(
                (node[0], node[1][0]), dataset_owner_path_spec
            )
            new_owners = apply_path_spec_to_delta(
                (node[0], node[1][1]), dataset_owner_path_spec
            )
            removed_owners.extend([i for i in old_owners if i not in new_owners])

        remove_owner_events = []
        for o in removed_owners:
            owner_id = o
            remove_owner_event = RemoveOwnerEvent(
                owner_id=owner_id or "unknown_owner",
                entity_id=raw_change.entity_urn,
                entity_type=raw_change.entity_type,
                original_event=raw_change.original_event,
            )
            remove_owner_events.append(remove_owner_event)
        return remove_owner_events
