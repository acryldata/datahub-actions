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
class AddOwnerEvent:
    owner_id: str
    entity_id: str
    entity_type: EntityType
    original_event: Any


class AddOwner:
    @staticmethod
    def get_subscriptions() -> List[Subscription]:
        return [
            Subscription(
                change_type=ChangeType.UPDATE,
                path_spec="com.linkedin.pegasus2avro.common.Ownership/owners/*/owner",
            ),
            Subscription(event_name=AddOwnerEvent.__name__),
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
    ) -> "AddOwnerEvent":
        return AddOwnerEvent(
            owner_id=semantic_change.attrs[AddOwner.get_urn_key()],
            entity_id=semantic_change.entity_id,
            entity_type=EntityType.from_string(semantic_change.entity_type),
            original_event=raw_change.original_event,
        )

    @staticmethod
    def from_semantic_changes(
        subscription: Optional[Subscription],
        raw_change: RawMetadataChange,
        semantic_changes: Optional[List[SemanticChange]],
    ) -> List["AddOwnerEvent"]:
        if semantic_changes is None:
            return AddOwner.get_semantic_change(subscription, raw_change)

        add_owner_events: List[AddOwnerEvent] = []
        if raw_change.aspect_name not in AddOwner.get_aspect_names():
            return add_owner_events
        for semantic_change in semantic_changes:
            if semantic_change.change_type != AddOwnerEvent.__name__:
                continue
            add_owner_events.append(
                AddOwner._from_semantic_change(semantic_change, raw_change)
            )
        return add_owner_events

    @staticmethod
    def get_semantic_change(
        subscription: Optional[Subscription], raw_change: RawMetadataChange
    ) -> List[AddOwnerEvent]:
        dataset_owner_path_spec = (
            "com.linkedin.pegasus2avro.common.Ownership/owners/*/owner".split("/")
        )

        # we are not applying proposals to non-dataset entities at the moment
        if raw_change.entity_type != EntityType.DATASET:
            return []

        added_owners = []

        for node in raw_change.added:
            added_owners.extend(apply_path_spec_to_delta(node, dataset_owner_path_spec))

        for node in raw_change.changed:
            old_owners = apply_path_spec_to_delta(
                (node[0], node[1][0]), dataset_owner_path_spec
            )
            new_owners = apply_path_spec_to_delta(
                (node[0], node[1][1]), dataset_owner_path_spec
            )
            added_owners.extend([i for i in new_owners if i not in old_owners])

        add_owner_events = []
        for o in added_owners:
            owner_id = o
            add_owner_event = AddOwnerEvent(
                owner_id=owner_id or "unknown_owner",
                entity_id=raw_change.entity_urn,
                entity_type=raw_change.entity_type,
                original_event=raw_change.original_event,
            )
            add_owner_events.append(add_owner_event)
        return add_owner_events
