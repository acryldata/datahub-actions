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
from datahub_actions.utils.delta_extractor_mcl import (
    ASPECT_EDITABLE_SCHEMAMETADATA,
    ASPECT_TAGS,
)

logger = logging.getLogger(__name__)


@dataclass
class AddTagEvent:
    tag_id: str
    entity_id: str
    entity_type: EntityType
    original_event: Any


class AddTag:
    @staticmethod
    def get_subscriptions() -> List[Subscription]:
        return [
            Subscription(
                change_type=ChangeType.UPDATE,
                path_spec="com.linkedin.pegasus2avro.schema.EditableSchemaMetadata/editableSchemaFieldInfo/*/globalTags",
            ),
            Subscription(event_name=AddTagEvent.__name__),
        ]

    @staticmethod
    def get_urn_key() -> str:
        return "tag"

    @staticmethod
    def get_aspect_names() -> List[str]:
        return [ASPECT_TAGS, ASPECT_EDITABLE_SCHEMAMETADATA]

    @staticmethod
    def _from_semantic_change(
        semantic_change: SemanticChange, raw_change: RawMetadataChange
    ) -> "AddTagEvent":
        return AddTagEvent(
            tag_id=semantic_change.attrs[AddTag.get_urn_key()],
            entity_id=semantic_change.entity_id,
            entity_type=EntityType.from_string(semantic_change.entity_type),
            original_event=raw_change.original_event,
        )

    @staticmethod
    def from_semantic_changes(
        subscription: Optional[Subscription],
        raw_change: RawMetadataChange,
        semantic_changes: Optional[List[SemanticChange]],
    ) -> List["AddTagEvent"]:
        if semantic_changes is None:
            return AddTag.get_semantic_change(subscription, raw_change)

        add_tag_events: List[AddTagEvent] = []
        if raw_change.aspect_name not in AddTag.get_aspect_names():
            return add_tag_events
        for semantic_change in semantic_changes:
            if semantic_change.change_type != AddTagEvent.__name__:
                continue
            try:
                add_tag_events.append(
                    AddTag._from_semantic_change(semantic_change, raw_change)
                )
            except KeyError:
                pass
        return add_tag_events

    @staticmethod
    def get_semantic_change(
        subscription: Optional[Subscription], raw_change: RawMetadataChange
    ) -> List[AddTagEvent]:
        field_tag_path_spec = "com.linkedin.pegasus2avro.schema.EditableSchemaMetadata/editableSchemaFieldInfo/* groupBy(fieldPath)/globalTags/tags/*/tag".split(
            "/"
        )
        dataset_tag_path_spec = (
            "com.linkedin.pegasus2avro.common.GlobalTags/tags/*/tag".split("/")
        )

        if raw_change.entity_type != EntityType.DATASET:
            return []

        added_tags = []

        for node in raw_change.added:
            added_tags.extend(apply_path_spec_to_delta(node, dataset_tag_path_spec))

        for node in raw_change.added:
            added_tags.extend(apply_path_spec_to_delta(node, field_tag_path_spec))

        for node in raw_change.changed:
            old_tags = apply_path_spec_to_delta(
                (node[0], node[1][0]), field_tag_path_spec
            )
            new_tags = apply_path_spec_to_delta(
                (node[0], node[1][1]), field_tag_path_spec
            )
            added_tags.extend([i for i in new_tags if i not in old_tags])

        add_tag_events = []
        for t in added_tags:
            tag_id = t
            add_tag_event = AddTagEvent(
                tag_id=tag_id or "unknown_tagId",
                entity_id=raw_change.entity_urn,
                entity_type=raw_change.entity_type,
                original_event=raw_change.original_event,
            )
            add_tag_events.append(add_tag_event)
        return add_tag_events
