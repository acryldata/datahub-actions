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
    ASPECT_GLOSSARY_TERMS,
)

logger = logging.getLogger(__name__)


@dataclass
class AddTermEvent:
    term_id: str
    entity_id: str
    entity_type: EntityType
    original_event: Any


class AddTerm:
    """A Helper Class to subscribe to AddTerm events"""

    @staticmethod
    def get_subscriptions() -> List[Subscription]:
        return [
            Subscription(
                change_type=ChangeType.UPDATE,
                path_spec="com.linkedin.pegasus2avro.schema.EditableSchemaMetadata/editableSchemaFieldInfo/*/globalTags",
            ),
            Subscription(event_name=AddTermEvent.__name__),
        ]

    @staticmethod
    def get_urn_key() -> str:
        return "urn"

    @staticmethod
    def get_aspect_names() -> List[str]:
        return [ASPECT_GLOSSARY_TERMS, ASPECT_EDITABLE_SCHEMAMETADATA]

    @staticmethod
    def _from_semantic_change(
        semantic_change: SemanticChange, raw_change: RawMetadataChange
    ) -> "AddTermEvent":
        return AddTermEvent(
            term_id=semantic_change.attrs[AddTerm.get_urn_key()],
            entity_id=semantic_change.entity_id,
            entity_type=EntityType.from_string(semantic_change.entity_type),
            original_event=raw_change.original_event,
        )

    @staticmethod
    def from_semantic_changes(
        subscription: Optional[Subscription],
        raw_change: RawMetadataChange,
        semantic_changes: Optional[List[SemanticChange]],
    ) -> List["AddTermEvent"]:
        if semantic_changes is None:
            return AddTerm.get_semantic_change(subscription, raw_change)

        add_term_events: List[AddTermEvent] = []
        if raw_change.aspect_name not in AddTerm.get_aspect_names():
            return add_term_events
        for semantic_change in semantic_changes:
            if semantic_change.change_type != AddTermEvent.__name__:
                continue
            try:
                add_term_events.append(
                    AddTerm._from_semantic_change(semantic_change, raw_change)
                )
            except KeyError:
                pass
        return add_term_events

    @staticmethod
    def get_semantic_change(
        subscription: Optional[Subscription], raw_change: RawMetadataChange
    ) -> List[AddTermEvent]:
        field_term_path_spec = "com.linkedin.pegasus2avro.schema.EditableSchemaMetadata/editableSchemaFieldInfo/* groupBy(fieldPath)/glossaryTerms/terms/*/urn".split(
            "/"
        )
        dataset_term_path_spec = (
            "com.linkedin.pegasus2avro.common.GlossaryTerms/terms/*/urn".split("/")
        )

        if raw_change.entity_type != EntityType.DATASET:
            return []

        added_terms = []

        for node in raw_change.added:
            added_terms.extend(apply_path_spec_to_delta(node, dataset_term_path_spec))

        for node in raw_change.added:
            added_terms.extend(apply_path_spec_to_delta(node, field_term_path_spec))

        for node in raw_change.changed:
            old_terms = apply_path_spec_to_delta(
                (node[0], node[1][0]), field_term_path_spec
            )
            new_terms = apply_path_spec_to_delta(
                (node[0], node[1][1]), field_term_path_spec
            )
            added_terms.extend([i for i in new_terms if i not in old_terms])

        add_term_events = []
        for t in added_terms:
            tag_id = t
            add_term_event = AddTermEvent(
                term_id=tag_id or "unknown_termId",
                entity_id=raw_change.entity_urn,
                entity_type=raw_change.entity_type,
                original_event=raw_change.original_event,
            )
            add_term_events.append(add_term_event)
        return add_term_events
