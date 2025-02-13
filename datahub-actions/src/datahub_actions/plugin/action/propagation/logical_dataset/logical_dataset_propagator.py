import json
import logging
import time
from typing import Iterable, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import GenericAspectClass
from datahub.utilities.urns.urn import Urn

from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.plugin.action.propagation.propagation_utils import (
    PropagationDirective,
    SourceDetails,
)
from datahub_actions.plugin.action.propagation.propagator import (
    EntityPropagator,
    EntityPropagatorConfig,
)

logger = logging.getLogger(__name__)


class DocsPropagatorConfig(EntityPropagatorConfig):
    columns_enabled: bool = True


class LogicalDatasetPropagationDirective(PropagationDirective):
    aspects: List[GenericAspectClass] = []


class DocsPropagator(EntityPropagator):
    def __init__(
        self, action_urn: str, graph: AcrylDataHubGraph, config: DocsPropagatorConfig
    ) -> None:
        super().__init__(action_urn, graph, config)
        self.config = config
        self.graph = graph
        self.actor_urn = action_urn

        self.mcl_processor.register_processor(
            entity_type="dataset",
            aspect="logicalParent",
            processor=self.process_mce,
        )

    def process_mce(
        self, event: EventEnvelope
    ) -> Optional[LogicalDatasetPropagationDirective]:
        """
        Return a tag urn to propagate or None if no propagation is desired
        """
        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            assert self.graph is not None
            # assert isinstance(self.config, TagPropagatorConfig)

            semantic_event = event.event
            if semantic_event.category == "TAG" and (
                semantic_event.operation == "ADD"
                or semantic_event.operation == "REMOVE"
            ):
                logger.info(f"Processing TAG event {event}")
                assert semantic_event.modifier, "tag urn should be present"
                propagate = self.config.enabled

                parameters = (
                    semantic_event.parameters
                    if semantic_event.parameters is not None
                    else semantic_event._inner_dict.get("__parameters_json", {})
                )
                context_str = (
                    parameters.get("context")
                    if parameters.get("context") is not None
                    else "{}"
                )

                source_details_parsed = SourceDetails.parse_obj(json.loads(context_str))
                propagation_relationships = self.get_propagation_relationships(
                    source_details_parsed
                )

                origin = parameters.get("origin")
                origin = origin or semantic_event.entityUrn

                via = (
                    semantic_event.entityUrn
                    if source_details_parsed.origin != semantic_event.entityUrn
                    else None
                )

                if propagate:
                    return LogicalDatasetPropagationDirective(
                        propagate=True,
                        aspects=[],
                        operation=semantic_event.operation,
                        entity=semantic_event.entityUrn,
                        via=via,
                        relationships=propagation_relationships,
                        origin=origin,
                        propagation_depth=(
                            source_details_parsed.propagation_depth + 1
                            if source_details_parsed.propagation_depth
                            else 1
                        ),
                        actor=(
                            semantic_event.auditStamp.actor
                            if semantic_event.auditStamp
                            else self.actor_urn
                        ),
                        propagation_started_at=(
                            source_details_parsed.propagation_started_at
                            if source_details_parsed.propagation_started_at
                            else int(time.time() * 1000.0)
                        ),
                    )
                else:
                    return LogicalDatasetPropagationDirective(
                        propagate=False,
                        actor=self.actor_urn,
                        aspects=[],
                        operation=semantic_event.modifier,
                        entity=semantic_event.entityUrn,
                        relationships=propagation_relationships,
                        origin=semantic_event.entityUrn,
                    )
        return None

    def create_property_change_proposal(
        self,
        propagation_directive: PropagationDirective,
        entity_urn: Urn,
        context: SourceDetails,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        assert isinstance(propagation_directive, LogicalDatasetPropagationDirective)
        return []
        # parent_urn = entity_urn.entity_ids[0]

        # for aspect in propagation_directive.aspects:
        # yield MetadataChangeProposalWrapper(
        #    entityUrn=str(entity_urn),
        #    aspect=aspect.to_obj(),
        # )
