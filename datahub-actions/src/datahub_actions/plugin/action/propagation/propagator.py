import logging
import time
from typing import List, Optional, Tuple

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.utilities.urns.urn import Urn
from pydantic.fields import Field
from pydantic.main import BaseModel

from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.plugin.action.mce_utils import MCEProcessor
from datahub_actions.plugin.action.mcl_utils import MCLProcessor
from datahub_actions.plugin.action.propagation.propagation_utils import (
    DirectionType,
    PropagationDirective,
    PropagationRelationships,
    PropertyPropagationDirective,
    RelationshipType,
    SourceDetails,
)

logger = logging.getLogger(__name__)


class EntityPropagatorConfig(BaseModel):
    enabled: bool = Field(
        True, description="Indicates whether entity propagation is enabled."
    )

    entity_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Pattern for entities urns to propagate.",
    )

    propagation_relationships: List[PropagationRelationships] = Field(
        [
            PropagationRelationships.SIBLING,
            PropagationRelationships.DOWNSTREAM,
            PropagationRelationships.UPSTREAM,
        ],
        description="Allowed propagation relationships.",
    )

    max_propagation_time_millis: int = 1000 * 60 * 60 * 1  # 1 hour
    max_propagation_depth: int = 5


class EntityPropagator:

    def __init__(
        self, action_urn: str, graph: AcrylDataHubGraph, config: EntityPropagatorConfig
    ):
        self.graph = graph
        self.config = config
        self.actor_urn = "urn:li:corpuser:__datahub_system"
        self.action_urn = action_urn
        self.mcl_processor = MCLProcessor()
        self.mce_processor = MCEProcessor()

    def should_propagate(
        self, event: EventEnvelope
    ) -> Optional[PropertyPropagationDirective]:
        """Determine if the event should trigger property propagation."""
        if self.mcl_processor.is_mcl(event):
            logger.info("Event is MCL")
            return self.mcl_processor.process(event)

        if self.mce_processor.is_mce(event):
            logger.info("Event is MCE")
            return self.mce_processor.process(event)

        return None

    def get_supported_property_types(self) -> List[str]:
        properties = set(self.mce_processor.entity_aspect_processors.keys())
        properties.update(self.mcl_processor.entity_aspect_processors.keys())
        return list(properties)

    def get_supported_aspects(self) -> List[str]:
        aspects: List[str] = []
        for key in self.mce_processor.entity_aspect_processors.keys():
            aspects.extend(self.mce_processor.entity_aspect_processors[key].keys())
        for key in self.mcl_processor.entity_aspect_processors.keys():
            aspects.extend(self.mcl_processor.entity_aspect_processors[key].keys())

        return list(set(aspects))

    def should_stop_propagation(
        self, source_details: SourceDetails
    ) -> Tuple[bool, str]:
        """
        Check if the propagation should be stopped based on the source details.
        Return result and reason.
        """
        if source_details.propagation_started_at and (
            int(time.time() * 1000.0) - source_details.propagation_started_at
            >= self.config.max_propagation_time_millis
        ):
            return (True, "Propagation time exceeded.")
        if (
            source_details.propagation_depth
            and source_details.propagation_depth >= self.config.max_propagation_depth
        ):
            return (True, "Propagation depth exceeded.")
        return False, ""

    def get_propagation_relationships(
        self, source_details: Optional[SourceDetails] = None
    ) -> List[Tuple[RelationshipType, DirectionType]]:

        possible_relationships = []
        if (source_details is not None) and (
            source_details.propagation_relationship
            and source_details.propagation_direction
        ):
            restricted_relationship = source_details.propagation_relationship
            restricted_direction = source_details.propagation_direction
        else:
            restricted_relationship = None
            restricted_direction = None

        for relationship in self.config.propagation_relationships:
            if relationship == PropagationRelationships.UPSTREAM:
                if (
                    restricted_relationship == RelationshipType.LINEAGE
                    and restricted_direction == DirectionType.DOWN
                ):  # Skip upstream if the propagation has been restricted to downstream
                    continue
                possible_relationships.append(
                    (RelationshipType.LINEAGE, DirectionType.UP)
                )
            elif relationship == PropagationRelationships.DOWNSTREAM:
                if (
                    restricted_relationship == RelationshipType.LINEAGE
                    and restricted_direction == DirectionType.UP
                ):  # Skip upstream if the propagation has been restricted to downstream
                    continue
                possible_relationships.append(
                    (RelationshipType.LINEAGE, DirectionType.DOWN)
                )
            elif relationship == PropagationRelationships.SIBLING:
                possible_relationships.append(
                    (RelationshipType.SIBLING, DirectionType.ALL)
                )
        logger.debug(f"Possible relationships: {possible_relationships}")
        return possible_relationships

    def create_property_change_proposal(
        self,
        propagation_directive: PropagationDirective,
        entity_urn: Urn,
        context: SourceDetails,
    ) -> Optional[MetadataChangeProposalWrapper]:
        raise NotImplementedError("Method not implemented")
