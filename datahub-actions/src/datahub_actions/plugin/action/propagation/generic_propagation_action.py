import json
import logging
from typing import Any, Dict, Iterable, List, Optional

import cachetools
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import GenericAspectClass
from datahub.utilities.urns.urn import Urn, guess_entity_type
from pydantic import Field

from datahub_actions.action.action import Action
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.mce_utils import MCEProcessor
from datahub_actions.plugin.action.mcl_utils import MCLProcessor
from datahub_actions.plugin.action.propagation.docs.docs_propagator import (
    DocsPropagator,
    DocsPropagatorConfig,
)
from datahub_actions.plugin.action.propagation.propagation_utils import (
    DirectionType,
    PropagationConfig,
    PropagationDirective,
    PropagationRelationships,
    PropertyPropagationDirective,
    PropertyType,
    RelationshipType,
    SourceDetails,
    get_unique_siblings,
)
from datahub_actions.plugin.action.propagation.propagator import EntityPropagator
from datahub_actions.plugin.action.stats_util import (
    ActionStageReport,
    EventProcessingStats,
)
from datahub_actions.plugin.action.tag.tag_propagator import (
    TagPropagator,
    TagPropagatorConfig,
)

logger = logging.getLogger(__name__)


class PropertyPropagationConfig(PropagationConfig):
    """
    Configuration model for property propagation.

    Attributes:
        enabled (bool): Indicates whether property propagation is enabled.
        supported_properties (List[str]): List of property types that can be propagated.
        entity_types_enabled (Dict[str, bool]): Mapping of entity types to whether propagation is enabled.
        propagation_relationships (List[PropagationRelationships]): Allowed propagation relationships.
    """

    enabled: bool = Field(
        True, description="Indicates whether property propagation is enabled."
    )
    supported_properties: List[PropertyType] = Field(
        [PropertyType.DOCUMENTATION, PropertyType.TAG],
        description="List of property types that can be propagated.",
    )

    entity_types_enabled: Dict[str, bool] = Field(
        {"schemaField": True, "dataset": False},
        description="Mapping of entity types to whether propagation is enabled.",
    )
    propagation_relationships: List[PropagationRelationships] = Field(
        [
            PropagationRelationships.SIBLING,
            PropagationRelationships.DOWNSTREAM,
            PropagationRelationships.UPSTREAM,
        ],
        description="Allowed propagation relationships.",
    )


class GenericPropagationAction(Action):
    """
    A generic action for propagating properties (documentation, tags, etc.) across related entities.
    """

    def __init__(self, config: PropertyPropagationConfig, ctx: PipelineContext):
        super().__init__()
        self.action_urn = (
            f"urn:li:dataHubAction:{ctx.pipeline_name}"
            if not ctx.pipeline_name.startswith("urn:li:dataHubAction")
            else ctx.pipeline_name
        )
        self.config = config
        self.last_config_refresh = 0
        self.ctx = ctx
        self.mcl_processor = MCLProcessor()
        self.mce_processor = MCEProcessor()
        self.actor_urn = "urn:li:corpuser:__datahub_system"
        self._stats = ActionStageReport()
        self._stats.start()
        assert self.ctx.graph
        self._rate_limited_emit_mcp = self.config.get_rate_limited_emit_mcp(
            self.ctx.graph.graph
        )

        self.propagators: List[EntityPropagator] = []
        self.propagators.append(
            DocsPropagator(
                self.action_urn,
                self.ctx.graph,
                DocsPropagatorConfig(
                    propagation_relationships=self.config.propagation_relationships
                ),
            )
        )
        self.propagators.append(
            TagPropagator(
                self.action_urn,
                self.ctx.graph,
                TagPropagatorConfig(
                    propagation_relationships=self.config.propagation_relationships
                ),
            )
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = PropertyPropagationConfig.parse_obj(config_dict or {})
        logger.info(
            f"Generic Propagation Config action configured with {action_config}"
        )
        return cls(action_config, ctx)

    def _extract_property_value(
        self, property_type: str, aspect_value: Optional[GenericAspectClass]
    ) -> Any:
        """Extract the property value from the aspect based on property type."""
        if not aspect_value:
            return None

        value_obj = json.loads(aspect_value.value)

        # Add property-specific extraction logic here
        if property_type == "documentation":
            return value_obj.get("documentation", "")
        elif property_type == "tags":
            return value_obj.get("tags", [])

        return None

    def act(self, event: EventEnvelope) -> None:
        """Process the event and emit change proposals."""
        for mcp in self.act_async(event):
            self._rate_limited_emit_mcp(mcp)

    def act_async(
        self, event: EventEnvelope
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Process the event asynchronously and yield change proposals."""
        if not self.config.enabled:
            logger.warning("Property propagation is disabled. Skipping event")
            return

        if not self._stats.event_processing_stats:
            self._stats.event_processing_stats = EventProcessingStats()

        stats = self._stats.event_processing_stats
        stats.start(event)

        try:
            logger.info("Calling propagators:")
            for propagator in self.propagators:
                logger.info(f"Calling propagator: {propagator.__class__.__name__}")
                directive = propagator.should_propagate(event)
                logger.info(
                    # f"Doc propagation directive {directive} for event: {event}"
                    f"Doc propagation directive {directive}"
                )
                if directive is not None and directive.propagate:
                    self._stats.increment_assets_processed(directive.entity)
                    yield from self._propagate_directive(directive, propagator)
            stats.end(event, success=True)
        except Exception:
            logger.error(f"Error processing event {event}:", exc_info=True)
            stats.end(event, success=False)

    def _propagate_directive(
        self, directive: PropertyPropagationDirective, propagator: EntityPropagator
    ) -> Iterable[MetadataChangeProposalWrapper]:
        assert self.ctx.graph
        logger.debug(f"Doc Propagation Directive: {directive}")
        # TODO: Put each mechanism behind a config flag to be controlled
        # externally.
        lineage_downstream = (
            RelationshipType.LINEAGE,
            DirectionType.DOWN,
        ) in directive.relationships
        lineage_upstream = (
            RelationshipType.LINEAGE,
            DirectionType.UP,
        ) in directive.relationships
        lineage_any = (
            RelationshipType.LINEAGE,
            DirectionType.ALL,
        ) in directive.relationships
        logger.info(
            f"Lineage Downstream: {lineage_downstream}, Lineage Upstream: {lineage_upstream}, Lineage Any: {lineage_any}"
        )
        if (
            lineage_downstream
            or lineage_any
            or lineage_upstream
            or (
                RelationshipType.SIBLING,
                DirectionType.ALL,
            )
        ):
            # Step 1: Propagate to downstream entities
            yield from self._propagate_property(
                propagator=propagator, directive=directive
            )

    def _propagate_property(
        self, propagator: EntityPropagator, directive: PropertyPropagationDirective
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Propagate the property according to the directive."""
        assert self.ctx.graph

        context = SourceDetails(
            origin=directive.origin,
            via=directive.via,
            propagated=True,
            actor=directive.actor,
            propagation_started_at=directive.propagation_started_at,
            propagation_depth=directive.propagation_depth,
        )

        # Propagate based on relationships
        for relationship, direction in directive.relationships:
            if relationship == RelationshipType.LINEAGE:
                if direction in (DirectionType.DOWN, DirectionType.ALL):
                    yield from self._propagate_to_direction(
                        propagator, directive, context, DirectionType.DOWN
                    )
                if direction in (DirectionType.UP, DirectionType.ALL):
                    yield from self._propagate_to_direction(
                        propagator, directive, context, DirectionType.UP
                    )
            elif relationship == RelationshipType.SIBLING:
                yield from self._propagate_to_siblings(propagator, directive, context)

    @cachetools.cachedmethod(cache=lambda self: cachetools.TTLCache(maxsize=1000, ttl=60 * 5))  # type: ignore[misc]
    def get_upstreams_cached(
        self, graph: AcrylDataHubGraph, entity_urn: str
    ) -> List[str]:
        """Get the upstream entity from cache."""
        return graph.get_upstreams(entity_urn=entity_urn)

    def _only_one_upstream_field(
        self,
        graph: AcrylDataHubGraph,
        downstream_field: str,
        upstream_field: str,
    ) -> bool:
        """
        Check if there is only one upstream field for the downstream field. If upstream_field is provided,
        it will also check if the upstream field is the only upstream

        TODO: We should cache upstreams because we make this fetch upstreams call FOR EVERY downstream that must be propagated to.
        """
        upstreams = graph.get_upstreams(entity_urn=downstream_field)
        # Use a set here in case there are duplicated upstream edges
        upstream_fields = list(
            {x for x in upstreams if guess_entity_type(x) == "schemaField"}
        )

        # If we found no upstreams for the downstream field, simply skip.
        if not upstream_fields:
            logger.debug(
                f"No upstream fields found. Skipping propagation to downstream {downstream_field}"
            )
            return False

        # Convert the set to a list to access by index
        result = len(upstream_fields) == 1 and upstream_fields[0] == upstream_field
        if not result:
            logger.warning(
                f"Failed check for single upstream: Found upstream fields {upstream_fields} for downstream {downstream_field}. Expecting only one upstream field: {upstream_field}"
            )
        return result

    def _propagate_to_direction(
        self,
        propagator: EntityPropagator,
        doc_propagation_directive: PropagationDirective,
        context: SourceDetails,
        direction: DirectionType,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Propagate the documentation to down/upstream entities.
        """
        logger.info(
            f"Propagating to {direction} for {doc_propagation_directive.entity}, context: {context} with propagator {propagator.__class__.__name__}"
        )
        assert self.ctx.graph
        direction_str = "Upstreams" if direction == DirectionType.UP else "Downstreams"
        if direction == DirectionType.DOWN:
            lineage_entities = self.ctx.graph.get_downstreams(
                entity_urn=doc_propagation_directive.entity
            )
        elif direction == DirectionType.UP:
            lineage_entities = self.ctx.graph.get_upstreams(
                entity_urn=doc_propagation_directive.entity
            )
        else:
            raise ValueError(f"Invalid direction: {direction}")

        logger.debug(
            f"{direction_str} {lineage_entities} for {doc_propagation_directive.entity}"
        )
        entity_urn = doc_propagation_directive.entity
        propagated_context = SourceDetails.parse_obj(context.dict())
        propagated_context.propagation_relationship = RelationshipType.LINEAGE
        propagated_context.propagation_direction = direction
        propagated_entities_this_hop_count = 0
        # breakpoint()
        if guess_entity_type(entity_urn) == "schemaField":
            lineage_fields = {
                x for x in lineage_entities if guess_entity_type(x) == "schemaField"
            }

            # We only propagate to the upstream field if there is only one
            # upstream field
            if direction == DirectionType.UP and len(lineage_fields) != 1:
                return

            for field in lineage_fields:
                schema_field_urn = Urn.from_string(field)
                parent_urn = schema_field_urn.entity_ids[0]
                field_path = schema_field_urn.entity_ids[1]

                logger.info(
                    f"Will {doc_propagation_directive.operation} directive: {doc_propagation_directive} for {field_path} on {schema_field_urn}"
                )

                parent_entity_type = guess_entity_type(parent_urn)

                if parent_entity_type == "dataset":
                    if self._only_one_upstream_field(
                        self.ctx.graph,
                        downstream_field=(
                            str(schema_field_urn)
                            if direction == DirectionType.DOWN
                            else entity_urn
                        ),
                        upstream_field=(
                            entity_urn
                            if direction == DirectionType.DOWN
                            else str(schema_field_urn)
                        ),
                    ):
                        if (
                            propagated_entities_this_hop_count
                            >= self.config.max_propagation_fanout
                        ):
                            # breakpoint()
                            logger.warning(
                                f"Exceeded max propagation fanout of {self.config.max_propagation_fanout}. Skipping propagation to {direction_str.lower()} {field}"
                            )
                            # No need to propagate to more downupstreams
                            return

                        if context.origin == schema_field_urn:
                            # No need to propagate to self
                            continue

                        mcpw = propagator.create_property_change_proposal(
                            doc_propagation_directive,
                            schema_field_urn,
                            context=propagated_context,
                        )
                        if mcpw:
                            yield mcpw
                        propagated_entities_this_hop_count += 1

                    elif parent_entity_type == "chart":
                        logger.warning(
                            "Charts are expected to have fields that are dataset schema fields. Skipping for now..."
                        )
                self._stats.increment_assets_impacted(field)

        elif guess_entity_type(entity_urn) == "dataset":
            logger.info(f"Propagating to {direction_str.lower()} for {entity_urn}")
            lineage_entity = {
                x for x in lineage_entities if guess_entity_type(x) == "dataset"
            }
            for dataset in lineage_entity:
                dataset_urn = Urn.from_string(dataset)
                logger.info(
                    f"Will {doc_propagation_directive.operation} directive: {doc_propagation_directive} for {dataset_urn}"
                )
                mcpw = propagator.create_property_change_proposal(
                    doc_propagation_directive,
                    dataset_urn,
                    context=propagated_context,
                )
                if mcpw:
                    yield mcpw
                self._stats.increment_assets_impacted(dataset)

        else:
            logger.warning(
                f"Unsupported entity type {guess_entity_type(entity_urn)} for {entity_urn}"
            )

    def _propagate_to_siblings(
        self,
        propagator: EntityPropagator,
        directive: PropertyPropagationDirective,
        context: SourceDetails,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Propagate property to sibling entities."""
        assert self.ctx.graph

        siblings = get_unique_siblings(self.ctx.graph, directive.entity)
        for sibling in siblings:
            if guess_entity_type(sibling) == guess_entity_type(directive.entity):
                maybe_mcp = propagator.create_property_change_proposal(
                    directive, Urn.from_string(sibling), context
                )
                if maybe_mcp:
                    yield maybe_mcp

    def close(self) -> None:
        # Implement the close method
        pass
