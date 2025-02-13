import logging
from typing import Iterable, List

import cachetools
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.utilities.urns.urn import Urn, guess_entity_type

from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.plugin.action.propagation.propagation_strategy.base_strategy import (
    BaseStrategy,
    BaseStrategyConfig,
)
from datahub_actions.plugin.action.propagation.propagation_utils import (
    DirectionType,
    PropagationDirective,
    PropertyPropagationDirective,
    RelationshipType,
    SourceDetails,
)
from datahub_actions.plugin.action.propagation.propagator import EntityPropagator
from datahub_actions.plugin.action.stats_util import ActionStageReport

logger = logging.getLogger(__name__)


class LineageBasedStrategyConfig(BaseStrategyConfig):
    pass


class LineageBasedStrategy(BaseStrategy):

    def __init__(
        self,
        graph: AcrylDataHubGraph,
        config: LineageBasedStrategyConfig,
        stats: ActionStageReport,
    ):
        super().__init__(graph, config, stats)

    def type(self) -> RelationshipType:
        return RelationshipType.LINEAGE

    def propagate(
        self,
        propagator: EntityPropagator,
        directive: PropertyPropagationDirective,
        context: SourceDetails,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        directions = directive.relationships[RelationshipType.LINEAGE]
        for direction in directions:
            if direction in (DirectionType.DOWN, DirectionType.ALL):
                yield from self._propagate_to_direction(
                    propagator, directive, context, DirectionType.DOWN
                )
            if direction in (DirectionType.UP, DirectionType.ALL):
                yield from self._propagate_to_direction(
                    propagator, directive, context, DirectionType.UP
                )

    def _propagate_to_direction(
        self,
        propagator: EntityPropagator,
        propagation_directive: PropagationDirective,
        context: SourceDetails,
        direction: DirectionType,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Propagate the documentation to down/upstream entities.
        """
        logger.info(
            f"Propagating to {direction} for {propagation_directive.entity}, context: {context} with propagator {propagator.__class__.__name__}"
        )
        assert self.graph
        direction_str = "Upstreams" if direction == DirectionType.UP else "Downstreams"
        if direction == DirectionType.DOWN:
            lineage_entities = self.graph.get_downstreams(
                entity_urn=propagation_directive.entity
            )
        elif direction == DirectionType.UP:
            lineage_entities = self.graph.get_upstreams(
                entity_urn=propagation_directive.entity
            )
        else:
            raise ValueError(f"Invalid direction: {direction}")

        logger.debug(
            f"{direction_str} {lineage_entities} for {propagation_directive.entity}"
        )
        entity_urn = propagation_directive.entity
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
                    f"Will {propagation_directive.operation} directive: {propagation_directive} for {field_path} on {schema_field_urn}"
                )

                parent_entity_type = guess_entity_type(parent_urn)

                if parent_entity_type == "dataset":
                    if self._only_one_upstream_field(
                        self.graph,
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

                        yield from propagator.create_property_change_proposal(
                            propagation_directive,
                            schema_field_urn,
                            context=propagated_context,
                        )
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
                    f"Will {propagation_directive.operation} directive: {propagation_directive} for {dataset_urn}"
                )
                yield from propagator.create_property_change_proposal(
                    propagation_directive,
                    dataset_urn,
                    context=propagated_context,
                )
                self._stats.increment_assets_impacted(dataset)

        else:
            logger.warning(
                f"Unsupported entity type {guess_entity_type(entity_urn)} for {entity_urn}"
            )

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
        upstreams = self.get_upstreams_cached(entity_urn=downstream_field)
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

    @cachetools.cachedmethod(cache=lambda self: cachetools.TTLCache(maxsize=1000, ttl=60 * 5))  # type: ignore[misc]
    def get_upstreams_cached(self, entity_urn: str) -> List[str]:
        """Get the upstream entity from cache."""
        return self.graph.get_upstreams(entity_urn=entity_urn)
