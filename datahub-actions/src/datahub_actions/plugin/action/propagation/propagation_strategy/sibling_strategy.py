import logging
from typing import Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.utilities.urns.urn import Urn, guess_entity_type

from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.plugin.action.propagation.propagation_strategy.base_strategy import (
    BaseStrategy,
    BaseStrategyConfig,
)
from datahub_actions.plugin.action.propagation.propagation_utils import (
    PropertyPropagationDirective,
    RelationshipType,
    SourceDetails,
    get_unique_siblings,
)
from datahub_actions.plugin.action.propagation.propagator import EntityPropagator
from datahub_actions.plugin.action.stats_util import ActionStageReport

logger = logging.getLogger(__name__)


class SiblingBasedStrategyConfig(BaseStrategyConfig):
    pass


class SiblingBasedStrategy(BaseStrategy):

    def __init__(
        self,
        graph: AcrylDataHubGraph,
        config: SiblingBasedStrategyConfig,
        stats: ActionStageReport,
    ):
        super().__init__(graph, config, stats)

    def type(self) -> RelationshipType:
        return RelationshipType.SIBLING

    def propagate(
        self,
        propagator: EntityPropagator,
        directive: PropertyPropagationDirective,
        context: SourceDetails,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Propagate property to sibling entities."""
        assert self.graph

        siblings = get_unique_siblings(self.graph, directive.entity)
        for sibling in siblings:
            if guess_entity_type(sibling) == guess_entity_type(directive.entity):
                maybe_mcp = propagator.create_property_change_proposal(
                    directive, Urn.from_string(sibling), context
                )
                if maybe_mcp:
                    yield from maybe_mcp
