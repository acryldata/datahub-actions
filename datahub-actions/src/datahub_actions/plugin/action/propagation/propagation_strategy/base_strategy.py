from typing import Iterable

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper

from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.plugin.action.propagation.propagation_utils import (
    PropertyPropagationDirective,
    RelationshipType,
    SourceDetails,
)
from datahub_actions.plugin.action.propagation.propagator import EntityPropagator
from datahub_actions.plugin.action.stats_util import ActionStageReport


class BaseStrategyConfig(ConfigModel):
    max_propagation_fanout: int = 1000


class BaseStrategy:
    def __init__(
        self,
        graph: AcrylDataHubGraph,
        config: BaseStrategyConfig,
        stats: ActionStageReport,
    ):
        self.graph = graph
        self.config = config
        self._stats = stats

    def type(self) -> RelationshipType:
        raise NotImplementedError("Subclasses must implement this method")

    def propagate(
        self,
        propagator: EntityPropagator,
        directive: PropertyPropagationDirective,
        context: SourceDetails,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        raise NotImplementedError("Subclasses must implement this method")
