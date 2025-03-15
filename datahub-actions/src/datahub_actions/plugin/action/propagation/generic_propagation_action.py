import json
import logging
from typing import Any, Dict, Iterable, List, Optional

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import GenericAspectClass
from pydantic import Field

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.mce_utils import MCEProcessor
from datahub_actions.plugin.action.mcl_utils import MCLProcessor
from datahub_actions.plugin.action.propagation.docs.docs_propagator import (
    DocsPropagator,
    DocsPropagatorConfig,
)
from datahub_actions.plugin.action.propagation.propagation_rule_config import (
    EntityLookup,
    PropagatedMetadata,
)
from datahub_actions.plugin.action.propagation.propagation_strategy.base_strategy import (
    BaseStrategy,
)
from datahub_actions.plugin.action.propagation.propagation_strategy.lineage_strategy import (
    LineageBasedStrategy,
    LineageBasedStrategyConfig,
)
from datahub_actions.plugin.action.propagation.propagation_strategy.sibling_strategy import (
    SiblingBasedStrategy,
    SiblingBasedStrategyConfig,
)
from datahub_actions.plugin.action.propagation.propagation_utils import (
    PropagationConfig,
    PropertyPropagationDirective,
    RelationshipType,
    SourceDetails,
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


class PropagationSettings(ConfigModel):
    description: DocsPropagatorConfig
    tags: TagPropagatorConfig
    # terms: TermPropagationSettings
    # structuredProperties: StructuredPropertyPropagationSettings


class PropagationRule(ConfigModel):
    metadataPropagated: Dict[PropagatedMetadata, Dict] = {}
    targetUrnResolution: List[EntityLookup]

    entityTypes: List[str]
    # propagationSettings: Union[PropagationSettings, List[AspectLookup]]

    # mcl: Optional[MclTriggerRule] = None


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

    propagation_rule: PropagationRule = Field(
        description="Rule for property propagation.",
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
        if (
            PropagatedMetadata.DOCUMENTATION
            in self.config.propagation_rule.metadataPropagated.keys()
        ):
            self.propagators.append(
                DocsPropagator(
                    self.action_urn,
                    self.ctx.graph,
                    DocsPropagatorConfig(
                        propagation_rule=self.config.propagation_rule,
                        **self.config.propagation_rule.metadataPropagated.get(
                            PropagatedMetadata.DOCUMENTATION, {}
                        ),
                    ),
                )
            )
        if PropagatedMetadata.TAGS in self.config.propagation_rule.metadataPropagated:
            self.propagators.append(
                TagPropagator(
                    self.action_urn,
                    self.ctx.graph,
                    TagPropagatorConfig(
                        propagation_rule=self.config.propagation_rule,
                        **self.config.propagation_rule.metadataPropagated.get(
                            PropagatedMetadata.TAGS, {}
                        ),
                    ),
                )
            )

        self.propagation_strategies: Dict[RelationshipType, BaseStrategy] = {}

        lineage_based_strategy = LineageBasedStrategy(
            self.ctx.graph, LineageBasedStrategyConfig(), self._stats
        )
        self.propagation_strategies[lineage_based_strategy.type()] = (
            lineage_based_strategy
        )

        sibling_based_strategy = SiblingBasedStrategy(
            self.ctx.graph, SiblingBasedStrategyConfig(), self._stats
        )

        self.propagation_strategies[sibling_based_strategy.type()] = (
            sibling_based_strategy
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
                    yield from self._propagate_directive(
                        propagator=propagator, directive=directive
                    )
            stats.end(event, success=True)
        except Exception:
            logger.error(f"Error processing event {event}:", exc_info=True)
            stats.end(event, success=False)

    def _propagate_directive(
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

        for relationship_type in directive.relationships.keys():
            prop_strategy = self.propagation_strategies.get(relationship_type)
            if prop_strategy:
                yield from prop_strategy.propagate(propagator, directive, context)
            else:
                logger.warning(
                    f"No propagation strategy found for relationship type {relationship_type}"
                )

    def close(self) -> None:
        # Implement the close method
        pass
