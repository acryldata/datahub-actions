# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging
import time
from typing import List, Optional

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.utilities.urns.urn import Urn
from pydantic import Field, validator

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
logger.setLevel(logging.DEBUG)


class TagPropagatorConfig(EntityPropagatorConfig):
    """
    Configuration model for tag propagation.

    Attributes:
        enabled (bool): Indicates whether tag propagation is enabled or not. Default is True.
        tag_prefixes (Optional[List[str]]): Optional list of tag prefixes to restrict tag propagation.
            If provided, only tags with prefixes in this list will be propagated. Default is None,
            meaning all tags will be propagated.

    Note:
        Tag propagation allows tags to be automatically propagated to downstream entities.
        Enabling tag propagation can help maintain consistent metadata across connected entities.
        The `enabled` attribute controls whether tag propagation is enabled or disabled.
        The `tag_prefixes` attribute can be used to specify a list of tag prefixes that define which tags
        should be propagated. If no prefixes are specified (default), all tags will be propagated.

    Example:
        config = TagPropagationConfig(enabled=True, tag_prefixes=["urn:li:tag:"])
    """

    tag_prefixes: Optional[List[str]] = Field(
        None,
        description="Optional list of tag prefixes to restrict tag propagation.",
        example=["urn:li:tag:classification"],
    )

    @validator("tag_prefixes", each_item=True)
    def tag_prefix_should_start_with_urn(cls, v: str) -> str:
        if v:
            return make_tag_urn(v)
        return v


class TagPropagationDirective(PropagationDirective):
    tag: str


class TagPropagator(EntityPropagator):
    def __init__(
        self, action_urn: str, graph: AcrylDataHubGraph, config: TagPropagatorConfig
    ) -> None:
        super().__init__(action_urn, graph, config)
        self.config = config
        self.graph = graph
        self.actor_urn = action_urn

        self.mce_processor.register_processor(
            entity_type="dataset",
            category="TAG",
            processor=self.process_mce,
        )

    def process_mce(self, event: EventEnvelope) -> Optional[TagPropagationDirective]:
        """
        Return a tag urn to propagate or None if no propagation is desired
        """
        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            assert self.graph is not None
            assert isinstance(self.config, TagPropagatorConfig)

            semantic_event = event.event
            if semantic_event.category == "TAG" and (
                semantic_event.operation == "ADD"
                or semantic_event.operation == "REMOVE"
            ):
                logger.info(f"Processing TAG event {event}")
                assert semantic_event.modifier, "tag urn should be present"
                propagate = self.config.enabled
                if self.config.tag_prefixes:
                    propagate = any(
                        [
                            True
                            for prefix in self.config.tag_prefixes
                            if semantic_event.modifier.startswith(prefix)
                        ]
                    )
                    if not propagate:
                        logger.debug(f"Not propagating {semantic_event.modifier}")

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
                    return TagPropagationDirective(
                        propagate=True,
                        tag=semantic_event.modifier,
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
                    return TagPropagationDirective(
                        propagate=False,
                        actor=self.actor_urn,
                        tag=semantic_event.modifier,
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
    ) -> Optional[MetadataChangeProposalWrapper]:
        assert isinstance(propagation_directive, TagPropagationDirective)
        logger.info(
            f"Creating tag propagation proposal for {propagation_directive} for entity {entity_urn} with context {context}"
        )
        self.graph.add_tags_to_dataset(
            str(entity_urn),
            [propagation_directive.tag],
            context=context.dict(),
        )

        return None
