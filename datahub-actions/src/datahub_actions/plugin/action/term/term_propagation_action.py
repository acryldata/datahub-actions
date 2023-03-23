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

import logging

from datahub.configuration.common import ConfigModel
from datahub.metadata.schema_classes import EntityChangeEventClass as EntityChangeEvent
from pydantic.class_validators import validator

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext

logger = logging.getLogger(__name__)


class TermPropagationConfig(ConfigModel):
    target_term: str = "urn:li:glossaryTerm:Classification.Confidential"

    @validator("target_term")
    def term_should_always_have_urn_prefix(cls, v: str) -> str:
        if not v.startswith("urn:li:glossaryTerm:"):
            return f"urn:li:glossaryTerm:{v}"
        else:
            return v


class TermPropagationAction(Action):
    def __init__(self, config: TermPropagationConfig, ctx: PipelineContext):
        self.config = config
        self.ctx = ctx

    def name(self) -> str:
        return "TermPropagator"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = TermPropagationConfig.parse_obj(config_dict or {})
        logger.info(f"Term Propagation Config action configured with {action_config}")
        return cls(action_config, ctx)

    def act(self, event: EventEnvelope) -> None:
        """This method responds to changes to glossary terms and propagates them to downstream entities"""

        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            assert self.ctx.graph is not None
            semantic_event = event.event
            if (
                semantic_event.category == "GLOSSARY_TERM"
                and semantic_event.operation == "ADD"
            ):
                # do work
                target_term_involved = False
                # Check which terms have connectivity to the target term
                if (
                    semantic_event.modifier == self.config.target_term
                    or self.ctx.graph.check_relationship(  # term has been directly applied  # term is indirectly associated
                        self.config.target_term,
                        semantic_event.modifier,
                        "IsA",
                    )
                ):
                    target_term_involved = True

            if target_term_involved:
                # find downstream lineage
                downstreams = self.ctx.graph.get_downstreams(
                    entity_urn=semantic_event.entityUrn
                )

                # apply terms to downstreams
                for dataset in downstreams:
                    self.ctx.graph.add_terms_to_dataset(
                        dataset, [self.config.target_term]
                    )
                    logger.info(f"Will add term {self.config.target_term} to {dataset}")
