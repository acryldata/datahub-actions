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
from typing import Optional

from datahub.configuration.common import ConfigModel
from pydantic import validator

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TagPropagationActionConfig(ConfigModel):
    tag_prefix: Optional[str]

    @validator("tag_prefix")
    def tag_prefix_should_start_with_urn(cls, v: str) -> str:
        if v and not v.startswith("urn:li:tag:"):
            return "urn:li:tag:" + v
        else:
            return v


class TagPropagationAction(Action):
    def __init__(self, config: TagPropagationActionConfig, ctx: PipelineContext):
        self.config: TagPropagationActionConfig = config
        self.ctx = ctx

    @classmethod
    def create(cls, config_dict, ctx):
        config = TagPropagationActionConfig.parse_obj(config_dict or {})
        logger.info(f"TagPropagationAction configured with {config}")
        return cls(config, ctx)

    def name(self) -> str:
        return "TagPropagator"

    def act(self, event: EventEnvelope) -> None:
        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            assert self.ctx.graph is not None
            semantic_event = event.event
            if semantic_event.category == "TAG" and semantic_event.operation == "ADD":
                assert semantic_event.modifier, "tag urn should be present"
                # do work
                if self.config.tag_prefix:
                    logger.info(
                        f"Only looking for tags with prefix {self.config.tag_prefix}"
                    )
                    # find downstream lineage
                    entity_urn: str = semantic_event.entityUrn
                    downstreams = self.ctx.graph.get_downstreams(entity_urn)
                    logger.info(
                        f"Detected {len(downstreams)} downstreams for {entity_urn}: {downstreams}"
                    )
                    logger.info(
                        f"Detected {semantic_event.modifier} added to {semantic_event.entityUrn}"
                    )
                    # apply tags to downstreams
                    for d in downstreams:
                        self.ctx.graph.add_tags_to_dataset(d, [semantic_event.modifier])
