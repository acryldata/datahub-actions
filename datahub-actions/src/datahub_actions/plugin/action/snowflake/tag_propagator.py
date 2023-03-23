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
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from pydantic import validator

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.snowflake.snowflake_util import SnowflakeTagHelper

logger = logging.getLogger(__name__)


class SnowflakeTagPropagatorConfig(ConfigModel):
    snowflake: SnowflakeV2Config
    tag_prefix: Optional[str] = None
    target_term: Optional[str] = None

    @validator("tag_prefix")
    def tag_prefix_should_start_with_urn(cls, v: str) -> str:
        if v and not v.startswith("urn:li:tag:"):
            return "urn:li:tag:" + v
        else:
            return v


class SnowflakeTagPropagatorAction(Action):
    def __init__(self, config: SnowflakeTagPropagatorConfig, ctx: PipelineContext):
        self.config: SnowflakeTagPropagatorConfig = config
        self.ctx = ctx
        logger.info("Snowflake tag sync enabled")
        self.snowflake_tag_helper = SnowflakeTagHelper(self.config.snowflake)

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeTagPropagatorConfig.parse_obj(config_dict or {})
        return cls(config, ctx)

    @staticmethod
    def is_snowflake_urn(urn: str) -> bool:
        return urn.startswith("urn:li:dataset:(urn:li:dataPlatform:snowflake")

    def name(self) -> str:
        return "SnowflakeTagPropagator"

    def act(self, event: EventEnvelope) -> None:
        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            assert self.ctx.graph is not None
            semantic_event = event.event
            if (
                semantic_event.category == "GLOSSARY_TERM"
                or semantic_event.category == "TAG"
            ) and semantic_event.operation == "ADD":
                if not self.is_snowflake_urn(semantic_event.entityUrn):
                    return
                assert semantic_event.modifier is not None

                entity_to_apply = None
                if semantic_event.category == "TAG" and self.config.tag_prefix:
                    logger.info(
                        f"Only looking for tags with prefix {self.config.tag_prefix}"
                    )
                    if semantic_event.modifier.startswith(self.config.tag_prefix):
                        entity_to_apply = semantic_event.modifier

                if (
                    semantic_event.category == "GLOSSARY_TERM"
                    and self.config.target_term
                ):
                    # Check which terms have connectivity to the target term
                    if (
                        semantic_event.modifier == self.config.target_term
                        or self.ctx.graph.check_relationship(  # term has been directly applied  # term is indirectly associated
                            self.config.target_term,
                            semantic_event.modifier,
                            "IsA",
                        )
                    ):
                        entity_to_apply = semantic_event.modifier

                if entity_to_apply is not None:
                    logger.info(
                        f"Will add {entity_to_apply} to Snowflake {semantic_event.entityUrn}"
                    )
                    self.snowflake_tag_helper.apply_tag_or_term(
                        semantic_event.entityUrn, entity_to_apply
                    )
