import json
import logging
from typing import Dict, List, Optional, Set, Union, cast

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    MetadataChangeLogClass,
    MetadataChangeProposalClass,
)
from pydantic import BaseModel

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import METADATA_CHANGE_LOG_EVENT_V1_TYPE
from datahub_actions.pipeline.pipeline_context import PipelineContext

logger = logging.getLogger(__name__)


class MetadataChangeEmitterConfig(BaseModel):
    gms_server: Optional[str]
    gms_auth_token: Optional[str]
    aspects_to_exclude: Optional[List]
    extra_headers: Optional[Dict[str, str]]


class MetadataChangeSyncAction(Action):
    rest_emitter: DatahubRestEmitter
    aspects_exclude_set: Set
    # By default, we exclude the following aspects since different datahub instances have their own encryption keys for
    # encrypting tokens and secrets, we can't decrypt them even if these values sync to another datahub instance
    # also, we don't sync execution request aspects because the ingestion recipe might contain datahub secret
    # that another datahub instance could not decrypt
    DEFAULT_ASPECTS_EXCLUDE_SET = {
        "dataHubAccessTokenInfo",
        "dataHubAccessTokenKey",
        "dataHubSecretKey",
        "dataHubSecretValue",
        "dataHubExecutionRequestInput",
        "dataHubExecutionRequestKey",
        "dataHubExecutionRequestResult",
    }

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = MetadataChangeEmitterConfig.parse_obj(config_dict or {})
        return cls(action_config, ctx)

    def __init__(self, config: MetadataChangeEmitterConfig, ctx: PipelineContext):
        self.config = config
        assert isinstance(self.config.gms_server, str)
        self.rest_emitter = DatahubRestEmitter(
            gms_server=self.config.gms_server,
            token=self.config.gms_auth_token,
            extra_headers=self.config.extra_headers,
        )
        if self.config.aspects_to_exclude is not None:
            self.aspects_exclude_set = self.DEFAULT_ASPECTS_EXCLUDE_SET.union(
                set(self.config.aspects_to_exclude)
            )

    def act(self, event: EventEnvelope) -> None:
        """
        This method listens for MetadataChangeLog events, casts it to MetadataChangeProposal,
        and emits it to another datahub instance
        """
        # MetadataChangeProposal only supports UPSERT type for now
        if event.event_type is METADATA_CHANGE_LOG_EVENT_V1_TYPE:
            orig_event = cast(MetadataChangeLogClass, event.event)
            if orig_event.get("aspectName") not in self.aspects_exclude_set:
                mcp = self.buildMcp(orig_event)
                if mcp is not None:
                    self.emit(mcp)

    def buildMcp(
        self, orig_event: MetadataChangeLogClass
    ) -> Union[MetadataChangeProposalClass, None]:
        try:
            mcp = MetadataChangeProposalClass(
                entityType=orig_event.get("entityType"),
                changeType=orig_event.get("changeType"),
                entityUrn=orig_event.get("entityUrn"),
                entityKeyAspect=orig_event.get("entityKeyAspect"),
                aspectName=orig_event.get("aspectName"),
                aspect=orig_event.get("aspect"),
            )
            return mcp
        except Exception as ex:
            logger.error(
                f"error when building mcp from mcl {json.dumps(orig_event.to_obj(), indent=4)}"
            )
            logger.error(f"exception: {ex}")
            return None

    def emit(self, mcp: MetadataChangeProposalClass) -> None:
        # Create an emitter to DataHub over REST
        try:
            # For unit test purpose, moving test_connection from initialization to here
            # if rest_emitter.server_config is empty, that means test_connection() has not been called before
            if not self.rest_emitter.server_config:
                self.rest_emitter.test_connection()

            self.rest_emitter.emit_mcp(mcp)
            logger.debug("finish emitting an event")
        except Exception as ex:
            logger.error(
                f"error when emitting mcp, {json.dumps(mcp.to_obj(), indent=4)}"
            )
            logger.error(f"exception: {ex}")

    def close(self) -> None:
        pass
