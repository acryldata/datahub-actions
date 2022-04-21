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

import importlib
import json
import logging
import sys
import traceback
from typing import Any, List, Optional

from acryl.executor.dispatcher.default_dispatcher import DefaultDispatcher
from acryl.executor.execution.executor import Executor
from acryl.executor.execution.reporting_executor import (
    ReportingExecutor,
    ReportingExecutorConfig,
)
from acryl.executor.execution.task import TaskConfig
from acryl.executor.request.execution_request import (
    ExecutionRequest as ExecutionRequestObj,
)
from acryl.executor.request.signal_request import SignalRequest as SignalRequestObj
from acryl.executor.secret.datahub_secret_store import DataHubSecretStoreConfig
from acryl.executor.secret.secret_store import SecretStoreConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from pydantic import BaseModel

from datahub_actions.api.action_core import (
    ActionContext,
    DataHubAction,
    RawMetadataChange,
    SemanticChange,
    Subscription,
)

logger = logging.getLogger(__name__)

DATAHUB_EXECUTION_REQUEST_ENTITY_NAME = "dataHubExecutionRequest"
DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME = "dataHubExecutionRequestInput"
DATAHUB_EXECUTION_REQUEST_SIGNAL_ASPECT_NAME = "dataHubExecutionRequestSignal"
APPLICATION_JSON_CONTENT_TYPE = "application/json"


class ExecutorActionConfig(BaseModel):
    # TODO: Support further configuring the secret stores for local executors
    local_executor_enabled: bool = True
    remote_executor_enabled: bool = False
    remote_executor_id: str = "remote"
    remote_executor_type: Optional[str] = None
    remote_executor_config: Optional[dict] = None


def _is_importable(path: str) -> bool:
    return "." in path or ":" in path


def import_path(path: str) -> Any:
    """
    Import an item from a package, where the path is formatted as 'package.module.submodule.ClassName'
    or 'package.module.submodule:ClassName.classmethod'. The dot-based format assumes that the bit
    after the last dot is the item to be fetched. In cases where the item to be imported is embedded
    within another type, the colon-based syntax can be used to disambiguate.
    """
    assert _is_importable(path), "path must be in the appropriate format"

    if ":" in path:
        module_name, object_name = path.rsplit(":", 1)
    else:
        module_name, object_name = path.rsplit(".", 1)

    item = importlib.import_module(module_name)
    for attr in object_name.split("."):
        item = getattr(item, attr)
    return item


# Listens to new Execution Requests & dispatches them to the appropriate handler.
class ExecutorAction(DataHubAction):
    @classmethod
    def create(cls, config_dict: dict, ctx: ActionContext) -> "DataHubAction":
        action_config = ExecutorActionConfig.parse_obj(config_dict or {})
        return cls(action_config, ctx)

    def __init__(self, action_config: ExecutorActionConfig, ctx: ActionContext):
        self.action_config = action_config
        self.ctx = ctx

        executors = []

        if action_config.local_executor_enabled is True:
            local_exec_config = self._build_default_executor_config(ctx)
            executors.append(ReportingExecutor(local_exec_config))

        if action_config.remote_executor_enabled is True:
            assert action_config.remote_executor_type is not None
            assert action_config.remote_executor_config is not None
            remote_executor = self._create_remote_executor(
                action_config.remote_executor_type, action_config.remote_executor_config
            )
            executors.append(remote_executor)

        # Construct execution request dispatcher
        self.dispatcher = DefaultDispatcher(executors)

    def name(self) -> str:
        return "ExecutionRequestAction"

    def subscriptions(self) -> List[Subscription]:
        return []

    def subscription_to_all(self) -> bool:
        """This overrides subscriptions method"""
        return True

    def act(
        self,
        match: Optional[Subscription],
        raw_change: RawMetadataChange,
        semantic_changes: Optional[List[SemanticChange]],
    ) -> List[MetadataChangeProposalWrapper]:
        """This method listens for ExecutionRequest changes to execute in schedule and trigger events"""

        orig_event = raw_change.original_event

        # Verify the original request type, etc.
        if (
            orig_event["entityType"] == DATAHUB_EXECUTION_REQUEST_ENTITY_NAME
            and orig_event["changeType"] == "UPSERT"
        ):

            if orig_event["aspectName"] == DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME:
                self._handle_execution_request_input(orig_event)

            elif (
                orig_event["aspectName"] == DATAHUB_EXECUTION_REQUEST_SIGNAL_ASPECT_NAME
            ):
                self._handle_execution_request_signal(orig_event)

        return []

    def _handle_execution_request_input(self, orig_event):

        entity_urn = orig_event["entityUrn"]
        entity_key = orig_event["entityKeyAspect"]

        # Get the run id to use.
        exec_request_id = None
        if entity_key is not None:
            exec_request_key = json.loads(
                entity_key[1]["value"]
            )  # this becomes the run id.
            exec_request_id = exec_request_key["id"]
        elif entity_urn is not None:
            urn_parts = entity_urn.split(":")
            exec_request_id = urn_parts[len(urn_parts) - 1]

        # Decode the aspect json into something more readable :)
        exec_request_input = json.loads(orig_event["aspect"][1]["value"])

        # Build an Execution Request
        exec_request = ExecutionRequestObj(
            executor_id=exec_request_input["executorId"],
            exec_id=exec_request_id,
            name=exec_request_input["task"],
            args=exec_request_input["args"],
        )

        # Try to dispatch the execution request
        try:
            self.dispatcher.dispatch(exec_request)
        except Exception:
            logger.error("ERROR", exc_info=sys.exc_info())

    def _handle_execution_request_signal(self, orig_event):

        entity_urn = orig_event["entityUrn"]

        if (
            orig_event["aspect"][1]["contentType"] == APPLICATION_JSON_CONTENT_TYPE
            and entity_urn is not None
        ):

            # Decode the aspect json into something more readable :)
            signal_request_input = json.loads(orig_event["aspect"][1]["value"])

            # Build a Signal Request
            urn_parts = entity_urn.split(":")
            exec_id = urn_parts[len(urn_parts) - 1]
            signal_request = SignalRequestObj(
                executor_id=signal_request_input["executorId"],
                exec_id=exec_id,
                signal=signal_request_input["signal"],
            )

            # Try to dispatch the signal request
            try:
                self.dispatcher.dispatch_signal(signal_request)
            except Exception:
                logger.error("ERROR", exc_info=sys.exc_info())

    def _build_default_executor_config(
        self, ctx: ActionContext
    ) -> ReportingExecutorConfig:

        # Build default task config
        local_task_config = TaskConfig(
            name="RUN_INGEST",
            type="acryl.executor.execution.sub_process_ingestion_task.SubProcessIngestionTask",
            configs=dict({}),
        )

        # Build default executor config
        local_executor_config = ReportingExecutorConfig(
            id="default",
            task_configs=[local_task_config],
            secret_stores=[
                SecretStoreConfig(type="env", config=dict({})),
                SecretStoreConfig(
                    type="datahub",
                    config=DataHubSecretStoreConfig(graph_client=ctx.graph.graph),
                ),
            ],
            graph_client=ctx.graph.graph,
        )

        return local_executor_config

    def _create_remote_executor(self, type: str, config: dict) -> Executor:
        try:
            executor_class = import_path(type)
            executor_instance = executor_class.create(config=config)
            return executor_instance
        except Exception:
            raise Exception(
                f"Failed to create instance of executor with type {type}: {traceback.format_exc(limit=3)}"
            )
