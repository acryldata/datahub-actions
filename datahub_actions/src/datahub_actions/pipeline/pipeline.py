import logging
import os
import re
import traceback
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel

from datahub.configuration import ConfigModel
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

from datahub_actions.action.action import Action
from datahub_actions.action.action_registry import action_registry
from datahub_actions.api.action_core import AcrylDataHubGraph
from datahub_actions.events.event import EnvelopedEvent
from datahub_actions.pipeline.context import ActionContext
from datahub_actions.pipeline.stats import PipelineStats
from datahub_actions.source.event_source import EventSource
from datahub_actions.source.event_source_registry import event_source_registry
from datahub_actions.transform.event_transformer import Transformer
from datahub_actions.transform.event_transformer_registry import (
    event_transformer_registry,
)
from datahub_actions.transform.filter.filter_transformer import (
    FilterTransformer,
    FilterTransformerConfig,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FailureMode(str, Enum):
    # Exit the process with an error code when a message fails to proces.
    THROW = "THROW"
    # Log the problematic message to a file on failure, then continue to process new events.
    LOG = "LOG"


class SourceConfig(ConfigModel):
    type: str
    config: Optional[Dict[str, Any]]


class TransformConfig(ConfigModel):
    type: str
    config: Optional[Dict[str, Any]]


class FilterConfig(ConfigModel):
    event_type: str
    fields: Dict[str, Any]


class ActionConfig(ConfigModel):
    type: str
    config: Optional[dict]


class PipelineConfig(BaseModel):
    name: str
    source: SourceConfig
    filter: Optional[FilterConfig]
    transform: Optional[List[TransformConfig]]
    action: ActionConfig
    datahub: DatahubClientConfig
    retry_count: Optional[int]
    failure_mode: Optional[FailureMode]
    failed_events_dir: Optional[str]  # The path where failed events should be logged.

    class Config:  
            use_enum_values = True


def create_action_context(
    pipeline_name: str, datahub_config: DatahubClientConfig
) -> ActionContext:
    return ActionContext(pipeline_name, AcrylDataHubGraph(DataHubGraph(datahub_config)))


def create_event_source(source_config: SourceConfig, ctx: ActionContext) -> EventSource:
    event_source_type = source_config.type
    event_source_class = event_source_registry.get(event_source_type)
    try:
        logger.debug(
            f"Attempting to instantiate new Event Source of type {source_config.type}.."
        )
        event_source_config = (
            source_config.config if source_config.config is not None else {}
        )
        return event_source_class.create(event_source_config, ctx)
    except Exception as e:
        logger.error(
            f"Caught exception while attempting to instantiate Event Source of type {source_config.type}: {traceback.format_exc(limit=3)}"
        )
        raise Exception(
            f"Caught exception while attempting to instantiate Event Source of type {source_config.type}"
        ) from e


def create_filter_transformer(
    filter_config: FilterConfig, ctx: ActionContext
) -> Transformer:
    try:
        logger.debug("Attempting to instantiate filter transformer..")
        filter_transformer_config = FilterTransformerConfig(
            event_type=filter_config.event_type, fields=filter_config.fields
        )
        return FilterTransformer(filter_transformer_config)
    except Exception as e:
        logger.error(
            f"Caught exception while attempting to instantiate Filter transformer: {traceback.format_exc(limit=3)}"
        )
        raise Exception(
            "Caught exception while attempting to instantiate Filter transformer"
        ) from e


def create_transformer(
    transform_config: TransformConfig, ctx: ActionContext
) -> Transformer:
    transformer_type = transform_config.type
    transformer_class = event_transformer_registry.get(transformer_type)
    try:
        logger.debug(
            f"Attempting to instantiate new Transformer of type {transform_config.type}.."
        )
        transformer_config = (
            transform_config.config if transform_config.config is not None else {}
        )
        return transformer_class.create(transformer_config, ctx)
    except Exception as e:
        logger.error(
            f"Caught exception while attempting to instantiate Transformer: {traceback.format_exc(limit=3)}"
        )
        raise Exception(
            "Caught exception while attempting to instantiate Transformer"
        ) from e


def create_action(action_config: ActionConfig, ctx: ActionContext) -> Action:
    action_type = action_config.type
    action_class = action_registry.get(action_type)
    try:
        logger.debug(
            f"Attempting to instantiate new Action of type {action_config.type}.."
        )
        action_config_dict = (
            action_config.config if action_config.config is not None else {}
        )
        return action_class.create(action_config_dict, ctx)
    except Exception as e:
        logger.error(
            f"Caught exception while attempting to instantiate Action: {traceback.format_exc(limit=3)}"
        )
        raise Exception(
            "Caught exception while attempting to instantiate Action"
        ) from e


def normalize_directory_name(name: str) -> str:
    # Lower case & remove whitespaces + periods.
    return re.sub(r"[^\w\-_]", "_", name.lower())


# The name of the file where failed events will be written.
FAILED_EVENTS_FILE_NAME = "failed_events.log"


# A component responsible for executing a single Actions pipeline.
class Pipeline:
    name: str
    source: EventSource
    transforms: List[Transformer] = []
    action: Action

    # Whether the Pipeline has been requested to shut down
    _shutdown: bool = False

    # Pipeline statistics
    _stats: PipelineStats = PipelineStats()

    # Error handling
    _retry_count: int = 3  # Number of times a single event should be retried in case of processing error.
    _failure_mode: FailureMode = (
        FailureMode.LOG
    )  # How a failure to process should be treated.
    _failed_events_dir: str = "/tmp/logs/datahub/actions"  # The top-level path where failed events will be logged.

    def __init__(
        self,
        name: str,
        source: EventSource,
        transforms: List[Transformer],
        action: Action,
        retry_count: Optional[int],
        failure_mode: Optional[FailureMode],
        failed_events_dir: Optional[str],
    ) -> None:
        self.name = name
        self.source = source
        self.transforms = transforms
        self.action = action
        if retry_count is not None:
            self._retry_count = retry_count
        if failure_mode is not None:
            self._failure_mode = failure_mode
        if failed_events_dir is not None:
            self._failed_events_dir = failed_events_dir
        self._init_failed_events_dir()

    @classmethod
    def create(cls, config_dict: dict) -> "Pipeline":
        config = PipelineConfig.parse_obj(config_dict)

        # Create Context
        ctx = create_action_context(config.name, config.datahub)

        # Create Event Source
        event_source = create_event_source(config.source, ctx)

        # Create Transforms
        transforms = []
        if config.filter is not None:
            transforms.append(create_filter_transformer(config.filter, ctx))

        if config.transform is not None:
            for transform_config in config.transform:
                transforms.append(create_transformer(transform_config, ctx))

        # Create Action
        action = create_action(config.action, ctx)

        # Finally, create Pipeline.
        return cls(
            config.name,
            event_source,
            transforms,
            action,
            config.retry_count,
            config.failure_mode,
            config.failed_events_dir,
        )

    # Launch the Pipeline.
    def start(self) -> None:
        # First, source the events.
        enveloped_events = self.source.events()
        for enveloped_event in enveloped_events:
            # Then, process the event.
            self._process_event(enveloped_event)
            # Finally, ack the event.
            self._ack_event(enveloped_event)

    def _process_event(self, enveloped_event: EnvelopedEvent) -> None:
        # Retry event processing.
        curr_attempt = 1
        max_attempts = self._retry_count + 1
        while curr_attempt <= max_attempts:
            try:
                # First, transform the event.
                transformed_event = self._transform_event(enveloped_event)

                # Then, invoke the action if the event is non-null.
                if transformed_event is not None:
                    self._execute_action(transformed_event)

                return

            except Exception as e:
                logger.error(
                    f"Caught exception while attempting to process event. Attempt {curr_attempt}/{max_attempts} event type: {enveloped_event.event_type}, pipeline name: {self.name}",
                    e,
                )
                curr_attempt = curr_attempt + 1

            logger.error(
                f"Failed to process event after {self._retry_count} retries. event type: {enveloped_event.event_type}, pipeline name: {self.name}. Handling failure..."
            )

            # Increment failed event count.
            self._stats.increment_exception_count()  # TODO rename this to failure count.

            # Finally, handle the failure
            self._handle_failure(enveloped_event)

    def _transform_event(
        self, enveloped_event: EnvelopedEvent
    ) -> Optional[EnvelopedEvent]:
        curr_event = enveloped_event
        for transformer in self.transforms:
            try:
                transformed_event = transformer.transform(curr_event)
                if transformed_event is None:
                    # Short circuit event. Skip to ack phase.
                    self._stats.increment_transformer_filtered_count(
                        type(transformer).__name__
                    )
                    self._ack_event(enveloped_event)
                    return None
                else:
                    curr_event = transformed_event  # type: ignore
            except Exception as e:
                self._stats.increment_transformer_exception_count(
                    type(transformer).__name__
                )
                raise Exception(
                    f"Caught exception while executing Transformer with type {type(transformer).__name__}"
                ) from e

        return curr_event

    def _execute_action(self, enveloped_event: EnvelopedEvent) -> None:
        try:
            self.action.act(enveloped_event)
        except Exception as e:
            self._stats.increment_action_exception_count()
            raise Exception(
                f"Caught exception while executing Action with type {type(self.action).__name__}"
            ) from e

    def _ack_event(self, enveloped_event: EnvelopedEvent) -> None:
        try:
            self.source.ack(enveloped_event)
            self._stats.increment_success_count()
        except Exception as e:
            self._stats.increment_exception_count()
            logger.error(
                f"Caught exception while attempting to ack successfully processed event. event type: {enveloped_event.event_type}, pipeline name: {self.name}",
                e,
            )
            logger.debug(f"Failed to ack event: {enveloped_event}")

    def _handle_failure(self, enveloped_event: EnvelopedEvent) -> None:
        # First, always save the failed event to a file. Useful for investigation.
        self._append_failed_event_to_file(enveloped_event)
        if self._failure_mode == FailureMode.THROW:
            raise Exception("Failed to process event after maximum retries.")
        elif self._failure_mode == FailureMode.LOG:
            # Simply return, nothing left to do.
            pass

    def _append_failed_event_to_file(self, enveloped_event: EnvelopedEvent) -> None:
        # First, convert the event to JSON.
        json = enveloped_event.to_json()
        # Then append to failed events file.
        self._failed_events_fd.write(json + "\n")
        self._failed_events_fd.flush()

    def _init_failed_events_dir(self) -> None:
        # create a directory for failed events from this actions pipeine.
        failed_events_dir = os.path.join(
            self._failed_events_dir, normalize_directory_name(self.name)
        )
        if not os.path.exists(failed_events_dir):
            os.makedirs(failed_events_dir)

        failed_events_file_name = os.path.join(
            failed_events_dir, FAILED_EVENTS_FILE_NAME
        )
        self._failed_events_fd = open(failed_events_file_name, "a")

    # Terminate the pipeline.
    def stop(self) -> None:
        logger.info(f"Preparing to stop Actions Pipeline with name {self.name}")
        self._shutdown = True
        self.source.close()
        self._failed_events_fd.close()

    # Get the pipeline statistics
    def stats(self) -> PipelineStats:
        return self._stats
