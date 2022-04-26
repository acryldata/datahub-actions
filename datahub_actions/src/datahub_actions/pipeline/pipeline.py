import logging
import os
from typing import List, Optional

from datahub_actions.action.action import Action
from datahub_actions.event.event import EventEnvelope
from datahub_actions.pipeline.pipeline_config import FailureMode, PipelineConfig
from datahub_actions.pipeline.pipeline_stats import PipelineStats
from datahub_actions.pipeline.pipeline_util import (
    create_action,
    create_action_context,
    create_event_source,
    create_filter_transformer,
    create_transformer,
    normalize_directory_name,
)
from datahub_actions.source.event_source import EventSource
from datahub_actions.transform.transformer import Transformer

logger = logging.getLogger(__name__)


# Defaults for the location where failed events will be written.
DEFAULT_FAILED_EVENTS_DIR = "/tmp/logs/datahub/actions"
DEFAULT_FAILED_EVENTS_FILE_NAME = "failed_events.log"  # Not currently configurable.
DEFAULT_FAILURE_MODE = FailureMode.CONTINUE


# A component responsible for executing a single Actions pipeline.
class Pipeline:
    """
    A Pipeline is responsible for coordinating execution of a single DataHub Action.

    Most notably, this responsibility includes:

        - sourcing events from an Event Source
        - executing a configurable chain of Transformers
        - invoking an Action with the final Event
        - acknowledging the processing of an Event with the Event Source

    Additionally, a Pipeline supports the following capabilities:

        - Configurable retries of event processing in cases of component failure
        - Configurable dead letter queue (defaults to failed_events.log file)
        - Capturing basic statistics about each Pipeline component
        - At-will start and stop

    """

    name: str
    source: EventSource
    transforms: List[Transformer] = []
    action: Action

    # Whether the Pipeline has been requested to shut down
    _shutdown: bool = False

    # Pipeline statistics
    _stats: PipelineStats = PipelineStats()

    # Options
    _retry_count: int = 3  # Number of times a single event should be retried in case of processing error.
    _failure_mode: FailureMode = DEFAULT_FAILURE_MODE
    _failed_events_dir: str = DEFAULT_FAILED_EVENTS_DIR  # The top-level path where failed events will be logged.

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
            config.options.retry_count if config.options else None,
            config.options.failure_mode if config.options else None,
            config.options.failed_events_dir if config.options else None,
        )

    # Start the Pipeline.
    def start(self) -> None:
        self._stats.mark_start()
        # First, source the events.
        enveloped_events = self.source.events()
        for enveloped_event in enveloped_events:
            # Then, process the event.
            self._process_event(enveloped_event)
            # Finally, ack the event.
            self._ack_event(enveloped_event)

    def _process_event(self, enveloped_event: EventEnvelope) -> None:

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
            self._stats.increment_failed_event_count()

            # Finally, handle the failure
            self._handle_failure(enveloped_event)

    def _transform_event(
        self, enveloped_event: EventEnvelope
    ) -> Optional[EventEnvelope]:
        curr_event = enveloped_event
        for transformer in self.transforms:
            transformer_name = type(transformer).__name__
            self._stats.increment_transformer_processed_count(transformer_name)
            try:
                transformed_event = transformer.transform(curr_event)
                if transformed_event is None:
                    # Short circuit if the transformer has filtered the event.
                    self._stats.increment_transformer_filtered_count(transformer_name)
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

    def _execute_action(self, enveloped_event: EventEnvelope) -> None:
        try:
            self.action.act(enveloped_event)
            self._stats.increment_action_success_count()
        except Exception as e:
            self._stats.increment_action_exception_count()
            raise Exception(
                f"Caught exception while executing Action with type {type(self.action).__name__}"
            ) from e

    def _ack_event(self, enveloped_event: EventEnvelope) -> None:
        try:
            self.source.ack(enveloped_event)
            self._stats.increment_success_count()
        except Exception as e:
            self._stats.increment_failed_ack_count()
            logger.error(
                f"Caught exception while attempting to ack successfully processed event. event type: {enveloped_event.event_type}, pipeline name: {self.name}",
                e,
            )
            logger.debug(f"Failed to ack event: {enveloped_event}")

    def _handle_failure(self, enveloped_event: EventEnvelope) -> None:
        # First, always save the failed event to a file. Useful for investigation.
        self._append_failed_event_to_file(enveloped_event)
        if self._failure_mode == FailureMode.THROW:
            raise Exception("Failed to process event after maximum retries.")
        elif self._failure_mode == FailureMode.CONTINUE:
            # Simply return, nothing left to do.
            pass

    def _append_failed_event_to_file(self, enveloped_event: EventEnvelope) -> None:
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
        try:
            os.makedirs(failed_events_dir, exist_ok=True)

            failed_events_file_name = os.path.join(
                failed_events_dir, DEFAULT_FAILED_EVENTS_FILE_NAME
            )
            self._failed_events_fd = open(failed_events_file_name, "a")
        except Exception as e:
            logger.debug(e)
            raise Exception(
                f"Caught exception while attempting to create failed events log file at path {failed_events_dir}. Please check your file system permissions."
            )

    # Terminate the pipeline.
    def stop(self) -> None:
        logger.debug(f"Preparing to stop Actions Pipeline with name {self.name}")
        self._shutdown = True
        self._failed_events_fd.close()
        self.source.close()
        self.action.close()

    # Get the pipeline statistics
    def stats(self) -> PipelineStats:
        return self._stats
