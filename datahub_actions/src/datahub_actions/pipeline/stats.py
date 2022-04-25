import datetime
import pprint
import sys
import json
from time import time
from typing import Dict

import click

# The sort_dicts option was added in Python 3.8.
if sys.version_info >= (3, 8):
    PPRINT_OPTIONS = {"sort_dicts": False}
else:
    PPRINT_OPTIONS: Dict = {}


# Class that stores running statistics for a single Action.
# TODO: Invocation time tracking.
class ActionStats:
    # The number of exception raised by the Action.
    exception_count: int = 0

    # The number of events that were actually submitted to the Action
    success_count: int = 0

    def increment_exception_count(self) -> None:
        self.exception_count = self.exception_count + 1

    def get_exception_count(self) -> int:
        return self.exception_count

    def increment_success_count(self) -> None:
        self.success_count = self.success_count + 1

    def get_success_count(self) -> int:
        return self.success_count

    def as_string(self) -> str:
        return pprint.pformat(json.dumps(self.__dict__, indent=4, sort_keys=True), width=150, **PPRINT_OPTIONS)


# Class that stores running statistics for a single Actions Transformer.
class TransformerStats:
    # The number of exceptions raised by the Transformer.
    exception_count: int = 0

    # The total number of events that were received by the transformer. 
    processed_count: int = 0

    # The number of events filtered by the Transformer. The total transformed count is equal to processed count - filtered count.
    filtered_count: int = 0

    def increment_exception_count(self) -> None:
        self.exception_count = self.exception_count + 1

    def increment_processed_count(self) -> None:
        self.processed_count = self.processed_count + 1

    def increment_filtered_count(self) -> None:
        self.filtered_count = self.filtered_count + 1

    def get_exception_count(self) -> int:
        return self.exception_count

    def get_processed_count(self) -> int:
        return self.processed_count

    def get_filtered_count(self) -> int:
        return self.filtered_count

    def as_string(self) -> str:
        return pprint.pformat(json.dumps(self.__dict__,  indent=4, sort_keys=True), width=150, **PPRINT_OPTIONS)


# Class that stores running statistics for a single Actions Pipeline.
class PipelineStats:
    # Timestamp in milliseconds when the pipeline was launched.
    started_at: int

    # Number of events that failed processing even after retry.
    failed_event_count: int = 0

    # Number of events that failed when "ack" was invoked.
    failed_ack_count: int = 0

    # Top-level number of succeeded processing executions.
    success_count: int = 0

    # Transformer Stats
    transformer_stats: Dict[str, TransformerStats] = {}

    # Action Stats
    action_stats: ActionStats = ActionStats()

    def mark_start(self) -> None:
        self.started_at = int(time() * 1000)

    def increment_failed_event_count(self) -> None:
        self.failed_event_count = self.failed_event_count + 1

    def increment_failed_ack_count(self) -> None:
        self.failed_ack_count = self.failed_ack_count + 1

    def increment_success_count(self) -> None:
        self.success_count = self.success_count + 1

    def increment_transformer_exception_count(self, transformer: str) -> None:
        if transformer not in self.transformer_stats:
            self.transformer_stats[transformer] = TransformerStats()
        self.transformer_stats[transformer].increment_exception_count()

    def increment_transformer_processed_count(self, transformer: str) -> None:
        if transformer not in self.transformer_stats:
            self.transformer_stats[transformer] = TransformerStats()
        self.transformer_stats[transformer].increment_processed_count()

    def increment_transformer_filtered_count(self, transformer: str) -> None:
        if transformer not in self.transformer_stats:
            self.transformer_stats[transformer] = TransformerStats()
        self.transformer_stats[transformer].increment_filtered_count()

    def increment_action_exception_count(self) -> None:
        self.action_stats.increment_exception_count()

    def increment_action_success_count(self) -> None:
        self.action_stats.increment_success_count()

    def get_started_at(self) -> int:
        return self.started_at

    def get_failed_event_count(self) -> int:
        return self.failed_event_count

    def get_failed_ack_count(self) -> int:
        return self.failed_ack_count

    def get_success_count(self) -> int:
        return self.success_count

    def get_transformer_stats(self, transformer: str) -> TransformerStats:
        if transformer not in self.transformer_stats:
            self.transformer_stats[transformer] = TransformerStats()
        return self.transformer_stats[transformer]

    def get_action_stats(self) -> ActionStats:
        return self.action_stats

    def as_string(self) -> str:
        return pprint.pformat(json.dumps(self.__dict__, indent=4, sort_keys=True), width=150, **PPRINT_OPTIONS)

    def pretty_print_summary(self, name: str) -> None:
        curr_time = int(time() * 1000)
        click.echo()
        click.secho(f"Pipeline Report for {name}", bold=True, fg="blue")
        click.echo()
        click.echo(
            f"Started at: {datetime.datetime.fromtimestamp(self._started_at/1000.0)} (Local Time)"
        )
        click.echo(f"Duration: {(curr_time - self._started_at)/1000.0}s")
        click.echo()
        click.secho("Pipeline statistics", bold=True)
        click.echo()
        click.echo(self.as_string())
        click.echo()
        if len(self.transformer_stats.keys()) > 0:
            click.secho("Transformer statistics", bold=True)
            for key in self.transformer_stats:
                click.echo()
                click.echo(f"{key}: {self._transformer_stats[key].as_string()}")
            click.echo()
        click.secho("Action statistics", bold=True)
        click.echo()
        click.echo(self.action_stats.as_string())
        click.echo()
