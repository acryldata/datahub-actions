import pprint
import sys
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
    _exception_count: int = 0

    def increment_exception_count(self) -> None:
        self._exception_count = self._exception_count + 1

    def get_exception_count(self) -> int:
        return self._exception_count

    def as_string(self) -> str:
        return pprint.pformat(self.__dict__, width=150, **PPRINT_OPTIONS)


# Class that stores running statistics for a single Actions Transformer.
class TransformerStats:
    # The number of exceptions raised by the Transformer.
    _exception_count: int = 0

    # The number of events filtered by the Transformer.
    _filtered_count: int = 0

    def increment_exception_count(self) -> None:
        self._exception_count = self._exception_count + 1

    def increment_filtered_count(self) -> None:
        self._filtered_count = self._filtered_count + 1

    def get_exception_count(self) -> int:
        return self._exception_count

    def get_filtered_count(self) -> int:
        return self._filtered_count

    def as_string(self) -> str:
        return pprint.pformat(self.__dict__, width=150, **PPRINT_OPTIONS)


# Class that stores running statistics for a single Actions Pipeline.
class PipelineStats:
    # Timestamp in milliseconds when the pipeline was launched.
    _started_at: int

    # Top-level number of failed processing executions. If a single event is reprocessed, this counter will be incremented.
    _exception_count: int = 0

    # Top-level number of succeeded processing executions.
    _success_count: int = 0

    # Transformer Stats
    _transformer_stats: Dict[str, TransformerStats] = {}

    # Action Stats
    _action_stats: ActionStats = ActionStats()

    def mark_start(self) -> None:
        self._started_at = int(time() * 1000)

    def increment_exception_count(self) -> None:
        self._exception_count = self._exception_count + 1

    def increment_success_count(self) -> None:
        self._success_count = self._success_count + 1

    def increment_transformer_exception_count(self, transformer: str) -> None:
        if transformer not in self._transformer_stats:
            self._transformer_stats[transformer] = TransformerStats()
        self._transformer_stats[transformer].increment_exception_count()

    def increment_transformer_filtered_count(self, transformer: str) -> None:
        if transformer not in self._transformer_stats:
            self._transformer_stats[transformer] = TransformerStats()
        self._transformer_stats[transformer].increment_filtered_count()

    def increment_action_exception_count(self) -> None:
        self._action_stats.increment_exception_count()

    def get_started_at(self) -> int:
        return self._started_at

    def get_exception_count(self) -> int:
        return self._exception_count

    def get_success_count(self) -> int:
        return self._success_count

    def get_transformer_stats(self, transformer: str) -> TransformerStats:
        if transformer not in self._transformer_stats:
            self._transformer_stats[transformer] = TransformerStats()
        return self._transformer_stats[transformer]

    def get_action_stats(self) -> ActionStats:
        return self._action_stats

    def as_string(self) -> str:
        return pprint.pformat(self.__dict__, width=150, **PPRINT_OPTIONS)

    def pretty_print_summary(self) -> None:
        click.echo()
        click.secho("Pipeline statistics", bold=True)
        click.echo(self.as_string())
        click.echo()
        if len(self._transformer_stats.keys()) > 0:
            click.secho("Transformer statistics", bold=True)
            for key in self._transformer_stats:
                click.echo(self._transformer_stats[key].as_string())
            click.echo()
        click.secho("Action statistics", bold=True)
        click.echo(self._action_stats.as_string())
        click.echo()
