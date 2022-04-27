from abc import ABCMeta, abstractmethod
from typing import Optional

from datahub_actions.event.event import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext


class Transformer(metaclass=ABCMeta):
    """
    A base class for all DataHub Event Transformers.

    A Transformer is responsible for filtering and / or transforming events emitted from an Event Source,
    prior to forwarding them to the configured Action.

    Transformers can be chained together to form a multi-stage "transformer chain". Each Transformer
    may provide its own semantics, configurations, compatibility and guarantees.
    """

    @classmethod
    @abstractmethod
    def create(cls, config: dict, ctx: PipelineContext) -> "Transformer":
        """Factory method to create an instance of a Transformer"""
        pass

    @abstractmethod
    def transform(self, event: EventEnvelope) -> Optional[EventEnvelope]:
        """
        Transform a single Event.

        This method returns an instance of EventEnvelope, or 'None' if the event has been filtered.
        """
