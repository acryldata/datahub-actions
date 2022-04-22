from abc import abstractmethod
from typing import Optional

from datahub_actions.events.event import EnvelopedEvent
from datahub_actions.pipeline.pipeline import PipelineContext


class Transformer:
    @classmethod
    @abstractmethod
    def create(cls, config: dict, ctx: PipelineContext) -> "Transformer":
        pass

    @abstractmethod
    def transform(self, event: EnvelopedEvent) -> Optional[EnvelopedEvent]:
        """
        Transform a single event.
        """
