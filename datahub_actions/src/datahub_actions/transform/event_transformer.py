from abc import abstractmethod
from typing import Optional

from datahub_actions.events.event import EnvelopedEvent
from datahub_actions.pipeline.context import ActionContext


class Transformer:
    @classmethod
    @abstractmethod
    def create(cls, config: dict, ctx: ActionContext) -> "Transformer":
        pass

    @abstractmethod
    def transform(self, event: EnvelopedEvent) -> Optional[EnvelopedEvent]:
        """
        Transform a single event.
        """
