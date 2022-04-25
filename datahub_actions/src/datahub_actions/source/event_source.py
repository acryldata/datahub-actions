from abc import abstractmethod
from typing import Iterable

from datahub.ingestion.api.closeable import Closeable

from datahub_actions.event.event import EnvelopedEvent
from datahub_actions.pipeline.context import ActionContext


class EventSource(Closeable):
    @classmethod
    @abstractmethod
    def create(cls, config_dict: dict, ctx: ActionContext) -> "EventSource":
        pass

    @abstractmethod
    def events(self) -> Iterable[EnvelopedEvent]:
        """
        Returns an iterable of DataHub events
        """

    @abstractmethod
    def ack(self, event: EnvelopedEvent) -> None:
        """
        Ack the processing of an individual event
        """
