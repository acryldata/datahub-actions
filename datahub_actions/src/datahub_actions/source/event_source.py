from abc import abstractmethod
from typing import Iterable

from datahub.ingestion.api.closeable import Closeable

from datahub_actions.events.event import EnvelopedEvent


class EventSource(Closeable):
    @classmethod
    @abstractmethod
    def create(cls, config_dict: dict) -> "EventSource":
        pass

    @abstractmethod
    def events(self) -> Iterable[EnvelopedEvent]:
        """
        Returns an iterable of DataHub events
        """

    @abstractmethod
    def ack(self, event: EnvelopedEvent):
        """
        Ack the processing of an individual event
        """
