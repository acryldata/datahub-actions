from abc import abstractmethod
from typing import Iterable

from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import RecordEnvelope

from datahub_actions.events.event import Event


class EventSource(Closeable):
    @classmethod
    @abstractmethod
    def create(cls, config_dict: dict) -> "EventSource":
        pass

    @abstractmethod
    def events(self) -> Iterable[RecordEnvelope[Event]]:
        """
        Returns an iterable of DataHub events
        """

    @abstractmethod
    def ack(self, event: RecordEnvelope[Event]):
        """
        Ack the processing of an individual event
        """
