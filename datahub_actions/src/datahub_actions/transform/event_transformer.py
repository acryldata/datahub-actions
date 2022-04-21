from abc import abstractmethod
from typing import Optional

from datahub.ingestion.api.common import RecordEnvelope

from datahub_actions.events.event import Event
from datahub_actions.pipeline.pipeline import PipelineContext


class Transformer:
    @classmethod
    @abstractmethod
    def create(cls, config: dict, ctx: PipelineContext) -> "Transformer":
        pass

    @abstractmethod
    def transform(
        self, event: RecordEnvelope[Event]
    ) -> Optional[RecordEnvelope[Event]]:
        """
        Transform a single event.
        """
