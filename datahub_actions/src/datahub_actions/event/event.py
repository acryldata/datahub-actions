import json
import logging
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict

logger = logging.getLogger(__name__)


class Event(metaclass=ABCMeta):
    """
    A DataHub Event.
    """

    @classmethod
    @abstractmethod
    def from_json(cls, json_str: str) -> "Event":
        """
        Convert from json format into the event object.
        """

    @abstractmethod
    def as_json(self) -> str:
        """
        Convert the event into its JSON representation.
        """


# An object representation of the actual change event.
@dataclass
class EventEnvelope:
    # The type of the event. This corresponds to the shape of the payload.
    event_type: str

    # The event itself
    event: Event

    # Arbitrary metadata about the event
    meta: Dict[str, Any]

    # Convert an enveloped event to JSON representation
    def as_json(self) -> str:
        # Be careful about converting meta bag, since anything can be put inside at runtime.
        meta_json = None
        try:
            if self.meta is not None:
                meta_json = json.dumps(self.meta)
        except Exception:
            logger.warn(
                f"Failed to serialize meta field of EventEnvelope to json {self.meta}. Ignoring it during serialization."
            )
        result = f'{{ "event_type": "{self.event_type}", "event": {self.event.as_json()}, "meta": {meta_json if meta_json is not None else "null"} }}'
        return result

    # Convert a json event envelope back into the object.
    @classmethod
    def from_json(cls, json_str: str) -> "EventEnvelope":
        pass
        # TODO: Deserialize a json event back to an Enveloped Event.
        # json_obj = json.loads(json)
        # event_type = json_obj["event_type"]
        # event = deserialize_event(event_type, json_obj["event"])
        # meta = json_obj["meta"] if "meta" in json_obj else {}
        # return EventEnvelope(
        #    event_type=event_type,
        #    event=event,
        #    meta=meta
        # )
