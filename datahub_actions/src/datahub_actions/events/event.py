import json
from dataclasses import dataclass
from enum import Enum

from datahub.metadata.schema_classes import DictWrapper


# The set of event types made available for consumption via the DataHub Actions Framework.
class EventType(Enum):
    # High-level event emitted important changes are made to an Entity on DataHub.
    ENTITY_CHANGE_EVENT = "EntityChangeEvent"

    # Low-level changelog event emitted when any change occurs at DataHub's storage layer.
    # Disclaimer: If possible, it is recommended that system-external consumers avoid depending on MetadataChangeLog.
    # It is a low-level, raw event produced from the internal DataHub CDC stream and is subject to change through time.
    METADATA_CHANGE_LOG = "MetadataChangeLogEvent"


# An object representation of the actual change event.
class Event(DictWrapper):
    # The raw fields of the event, in an easily accessible form.
    pass


# An object representation of the actual change event.
@dataclass
class EnvelopedEvent:
    # The type of the event.
    event_type: EventType

    # The event itself
    event: DictWrapper

    # Arbitrary metadata about the event
    meta: dict

    # Convert an enveloped event to JSON representation
    def to_json(self):
        return f"{{ \"event_type\": {self.event_type.value}, \"event\": {json.dumps(self.event.to_obj())}, \"meta\": {json.dumps(self.meta)} }}"
