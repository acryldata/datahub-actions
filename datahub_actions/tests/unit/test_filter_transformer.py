from datahub_actions.transform.filter.filter_transformer import (
    FilterTransformer,
    FilterTransformerConfig,
)
from datahub_actions.events.event import EnvelopedEvent, EventType, Event


def test_filter_transformer_does_exact_match():
    filter_transformer_config = FilterTransformerConfig.parse_obj({
        "event_type": "EntityChangeEvent",
        "fields": {
            "a": "b"
        }
    })
    filter_transformer = FilterTransformer(filter_transformer_config)
    assert filter_transformer.transform(EnvelopedEvent(
        event_type=EventType.ENTITY_CHANGE_EVENT,
        event={
            "a":"b"
        }
    ))