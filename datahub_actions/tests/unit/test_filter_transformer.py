from datahub_actions.events.event import EnvelopedEvent, EventType
from datahub_actions.transform.filter.filter_transformer import (
    FilterTransformer,
    FilterTransformerConfig,
)


def test_filter_transformer_does_exact_match():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {"event_type": "EntityChangeEvent", "fields": {"a": "b"}}
    )
    filter_transformer = FilterTransformer(filter_transformer_config)
    result = filter_transformer.transform(
        EnvelopedEvent(
            event_type=EventType.ENTITY_CHANGE_EVENT, event={"a": "b"}, meta={}
        )
    )

    assert result == EnvelopedEvent(
        event_type=EventType.ENTITY_CHANGE_EVENT, event={"a": "b"}, meta={}
    )

    result2 = filter_transformer.transform(
        EnvelopedEvent(
            event_type=EventType.ENTITY_CHANGE_EVENT, event={"a": "c"}, meta={}
        )
    )

    assert result2 is None
