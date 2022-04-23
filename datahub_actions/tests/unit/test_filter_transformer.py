from datahub.metadata.schema_classes import DictWrapper

from datahub_actions.events.event import EnvelopedEvent, EventType
from datahub_actions.transform.filter.filter_transformer import (
    FilterTransformer,
    FilterTransformerConfig,
)


class TestEvent(DictWrapper):
    def __init__(self, field1: str, field2: str):
        super().__init__()
        self._inner_dict["field1"] = field1
        self._inner_dict["field2"] = field2


def test_filter_transformer_does_exact_match():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {"event_type": "EntityChangeEvent", "fields": {"field1": "a", "field2": "b"}}
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    test_event = TestEvent("a", "b")

    result = filter_transformer.transform(
        EnvelopedEvent(
            event_type=EventType.ENTITY_CHANGE_EVENT, event=test_event, meta={}
        )
    )

    assert result is not None
