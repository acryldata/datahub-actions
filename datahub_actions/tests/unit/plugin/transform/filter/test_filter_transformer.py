import json
from typing import Any

from datahub.metadata.schema_classes import DictWrapper

from datahub_actions.event.event import Event, EventEnvelope, EventType
from datahub_actions.plugin.transform.filter.filter_transformer import (
    FilterTransformer,
    FilterTransformerConfig,
)


class TestEvent(Event, DictWrapper):
    def __init__(self, field1: Any, field2: Any):
        super().__init__()
        self._inner_dict["field1"] = field1
        self._inner_dict["field2"] = field2

    @classmethod
    def from_obj(cls, obj: dict, tuples: bool = False) -> "TestEvent":
        return cls(obj["field1"], obj["field2"])

    def to_obj(self, tuples: bool = False) -> dict:
        return self._inner_dict

    @classmethod
    def from_json(cls, json_str: str) -> "TestEvent":
        json_obj = json.loads(json_str)
        return TestEvent.from_obj(json_obj)

    def as_json(self) -> str:
        return json.dumps(self.to_obj())


def test_does_exact_match():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {"event_type": "EntityChangeEvent", "event": {"field1": "a", "field2": "b"}}
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    test_event = TestEvent("a", "b")

    result = filter_transformer.transform(
        EventEnvelope(
            event_type=EventType.ENTITY_CHANGE_EVENT, event=test_event, meta={}
        )
    )

    assert result is not None


def test_returns_none_when_no_match():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {"event_type": "EntityChangeEvent", "event": {"field1": "a", "field2": "b"}}
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    test_event = TestEvent("a", "c")

    result = filter_transformer.transform(
        EventEnvelope(
            event_type=EventType.ENTITY_CHANGE_EVENT, event=test_event, meta={}
        )
    )
    assert result is None


def test_matches_on_nested_event():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {
            "event_type": "EntityChangeEvent",
            "event": {"field1": {"nested_1": {"nested_b": "a"}}},
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)
    test_event = TestEvent({"nested_1": {"nested_b": "a"}, "nested_2": "c"}, None)
    result = filter_transformer.transform(
        EventEnvelope(
            event_type=EventType.ENTITY_CHANGE_EVENT, event=test_event, meta={}
        )
    )
    assert result is not None


def test_returns_none_when_no_match_nested_event():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {
            "event_type": "EntityChangeEvent",
            "event": {"field1": {"nested_1": {"nested_b": "a"}}},
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)
    test_event = TestEvent({"nested_1": {"nested_b": "b"}, "nested_2": "c"}, None)
    result = filter_transformer.transform(
        EventEnvelope(
            event_type=EventType.ENTITY_CHANGE_EVENT, event=test_event, meta={}
        )
    )
    assert result is None


def test_returns_none_when_different_data_type():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {
            "event_type": "EntityChangeEvent",
            "event": {"field1": {"nested_1": {"nested_b": "a"}}},
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)
    test_event = TestEvent({"nested_1": ["a"]}, None)
    result = filter_transformer.transform(
        EventEnvelope(
            event_type=EventType.ENTITY_CHANGE_EVENT, event=test_event, meta={}
        )
    )
    assert result is None


def test_returns_match_when_either_is_present():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {
            "event_type": "EntityChangeEvent",
            "event": {"field1": {"nested_1": ["a", "b"]}},
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)
    test_event = TestEvent({"nested_1": "a"}, None)
    result = filter_transformer.transform(
        EventEnvelope(
            event_type=EventType.ENTITY_CHANGE_EVENT, event=test_event, meta={}
        )
    )
    assert result is not None
