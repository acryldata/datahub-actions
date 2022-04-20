from unittest import mock

from datahub_actions.api.action_core import SemanticChange
from datahub_actions.events.transformers.filter_by_attr_value import (
    filter_by_attr_value,
)


def test_filter_by_attr_value_missing_attr():

    semantic_change = SemanticChange(
        change_type="",
        entity_type="",
        entity_id="",
        entity_key={},
        attrs={},
    )

    assert (
        filter_by_attr_value(
            [semantic_change],
            mock.MagicMock(),
            mock.MagicMock(),
            {"attr_name": "status", "attr_val_to_keep": "FAILURE"},
        )
        == []
    )


def test_filter_by_attr_value_correct_attr():
    semantic_change = SemanticChange(
        change_type="",
        entity_type="",
        entity_id="",
        entity_key={},
        attrs={"status": "FAILURE"},
    )

    assert filter_by_attr_value(
        [semantic_change],
        mock.MagicMock(),
        mock.MagicMock(),
        {"attr_name": "status", "attr_val_to_keep": "FAILURE"},
    ) == [semantic_change]


def test_filter_by_attr_value_incorrect_attr():
    semantic_change = SemanticChange(
        change_type="",
        entity_type="",
        entity_id="",
        entity_key={},
        attrs={"status": "SUCCESS"},
    )

    assert (
        filter_by_attr_value(
            [semantic_change],
            mock.MagicMock(),
            mock.MagicMock(),
            {"attr_name": "status", "attr_val_to_keep": "FAILURE"},
        )
        == []
    )
