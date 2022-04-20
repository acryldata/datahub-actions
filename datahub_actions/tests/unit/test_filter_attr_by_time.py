from unittest import mock

from freezegun import freeze_time

from datahub_actions.api.action_core import SemanticChange
from datahub_actions.events.transformers.filter_attr_by_time import filter_attr_by_time


@freeze_time("2022-02-17 12:00:00")
def test_filter_by_time():

    pending_proposals = [
        {
            "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
            "entity_type": "DATASET",
            "created_time": 1645109741635,
            "created_actor_urn": "urn:li:corpuser:admin",
            "urn": "urn:li:actionRequest:731f4cd2-54fe-43de-a41a-1aa3f1bc3179",
            "type": "TAG_ASSOCIATION",
            "params_glossaryTermProposal": None,
            "params_tagProposal_tag_urn": "urn:li:tag:dev_test",
        },
        {
            "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
            "entity_type": "DATASET",
            "created_time": 1640109741635,
            "created_actor_urn": "urn:li:corpuser:admin",
            "urn": "urn:li:actionRequest:731f4cd2-54fe-43de-a41a-1aa3f1bc3179",
            "type": "TAG_ASSOCIATION",
            "params_glossaryTermProposal": None,
            "params_tagProposal_tag_urn": "urn:li:tag:dev_test",
        },
    ]

    semantic_change_in = SemanticChange(
        change_type="",
        entity_type="",
        entity_id="",
        entity_key={},
        attrs={"pending_proposals": pending_proposals},
    )

    semantic_change_new_proposals = SemanticChange(
        change_type="",
        entity_type="",
        entity_id="",
        entity_key={},
        attrs={
            "pending_proposals": pending_proposals,
            "pending_proposals_new": [pending_proposals[0]],
        },
    )

    assert filter_attr_by_time(
        [semantic_change_in],
        mock.MagicMock(),
        mock.MagicMock(),
        {
            "attr": "pending_proposals",
            "time_field": "created_time",
            "new_attr": "pending_proposals_new",
            "window_seconds": 86400,
            "window_filter_type": "SINCE_BEGIN",
        },
    ) == [semantic_change_new_proposals]

    semantic_change_old_proposals = SemanticChange(
        change_type="",
        entity_type="",
        entity_id="",
        entity_key={},
        attrs={
            "pending_proposals": pending_proposals,
            "pending_proposals_new": [pending_proposals[1]],
        },
    )

    assert filter_attr_by_time(
        [semantic_change_in],
        mock.MagicMock(),
        mock.MagicMock(),
        {
            "attr": "pending_proposals",
            "time_field": "created_time",
            "new_attr": "pending_proposals_new",
            "window_seconds": 86400,
            "window_filter_type": "BEFORE_BEGIN",
        },
    ) == [semantic_change_old_proposals]
