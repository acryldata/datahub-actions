from unittest import mock

from datahub_actions.api.action_core import InvocationParams, SemanticChange
from datahub_actions.events.transformers.flatten_attr_map import flatten_attr_map


def test_flatten_attr_map():
    result = flatten_attr_map(
        [
            SemanticChange(
                change_type="",
                entity_type="",
                entity_id="",
                entity_key={},
                attrs={
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
                        "type": "DATASET",
                    },
                    "created": {
                        "time": 1645021517917,
                        "actor": {"urn": "urn:li:corpuser:admin"},
                    },
                    "urn": "urn:li:actionRequest:03c3fdc7-3147-48a1-af84-a1af1a45a202",
                    "type": "TERM_ASSOCIATION",
                    "params": {
                        "glossaryTermProposal": {
                            "glossaryTerm": {
                                "urn": "urn:li:glossaryTerm:CustomerAccount"
                            }
                        },
                        "tagProposal": None,
                    },
                },
            )
        ],
        mock.MagicMock(),
        InvocationParams(),
        {
            "flatten_attr": "params",
            "flatten_attr_to": "params_flat",
            "keep_attr": False,
        },
    )

    assert result[0].attrs == {
        "entity": {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
            "type": "DATASET",
        },
        "created": {
            "time": 1645021517917,
            "actor": {"urn": "urn:li:corpuser:admin"},
        },
        "urn": "urn:li:actionRequest:03c3fdc7-3147-48a1-af84-a1af1a45a202",
        "type": "TERM_ASSOCIATION",
        "params_flat": {
            "tagProposal": None,
            "glossaryTermProposal_glossaryTerm_urn": "urn:li:glossaryTerm:CustomerAccount",
        },
    }
