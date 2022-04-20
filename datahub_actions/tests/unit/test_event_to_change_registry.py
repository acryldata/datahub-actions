from datahub_actions.api.action_core import (
    EntityType,
    RawMetadataChange,
    SemanticChange,
    Subscription,
)
from datahub_actions.events.event_to_change_registry import (
    get_events_to_aspect_mapping,
    get_subscriptions_to_semantic_events,
)


def test_get_events_to_aspect_mapping():
    assert get_events_to_aspect_mapping(
        RawMetadataChange(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
            entity_type=EntityType.DATASET,
            added=[{"urn": "urn:li:glossaryTerm:Classification.Confidential"}],
            removed=[],
            changed=[],
            original_event={
                "auditHeader": None,
                "entityType": "dataset",
                "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
                "entityKeyAspect": None,
                "changeType": "UPSERT",
                "aspectName": "editableSchemaMetadata",
                "aspect": (
                    "com.linkedin.pegasus2avro.mxe.GenericAspect",
                    {
                        "value": b'{"editableSchemaFieldInfo":[{"fieldPath":"user_id","glossaryTerms":{"auditStamp":{"actor":"urn:li:corpuser:admin","time":1644942935508},"terms":[{"urn":"urn:li:glossaryTerm:Classification.Confidential"}]}}]}',
                        "contentType": "application/json",
                    },
                ),
                "systemMetadata": (
                    "com.linkedin.pegasus2avro.mxe.SystemMetadata",
                    {
                        "lastObserved": 1644942935509,
                        "runId": "no-run-id-provided",
                        "registryName": "unknownRegistry",
                        "registryVersion": "0.0.0.0-dev",
                        "properties": None,
                    },
                ),
                "previousAspectValue": None,
                "previousSystemMetadata": None,
            },
            aspect_name="editableSchemaMetadata",
        )
    ) == {
        "AddTermEvent": [{"urn": "urn:li:glossaryTerm:Classification.Confidential"}],
    }


def test_raw_metadata_change_mapping_with_no_aspect():
    data = RawMetadataChange(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        entity_type=EntityType.DATASET,
        added=[],
        removed=[],
        changed=[],
        original_event={},
        aspect_name=None,
    )
    assert get_events_to_aspect_mapping(data) == {}


def test_raw_metadata_change_mapping_with_unrecognized_aspect():
    data = RawMetadataChange(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        entity_type=EntityType.DATASET,
        added=[],
        removed=[],
        changed=[],
        original_event={},
        aspect_name="fake_aspect_not_recognized",
    )
    assert get_events_to_aspect_mapping(data) == {}


def test_raw_metadata_change_mapping_with_ownership_aspect_added():
    data = RawMetadataChange(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        entity_type=EntityType.DATASET,
        added=[
            {
                "owner": "urn:li:corpuser:aseem.bansal",
                "type": "DATAOWNER",
                "source": {"type": "MANUAL"},
            }
        ],
        removed=[],
        changed=[],
        original_event={},
        aspect_name="ownership",
    )
    assert get_events_to_aspect_mapping(data) == {
        "AddOwnerEvent": [
            {
                "owner": "urn:li:corpuser:aseem.bansal",
                "type": "DATAOWNER",
                "source": {"type": "MANUAL"},
            }
        ]
    }


def test_raw_metadata_change_mapping_with_ownership_aspect_removed():
    data = RawMetadataChange(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        entity_type=EntityType.DATASET,
        added=[],
        removed=[
            {
                "owner": "urn:li:corpuser:aseem.bansal",
                "type": "DATAOWNER",
                "source": {"type": "MANUAL"},
            }
        ],
        changed=[],
        original_event={},
        aspect_name="ownership",
    )
    assert get_events_to_aspect_mapping(data) == {
        "RemoveOwnerEvent": [
            {
                "owner": "urn:li:corpuser:aseem.bansal",
                "type": "DATAOWNER",
                "source": {"type": "MANUAL"},
            }
        ]
    }


def test_get_events_to_aspect_mapping_with_ownership_aspect_removed():
    data = RawMetadataChange(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        entity_type=EntityType.DATASET,
        added=[],
        removed=[
            {
                "owner": "urn:li:corpuser:aseem.bansal",
                "type": "DATAOWNER",
                "source": {"type": "MANUAL"},
            }
        ],
        changed=[],
        original_event={},
        aspect_name="ownership",
    )
    assert get_events_to_aspect_mapping(data) == {
        "RemoveOwnerEvent": [
            {
                "owner": "urn:li:corpuser:aseem.bansal",
                "type": "DATAOWNER",
                "source": {"type": "MANUAL"},
            }
        ]
    }


def test_get_subscriptions_to_semantic_events_for_correct_add_owner_event():
    data = RawMetadataChange(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        entity_type=EntityType.DATASET,
        added=[
            {
                "owner": "urn:li:corpuser:aseem.bansal",
                "type": "DATAOWNER",
                "source": {"type": "MANUAL"},
            }
        ],
        removed=[],
        changed=[],
        original_event={},
        aspect_name="ownership",
    )
    subscription = Subscription(event_name="AddOwnerEvent")
    assert get_subscriptions_to_semantic_events(data, [subscription]) == {
        subscription: [
            SemanticChange(
                change_type="AddOwnerEvent",
                entity_type="DATASET",
                entity_id="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
                entity_key={},
                attrs={
                    "owner": "urn:li:corpuser:aseem.bansal",
                    "type": "DATAOWNER",
                    "source": {"type": "MANUAL"},
                },
            )
        ]
    }


def test_get_subscriptions_to_semantic_events_for_incorrect_add_owner_event():
    data = RawMetadataChange(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        entity_type=EntityType.DATASET,
        added=[
            {
                "owner": "urn:li:corpuser:aseem.bansal",
                "type": "DATAOWNER",
                "source": {"type": "MANUAL"},
            }
        ],
        removed=[],
        changed=[],
        original_event={},
        aspect_name="ownership",
    )
    subscription = Subscription(event_name="RemoveOwnerEvent")
    assert get_subscriptions_to_semantic_events(data, [subscription]) == {}
