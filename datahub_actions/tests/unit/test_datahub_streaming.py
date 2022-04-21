from typing import Any

from datahub_actions.actions.hello_world.hello_world import (
    HelloWorldAction,
    HelloWorldConfig,
)
from datahub_actions.api.action_core import (
    EntityType,
    RawMetadataChange,
    SemanticChange,
    Subscription,
)
from datahub_actions.events.add_owner_event import AddOwnerEvent
from datahub_actions.events.add_tag_event import AddTagEvent
from datahub_actions.events.add_term_event import AddTermEvent
from datahub_actions.events.datahub_execution_request_result_event import (
    DataHubExecutionRequestResultEvent,
)
from datahub_actions.events.event_to_change_registry import (
    get_helper_for_event,
    get_recognized_events,
)
from datahub_actions.events.remove_owner_event import RemoveOwnerEvent
from datahub_actions.source.datahub_streaming import handle_mcl


def get_dummy_action(subscribe_to_all=False):
    return HelloWorldAction(
        HelloWorldConfig(subscribe_to_all=subscribe_to_all), None  # type: ignore
    )


def assert_only_one_event_present(event_name: str, action: HelloWorldAction) -> Any:
    for event in get_recognized_events():
        event_objs = get_helper_for_event(event).from_semantic_changes(
            None,
            action.raw_change,
            action.semantic_changes,
        )
        print(f"Input {event_name} testing for {event} got {event_objs}")
        if event_name == event:
            assert len(event_objs) == 1
        else:
            assert len(event_objs) == 0
        print(f"Input {event_name} testing for {event} Passed")


def test_correct_args_sent_to_dummy_action_for_managed_ingestion_event():
    mcl = {
        "entityUrn": "String1",
        "entityType": "String2",
    }
    action = get_dummy_action(subscribe_to_all=True)

    for item in handle_mcl(mcl, [action]):
        break
    assert action.raw_change == RawMetadataChange(
        entity_urn=mcl["entityUrn"],
        entity_type=EntityType.UNKNOWN,
        original_event=mcl,
        added=[],
        removed=[],
        changed=[],
        aspect_name="None",
    )


def test_correct_args_sent_to_dummy_action_for_add_owner_event():
    mcl = {
        "auditHeader": None,
        "entityType": "dataset",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "entityKeyAspect": None,
        "changeType": "UPSERT",
        "aspectName": "ownership",
        "aspect": (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"owners":[{"owner":"urn:li:corpuser:aseem.bansal","type":"DATAOWNER","source":{"type":"MANUAL"}}],"lastModified":{"actor":"urn:li:corpuser:admin","time":1644305321992}}',
                "contentType": "application/json",
            },
        ),
        "systemMetadata": (
            "com.linkedin.pegasus2avro.mxe.SystemMetadata",
            {
                "lastObserved": 1644305372882,
                "runId": "no-run-id-provided",
                "registryName": "unknownRegistry",
                "registryVersion": "0.0.0.0-dev",
                "properties": None,
            },
        ),
        "previousAspectValue": (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"owners":[],"lastModified":{"actor":"urn:li:corpuser:admin","time":1644305321992}}',
                "contentType": "application/json",
            },
        ),
        "previousSystemMetadata": (
            "com.linkedin.pegasus2avro.mxe.SystemMetadata",
            {
                "lastObserved": 1644305321993,
                "runId": "no-run-id-provided",
                "registryName": "unknownRegistry",
                "registryVersion": "0.0.0.0-dev",
                "properties": None,
            },
        ),
    }
    action = get_dummy_action()

    for item in handle_mcl(mcl, [action]):
        break

    assert action.semantic_changes == [
        SemanticChange(
            change_type=AddOwnerEvent.__name__,
            entity_type="DATASET",
            entity_id="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
            entity_key={},
            attrs={
                "type": "DATAOWNER",
                "owner": "urn:li:corpuser:aseem.bansal",
                "source": {"type": "MANUAL"},
            },
        )
    ]
    assert action.raw_change == RawMetadataChange(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        entity_type=EntityType.DATASET,
        original_event=mcl,
        added=[
            {
                "type": "DATAOWNER",
                "owner": "urn:li:corpuser:aseem.bansal",
                "source": {"type": "MANUAL"},
            }
        ],
        removed=[],
        changed=[],
        aspect_name="ownership",
    )
    assert action.match == Subscription(event_name=AddOwnerEvent.__name__)

    assert_only_one_event_present(AddOwnerEvent.__name__, action)


def test_correct_args_sent_to_dummy_action_for_remove_owner_event():
    mcl = {
        "auditHeader": None,
        "entityType": "dataset",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "entityKeyAspect": None,
        "changeType": "UPSERT",
        "aspectName": "ownership",
        "aspect": (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"owners":[],"lastModified":{"actor":"urn:li:corpuser:admin","time":1644301505220}}',
                "contentType": "application/json",
            },
        ),
        "systemMetadata": (
            "com.linkedin.pegasus2avro.mxe.SystemMetadata",
            {
                "lastObserved": 1644301505222,
                "runId": "no-run-id-provided",
                "registryName": "unknownRegistry",
                "registryVersion": "0.0.0.0-dev",
                "properties": None,
            },
        ),
        "previousAspectValue": (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"owners":[{"type":"DATAOWNER","owner":"urn:li:corpuser:aseem.bansal","source":{"type":"MANUAL"}}],"lastModified":{"actor":"urn:li:corpuser:admin","time":1644300864673}}',
                "contentType": "application/json",
            },
        ),
        "previousSystemMetadata": (
            "com.linkedin.pegasus2avro.mxe.SystemMetadata",
            {
                "lastObserved": 1644300872303,
                "runId": "no-run-id-provided",
                "registryName": "unknownRegistry",
                "registryVersion": "0.0.0.0-dev",
                "properties": None,
            },
        ),
    }
    action = get_dummy_action()

    for item in handle_mcl(mcl, [action]):
        break

    assert action.semantic_changes == [
        SemanticChange(
            change_type=RemoveOwnerEvent.__name__,
            entity_type="DATASET",
            entity_id="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
            entity_key={},
            attrs={
                "type": "DATAOWNER",
                "owner": "urn:li:corpuser:aseem.bansal",
                "source": {"type": "MANUAL"},
            },
        )
    ]
    assert action.raw_change == RawMetadataChange(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        entity_type=EntityType.DATASET,
        original_event=mcl,
        added=[],
        removed=[
            {
                "type": "DATAOWNER",
                "owner": "urn:li:corpuser:aseem.bansal",
                "source": {"type": "MANUAL"},
            }
        ],
        changed=[],
        aspect_name="ownership",
    )
    assert action.match == Subscription(event_name=RemoveOwnerEvent.__name__)

    assert_only_one_event_present(RemoveOwnerEvent.__name__, action)


def test_correct_args_sent_to_dummy_action_for_add_tag_event():
    mcl = {
        "auditHeader": None,
        "entityType": "dataset",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "entityKeyAspect": None,
        "changeType": "UPSERT",
        "aspectName": "globalTags",
        "aspect": (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"tags":[{"tag":"urn:li:tag:aseem"}]}',
                "contentType": "application/json",
            },
        ),
        "systemMetadata": (
            "com.linkedin.pegasus2avro.mxe.SystemMetadata",
            {
                "lastObserved": 1644305666783,
                "runId": "no-run-id-provided",
                "registryName": "unknownRegistry",
                "registryVersion": "0.0.0.0-dev",
                "properties": None,
            },
        ),
        "previousAspectValue": (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {"value": b'{"tags":[]}', "contentType": "application/json"},
        ),
        "previousSystemMetadata": (
            "com.linkedin.pegasus2avro.mxe.SystemMetadata",
            {
                "lastObserved": 1644305660068,
                "runId": "no-run-id-provided",
                "registryName": "unknownRegistry",
                "registryVersion": "0.0.0.0-dev",
                "properties": None,
            },
        ),
    }
    action = get_dummy_action()

    for item in handle_mcl(mcl, [action]):
        break

    assert action.semantic_changes == [
        SemanticChange(
            change_type=AddTagEvent.__name__,
            entity_type="DATASET",
            entity_id="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
            entity_key={},
            attrs={
                "tag": "urn:li:tag:aseem",
            },
        )
    ]
    assert action.raw_change == RawMetadataChange(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        entity_type=EntityType.DATASET,
        original_event=mcl,
        added=[
            {
                "tag": "urn:li:tag:aseem",
            }
        ],
        removed=[],
        changed=[],
        aspect_name="globalTags",
    )
    assert action.match == Subscription(event_name=AddTagEvent.__name__)

    assert_only_one_event_present(AddTagEvent.__name__, action)


def test_correct_args_sent_to_dummy_action_for_add_term_event():
    mcl = {
        "auditHeader": None,
        "entityType": "dataset",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "entityKeyAspect": None,
        "changeType": "UPSERT",
        "aspectName": "glossaryTerms",
        "aspect": (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"auditStamp":{"actor":"urn:li:corpuser:admin","time":1644305810797},"terms":[{"urn":"urn:li:glossaryTerm:SavingAccount"}]}',
                "contentType": "application/json",
            },
        ),
        "systemMetadata": (
            "com.linkedin.pegasus2avro.mxe.SystemMetadata",
            {
                "lastObserved": 1644305810801,
                "runId": "no-run-id-provided",
                "registryName": "unknownRegistry",
                "registryVersion": "0.0.0.0-dev",
                "properties": None,
            },
        ),
        "previousAspectValue": None,
        "previousSystemMetadata": None,
    }
    action = get_dummy_action()

    for item in handle_mcl(mcl, [action]):
        break

    assert action.semantic_changes == [
        SemanticChange(
            change_type=AddTermEvent.__name__,
            entity_type="DATASET",
            entity_id="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
            entity_key={},
            attrs={"urn": "urn:li:glossaryTerm:SavingAccount"},
        )
    ]
    assert action.raw_change == RawMetadataChange(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        entity_type=EntityType.DATASET,
        original_event=mcl,
        added=[{"urn": "urn:li:glossaryTerm:SavingAccount"}],
        removed=[],
        changed=[],
        aspect_name="glossaryTerms",
    )
    assert action.match == Subscription(event_name=AddTermEvent.__name__)

    assert_only_one_event_present(AddTermEvent.__name__, action)


def test_correct_args_sent_to_dummy_action_for_add_term_event_on_schema():
    mcl = {
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
    }

    action = get_dummy_action()

    for item in handle_mcl(mcl, [action]):
        break

    assert action.semantic_changes == [
        SemanticChange(
            change_type=AddTermEvent.__name__,
            entity_type="DATASET",
            entity_id="urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
            entity_key={},
            attrs={"urn": "urn:li:glossaryTerm:Classification.Confidential"},
        )
    ]
    assert action.raw_change == RawMetadataChange(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        entity_type=EntityType.DATASET,
        original_event=mcl,
        added=[{"urn": "urn:li:glossaryTerm:Classification.Confidential"}],
        removed=[],
        changed=[],
        aspect_name="editableSchemaMetadata",
    )
    assert action.match == Subscription(event_name=AddTermEvent.__name__)

    assert_only_one_event_present(AddTermEvent.__name__, action)


def test_correct_args_sent_to_dummy_action_for_execution_request_result_event():
    mcl = {
        "auditHeader": None,
        "entityType": "dataHubExecutionRequest",
        "entityUrn": None,
        "entityKeyAspect": (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"id": "39345be8-4e91-49da-8c8f-74adc7bb2b4d"}',
                "contentType": "application/json",
            },
        ),
        "changeType": "UPSERT",
        "aspectName": "dataHubExecutionRequestResult",
        "aspect": (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"report":"REPORT_DUMMY","startTimeMs":1645776135491,"durationMs":173927,"status":"FAILURE"}',
                "contentType": "application/json",
            },
        ),
        "systemMetadata": (
            "com.linkedin.pegasus2avro.mxe.SystemMetadata",
            {
                "lastObserved": 1645776309485,
                "runId": "no-run-id-provided",
                "registryName": "unknownRegistry",
                "registryVersion": "0.0.0.0-dev",
                "properties": None,
            },
        ),
        "previousAspectValue": (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"startTimeMs":1645776135491,"status":"RUNNING"}',
                "contentType": "application/json",
            },
        ),
        "previousSystemMetadata": (
            "com.linkedin.pegasus2avro.mxe.SystemMetadata",
            {
                "lastObserved": 1645776135506,
                "runId": "no-run-id-provided",
                "registryName": "unknownRegistry",
                "registryVersion": "0.0.0.0-dev",
                "properties": None,
            },
        ),
    }

    action = get_dummy_action()

    for item in handle_mcl(mcl, [action]):
        break

    assert action.raw_change == RawMetadataChange(
        entity_urn="",
        entity_type=EntityType.DATAHUB_EXECUTION_REQUEST,
        original_event=mcl,
        added=[],
        removed=[],
        changed=[
            {
                "report": "REPORT_DUMMY",
                "startTimeMs": 1645776135491,
                "durationMs": 173927,
                "status": "FAILURE",
            }
        ],
        aspect_name="dataHubExecutionRequestResult",
    )
    assert action.match == Subscription(
        event_name=DataHubExecutionRequestResultEvent.__name__
    )
    assert action.semantic_changes == [
        SemanticChange(
            change_type=DataHubExecutionRequestResultEvent.__name__,
            entity_type="DATAHUB_EXECUTION_REQUEST",
            entity_id="",
            entity_key={"id": "39345be8-4e91-49da-8c8f-74adc7bb2b4d"},
            attrs={
                "report": "REPORT_DUMMY",
                "startTimeMs": 1645776135491,
                "durationMs": 173927,
                "status": "FAILURE",
            },
        )
    ]

    assert_only_one_event_present(DataHubExecutionRequestResultEvent.__name__, action)
