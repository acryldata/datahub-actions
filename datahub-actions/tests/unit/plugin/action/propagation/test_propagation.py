import json
from typing import Optional
from unittest.mock import MagicMock, patch

import datahub.metadata.schema_classes as models
import freezegun
import pytest

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.propagation.generic_propagation_action import (
    GenericPropagationAction,
    PropertyPropagationConfig,
)
from datahub_actions.plugin.action.propagation.propagation_utils import (
    DirectionType,
    PropagationRelationships,
    PropertyType,
    RelationshipType,
)


@pytest.fixture
def config():
    return PropertyPropagationConfig(
        enabled=True,
        supported_properties=[PropertyType.DOCUMENTATION, PropertyType.TAG],
        entity_types_enabled={"schemaField": True, "dataset": False},
        propagation_relationships=[
            PropagationRelationships.SIBLING,
        ],
    )


@pytest.fixture
def graph():
    return MagicMock()


@pytest.fixture
def ctx(graph):
    return PipelineContext(pipeline_name="test_pipeline", graph=graph)


@pytest.fixture
def action(
    config: PropertyPropagationConfig, ctx: PipelineContext
) -> GenericPropagationAction:
    return GenericPropagationAction(config=config, ctx=ctx)


@pytest.fixture
def event():
    return EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="DOCUMENTATION",
            entityType="schemaField",
            entityUrn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
            parameters={  # type: ignore
                "fieldPath": "0",
                "description": "test docs",
            },
            operation="UPDATE",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )


@pytest.fixture
def tag_event():
    return EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="TAG",
            entityType="dataset",
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
            modifier="urn:li:tag:tag1",
            operation="ADD",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )


def create_action(
    graph_mock: Optional[MagicMock] = None,
    propagation_config: Optional[PropertyPropagationConfig] = None,
) -> GenericPropagationAction:
    config = (
        PropertyPropagationConfig(
            enabled=True,
            supported_properties=[PropertyType.DOCUMENTATION, PropertyType.TAG],
            entity_types_enabled={"schemaField": True, "dataset": False},
            propagation_relationships=[
                PropagationRelationships.SIBLING,
                PropagationRelationships.DOWNSTREAM,
                PropagationRelationships.UPSTREAM,
            ],
        )
        if not propagation_config
        else propagation_config
    )
    graph = MagicMock() if not graph_mock else graph_mock
    ctx = PipelineContext(pipeline_name="test_pipeline", graph=graph)
    return GenericPropagationAction(config=config, ctx=ctx)


def test_act_async_siblings():
    action = create_action()
    action._rate_limited_emit_mcp = MagicMock()
    graph_mock = MagicMock()
    graph_mock.graph = MagicMock()
    graph_mock.graph.get_aspect.configure_mock(
        side_effect=lambda urn, aspect_type: {
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
                models.SiblingsClass,
            ): models.SiblingsClass(
                siblings=[
                    "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_sibling1,PROD)"
                ],
                primary=True,
            ),
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_sibling1,PROD)",
                models.SchemaMetadataClass,
            ): models.SchemaMetadataClass(
                schemaName="my_database.my_table_sibling1",
                platform="urn:li:dataPlatform:hive",
                version=1,
                platformSchema=models.MySqlDDLClass(tableSchema="table_schema"),
                fields=[
                    models.SchemaFieldClass(
                        fieldPath="0",
                        type=models.SchemaFieldDataTypeClass(models.StringTypeClass()),
                        description="sibling's field",
                        nullable=True,
                        recursive=False,
                        nativeDataType="string",
                    )
                ],
                hash="hash",
            ),
        }.get((urn, aspect_type))
    )
    action = create_action(graph_mock)
    test_event = EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="DOCUMENTATION",
            entityType="schemaField",
            entityUrn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
            parameters={  # type: ignore
                "fieldPath": "0",
                "description": "test docs",
            },
            operation="UPDATE",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )
    results = list(action.act_async(test_event))
    assert len(results) == 1


@patch(
    "datahub_actions.plugin.action.propagation.propagator.EntityPropagator.create_property_change_proposal"
)
def test_propagation_upstream(mock_create_property_change_proposal):
    action = create_action()
    action._rate_limited_emit_mcp = MagicMock()
    graph_mock = MagicMock()
    graph_mock.graph = MagicMock()
    mock_create_property_change_proposal.return_value = MagicMock()
    graph_mock.get_upstreams.configure_mock(
        side_effect=lambda entity_urn: {
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)": [
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD),0)"
            ]
        }.get(entity_urn, [])
    )
    graph_mock.graph.get_aspect.configure_mock(
        side_effect=lambda urn, aspect_type: {
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD)",
                models.SchemaMetadataClass,
            ): models.SchemaMetadataClass(
                schemaName="my_database.my_table_upstream1",
                platform="urn:li:dataPlatform:hive",
                version=1,
                platformSchema=models.MySqlDDLClass(tableSchema="table_schema"),
                fields=[
                    models.SchemaFieldClass(
                        fieldPath="0",
                        type=models.SchemaFieldDataTypeClass(models.StringTypeClass()),
                        description="upstream's field",
                        nullable=True,
                        recursive=False,
                        nativeDataType="string",
                    )
                ],
                hash="hash",
            )
        }.get((urn, aspect_type))
    )

    action = create_action(graph_mock)
    test_event = EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="DOCUMENTATION",
            entityType="schemaField",
            entityUrn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
            parameters={  # type: ignore
                "fieldPath": "0",
                "description": "test docs",
            },
            operation="UPDATE",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )
    results = list(action.act_async(test_event))
    assert len(results) == 1
    assert (
        results[0].entityUrn
        == "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD),0)"
    )
    assert results[0].aspectName == "documentation"
    documentation_aspect = results[0].aspect
    assert isinstance(documentation_aspect, models.DocumentationClass)
    assert documentation_aspect.documentations[0].documentation == "test docs"


@freezegun.freeze_time("2025-01-01 00:00:00+00:00")
def test_tag_propagation_upstream():
    action = create_action()
    action._rate_limited_emit_mcp = MagicMock()
    graph_mock = MagicMock()
    graph_mock.graph = MagicMock()
    graph_mock.get_upstreams.configure_mock(
        side_effect=lambda entity_urn: {
            "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)": [
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD)"
            ]
        }.get(entity_urn, [])
    )

    action = create_action(graph_mock)
    test_event = EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="TAG",
            entityType="dataset",
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
            modifier="urn:li:tag:tag1",
            operation="ADD",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )
    list(action.act_async(test_event))
    graph_mock.add_tags_to_dataset.assert_called_with(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD)",
        ["urn:li:tag:tag1"],
        context={
            "origin": "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
            "via": "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
            "propagated": "true",
            "actor": "urn:li:corpuser:__datahub_system",
            "propagation_started_at": 1735689600000,
            "propagation_depth": 1,
            "propagation_relationship": RelationshipType.LINEAGE,
            "propagation_direction": DirectionType.UP,
        },
    )


def test_propagation_disabled(
    config: PropertyPropagationConfig,
    action: GenericPropagationAction,
    event: EventEnvelope,
) -> None:
    config.enabled = False
    action._rate_limited_emit_mcp = MagicMock()
    results = list(action.act_async(event))
    assert len(results) == 0
    assert action._rate_limited_emit_mcp.call_count == 0


def test_sibling_propagation() -> None:
    config = PropertyPropagationConfig(
        enabled=True,
        supported_properties=[PropertyType.DOCUMENTATION, PropertyType.TAG],
        entity_types_enabled={"schemaField": True, "dataset": False},
        propagation_relationships=[
            PropagationRelationships.SIBLING,
        ],
    )

    graph_mock = MagicMock()
    graph_mock.graph = MagicMock()
    graph_mock.graph.get_aspect.configure_mock(
        side_effect=lambda urn, aspect_type: {
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
                models.SiblingsClass,
            ): models.SiblingsClass(
                siblings=[
                    "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_sibling1,PROD)"
                ],
                primary=True,
            ),
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_sibling1,PROD)",
                models.SchemaMetadataClass,
            ): models.SchemaMetadataClass(
                schemaName="my_database.my_table_sibling1",
                platform="urn:li:dataPlatform:hive",
                version=1,
                platformSchema=models.MySqlDDLClass(tableSchema="table_schema"),
                fields=[
                    models.SchemaFieldClass(
                        fieldPath="0",
                        type=models.SchemaFieldDataTypeClass(models.StringTypeClass()),
                        description="sibling's field",
                        nullable=True,
                        recursive=False,
                        nativeDataType="string",
                    )
                ],
                hash="hash",
            ),
        }.get((urn, aspect_type))
    )
    action = create_action(graph_mock=graph_mock, propagation_config=config)
    action._rate_limited_emit_mcp = MagicMock()

    test_event = EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="DOCUMENTATION",
            entityType="schemaField",
            entityUrn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
            parameters={  # type: ignore
                "fieldPath": "0",
                "description": "test docs",
            },
            operation="UPDATE",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )
    results = list(action.act_async(test_event))

    assert len(results) == 1
    assert (
        results[0].entityUrn
        == "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_sibling1,PROD),0)"
    )
    assert results[0].aspectName == "documentation"
    documentation_aspect = results[0].aspect
    assert documentation_aspect.documentations[0].documentation == "test docs"  # type: ignore


def test_unsupported_property_type(action: GenericPropagationAction) -> None:
    aspect_value = MagicMock()
    aspect_value.value = json.dumps({"unsupported_property": "value"})

    result = action._extract_property_value("unsupported_property", aspect_value)
    assert result is None
