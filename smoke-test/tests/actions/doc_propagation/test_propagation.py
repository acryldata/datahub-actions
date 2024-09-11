import logging
import os
import tempfile
from pathlib import Path
from random import randint
from typing import Any, List

import datahub.metadata.schema_classes as models
import pytest
from datahub.api.entities.dataset.dataset import Dataset
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.utilities.urns.urn import Urn

from tests.utils import (
    delete_urns_from_file,
    get_gms_url,
    ingest_file_via_rest,
    wait_for_writes_to_sync,
)

logger = logging.getLogger(__name__)


start_index = randint(10, 10000)
dataset_urns = [
    make_dataset_urn("snowflake", f"table_foo_{i}")
    for i in range(start_index, start_index + 10)
]


class FileEmitter:
    def __init__(self, filename: str) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id="create_test_data"),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event):
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self):
        self.sink.close()


def sanitize_field(field: models.SchemaFieldClass) -> models.SchemaFieldClass:
    if field.fieldPath.startswith("[version=2.0]"):
        field.fieldPath = field.fieldPath.split(".")[-1]

    return field


def sanitize(event: Any) -> Any:
    if isinstance(event, MetadataChangeProposalWrapper):
        if event.aspectName == "schemaMetadata":
            assert isinstance(event.aspect, models.SchemaMetadataClass)
            schema_metadata: models.SchemaMetadataClass = event.aspect
            schema_metadata.fields = [
                sanitize_field(field) for field in schema_metadata.fields
            ]

    return event


def create_test_data(filename: str, test_resources_dir: str) -> List[str]:
    def get_urns_from_mcp(mcp: MetadataChangeProposalWrapper) -> List[str]:
        assert mcp.entityUrn
        urns = [mcp.entityUrn]
        if mcp.aspectName == "schemaMetadata":
            dataset_urn = mcp.entityUrn
            assert isinstance(mcp.aspect, models.SchemaMetadataClass)
            schema_metadata: models.SchemaMetadataClass = mcp.aspect
            for field in schema_metadata.fields:
                field_urn = make_schema_field_urn(dataset_urn, field.fieldPath)
                urns.append(field_urn)
        return urns

    mcps = []
    all_urns = []
    for dataset in Dataset.from_yaml(file=f"{test_resources_dir}/datasets.yaml"):
        mcps.extend([sanitize(event) for event in dataset.generate_mcp()])

    file_emitter = FileEmitter(filename)
    for mcp in mcps:
        all_urns.extend(get_urns_from_mcp(mcp))
        file_emitter.emit(mcp)

    file_emitter.close()
    return list(set(all_urns))


@pytest.fixture(scope="module", autouse=False)
def root_dir(pytestconfig):
    return pytestconfig.rootdir


@pytest.fixture(scope="module", autouse=False)
def test_resources_dir(root_dir):
    return Path(root_dir) / "tests" / "actions" / "doc_propagation" / "resources"


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(request, test_resources_dir, graph):
    new_file, filename = tempfile.mkstemp(suffix=".json")
    try:
        all_urns = create_test_data(filename, test_resources_dir)
        print("ingesting datasets test data")
        ingest_file_via_rest(filename)
        yield
        print("removing test data")
        delete_urns_from_file(filename)
        for urn in all_urns:
            graph.delete_entity(urn, hard=True)
        wait_for_writes_to_sync()
    finally:
        os.remove(filename)


@pytest.fixture(scope="module", autouse=False)
def graph():
    graph: DataHubGraph = DataHubGraph(config=DatahubClientConfig(server=get_gms_url()))
    yield graph


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


def add_col_col_lineage(graph):
    field_path = "ip"

    downstream_field = f"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,user.clicks,PROD),{field_path})"
    upstream_field = f"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:events,ClickEvent,PROD),{field_path})"
    dataset1 = "urn:li:dataset:(urn:li:dataPlatform:hive,user.clicks,PROD)"
    upstreams = graph.get_aspect(dataset1, models.UpstreamLineageClass)
    upstreams.fineGrainedLineages = [
        models.FineGrainedLineageClass(
            upstreamType=models.FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            downstreamType=models.FineGrainedLineageDownstreamTypeClass.FIELD,
            upstreams=[upstream_field],
            downstreams=[downstream_field],
        )
    ]
    graph.emit(MetadataChangeProposalWrapper(entityUrn=dataset1, aspect=upstreams))
    wait_for_writes_to_sync()
    return downstream_field, upstream_field


def add_field_description(f1, description, graph):
    urn = Urn.from_string(f1)
    dataset_urn = urn.entity_ids[0]
    schema_metadata = graph.get_aspect(dataset_urn, models.SchemaMetadataClass)
    field = next(f for f in schema_metadata.fields if f.fieldPath == urn.entity_ids[1])
    field.description = description
    graph.emit(
        MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=schema_metadata)
    )
    wait_for_writes_to_sync()


def check_propagated_description(downstream_field, description, graph):
    documentation = graph.get_aspect(downstream_field, models.DocumentationClass)
    assert any(doc.documentation == description for doc in documentation.documentations)


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_col_col_propagation(ingest_cleanup_data, graph):
    downstream_field, upstream_field = add_col_col_lineage(graph)
    add_field_description(upstream_field, "This is the new description", graph)
    check_propagated_description(downstream_field, "This is the new description", graph)
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    print("test_col_col_propagation")
    pass
