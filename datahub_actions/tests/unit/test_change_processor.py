# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datahub_actions.api.action_core import EntityType, RawMetadataChange
from datahub_actions.utils.change_processor import MetadataChangeProcessor


def test_MetadataChangeProcessor_calculate_delta_from_mcl_for_AddOwnerEvent():
    input = {
        "auditHeader": None,
        "entityType": "dataset",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "entityKeyAspect": None,
        "changeType": "UPSERT",
        "aspectName": "ownership",
        "aspect": (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"owners":[{"owner":"urn:li:corpuser:aseem.bansal","type":"DATAOWNER","source":{"type":"MANUAL"}}],"lastModified":{"actor":"urn:li:corpuser:datahub","time":1643907350936}}',
                "contentType": "application/json",
            },
        ),
        "systemMetadata": (
            "com.linkedin.pegasus2avro.mxe.SystemMetadata",
            {
                "lastObserved": 1643907400013,
                "runId": "no-run-id-provided",
                "registryName": "unknownRegistry",
                "registryVersion": "0.0.0.0-dev",
                "properties": None,
            },
        ),
        "previousAspectValue": (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"owners":[],"lastModified":{"actor":"urn:li:corpuser:datahub","time":1643907350936}}',
                "contentType": "application/json",
            },
        ),
        "previousSystemMetadata": (
            "com.linkedin.pegasus2avro.mxe.SystemMetadata",
            {
                "lastObserved": 1643907350940,
                "runId": "no-run-id-provided",
                "registryName": "unknownRegistry",
                "registryVersion": "0.0.0.0-dev",
                "properties": None,
            },
        ),
    }

    output = MetadataChangeProcessor.calculate_delta_from_mcl(input)

    assert output == RawMetadataChange(
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
        original_event=input,
        aspect_name="ownership",
    )
