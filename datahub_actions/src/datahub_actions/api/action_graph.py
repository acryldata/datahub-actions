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

# TODO: John Clean this up.

import json
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from datahub.configuration.common import OperationalError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeProposal
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    DatasetSnapshotClass,
    GenericAspectClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MetadataChangeEventClass,
    TagAssociationClass,
)


@dataclass
class AcrylDataHubGraph:
    def __init__(self, baseGraph: DataHubGraph):
        self.graph = baseGraph

    def get_by_query(
        self,
        query: str,
        entity: str,
        start: int = 0,
        count: int = 100,
        filters: Optional[Dict] = None,
    ) -> List[Dict]:
        url_frag = "/entities?action=search"
        url = f"{self.graph._gms_server}{url_frag}"
        payload = {"input": query, "start": start, "count": count, "entity": entity}
        if filters is not None:
            payload["filter"] = filters

        headers = {
            "X-RestLi-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        }

        try:
            response = self.graph._session.post(
                url, data=json.dumps(payload), headers=headers
            )
            if response.status_code != 200:
                return []
            json_resp = response.json()
            return json_resp.get("value", {}).get("entities")
        except Exception as e:
            print(e)
            return []

    def get_by_graphql_query(self, query: Dict) -> Dict:
        url_frag = "/api/graphql"
        url = f"{self.graph._gms_server}{url_frag}"

        headers = {
            "X-DataHub-Actor": "urn:li:corpuser:admin",
            "Content-Type": "application/json",
        }
        try:
            response = self.graph._session.post(
                url, data=json.dumps(query), headers=headers
            )
            if response.status_code != 200:
                return {}
            json_resp = response.json()
            return json_resp.get("data", {})
        except Exception as e:
            print(e)
            return {}

    def query_constraints_for_dataset(self, dataset_id: str) -> List:
        resp = self.get_by_graphql_query(
            {
                "query": """
query dataset($input: String!) {
  dataset(urn: $input) {
    constraints {
      type
      displayName
      description
      params {
        hasGlossaryTermInNodeParams {
          nodeName
        }
      }
    }
  }
}
""",
                "variables": {"input": dataset_id},
            }
        )
        constraints: List = resp.get("dataset", {}).get("constraints", [])
        return constraints

    def query_execution_result_details(self, execution_id: str) -> Any:
        resp = self.get_by_graphql_query(
            {
                "query": """
query executionRequest($urn: String!) {
  executionRequest(urn: $urn) {
    input {
      task
      arguments {
        key
        value
      }
    }
  }
}
""",
                "variables": {"urn": f"urn:li:dataHubExecutionRequest:{execution_id}"},
            }
        )
        return resp.get("executionRequest", {}).get("input", {})

    def query_ingestion_sources(self) -> List:
        sources = []
        start, count = 0, 10
        while True:
            resp = self.get_by_graphql_query(
                {
                    "query": """
query listIngestionSources($input: ListIngestionSourcesInput!, $execution_start: Int!, $execution_count: Int!) {
  listIngestionSources(input: $input) {
    start
    count
    total
    ingestionSources {
      urn
      type
      name
      executions(start: $execution_start, count: $execution_count) {
        start
        count
        total
        executionRequests {
          urn
        }
      }
    }
  }
}
""",
                    "variables": {
                        "input": {"start": start, "count": count},
                        "execution_start": 0,
                        "execution_count": 10,
                    },
                }
            )
            listIngestionSources = resp.get("listIngestionSources", {})
            sources.extend(listIngestionSources.get("ingestionSources", []))

            cur_total = listIngestionSources.get("total", 0)
            if cur_total > count:
                start += count
            else:
                break
        return sources

    def query_proposals_for_user(
        self, user_id: str, start: int = 0, count: int = 10, pending: bool = True
    ) -> Dict:

        if pending:
            status = "PENDING"
        else:
            status = "COMPLETED"

        resp = self.get_by_graphql_query(
            {
                "query": """
query listActionRequests($input: ListActionRequestsInput!) {
  listActionRequests(input: $input) {
    start
    count
    total
    actionRequests {
      entity {
        urn
        type
      }
      created {
        time
        actor {
          urn
        }
      }
      urn
      type
      params {
        glossaryTermProposal {
          glossaryTerm {
            urn
          }
        }
        tagProposal {
          tag {
            urn
          }
        }
      }
    }
  }
}
            """,
                "variables": {
                    "input": {
                        "start": start,
                        "count": count,
                        "status": status,
                        "assignee": {"type": "USER", "urn": user_id},
                    }
                },
            }
        )
        listActionRequests: Dict = resp.get("listActionRequests", {})
        return listActionRequests.get("actionRequests", {})

    def get_downstreams(self, entity_urn: str) -> List[str]:
        url_frag = f"/relationships?direction=INCOMING&types=List(DownstreamOf)&urn={urllib.parse.quote(entity_urn)}"
        url = f"{self.graph._gms_server}{url_frag}"
        response = self.graph._get_generic(url)
        if response["count"] > 0:
            relnships = response["relationships"]
            entities = [x["entity"] for x in relnships]
            return entities
        return []

    def get_relationships(
        self, entity_urn: str, direction: str, relationship_types: List[str]
    ) -> List[str]:
        url_frag = (
            f"/relationships?"
            f"direction={direction}"
            f"&types=List({','.join(relationship_types)})"
            f"&urn={urllib.parse.quote(entity_urn)}"
        )

        url = f"{self.graph._gms_server}{url_frag}"
        response = self.graph._get_generic(url)
        if response["count"] > 0:
            relnships = response["relationships"]
            entities = [x["entity"] for x in relnships]
            return entities
        return []

    def check_relationship(self, entity_urn, target_urn, relationship_type):
        url_frag = f"/relationships?direction=INCOMING&types=List({relationship_type})&urn={urllib.parse.quote(entity_urn)}"
        url = f"{self.graph._gms_server}{url_frag}"
        response = self.graph._get_generic(url)
        if response["count"] > 0:
            relnships = response["relationships"]
            entities = [x["entity"] for x in relnships]
            return target_urn in entities
        return False

    def add_tags_to_dataset(
        self, entity_urn: str, dataset_tags: List[str], field_tags: Dict = {}
    ) -> None:
        aspect = "globalTags"
        global_tags = (
            self.graph.get_aspect(
                entity_urn,
                aspect,
                aspect_type_name="com.linkedin.common.GlobalTags",
                aspect_type=GlobalTagsClass,
            )
            or GlobalTagsClass.construct_with_defaults()
        )

        tag_map = {}
        for tag_assoc in global_tags.tags:
            tag_map[tag_assoc.tag] = tag_assoc

        will_write = False
        if len(tag_map.keys()) != len(global_tags.tags):
            # we have dups
            will_write = True
            global_tags.tags = [tag_assoc for tag_assoc in tag_map.values()]

        for tag in dataset_tags:
            if tag not in tag_map:
                global_tags.tags.append(TagAssociationClass(tag))
                will_write = True

        if will_write:
            # TODO: Return mcp-s back to caller instead of performing the write ourselves
            self.graph.emit_mce(
                MetadataChangeEventClass(
                    proposedSnapshot=DatasetSnapshotClass(
                        urn=entity_urn,
                        aspects=[global_tags],
                    )
                )
            )
            # self.emit_mcp(
            #    MetadataChangeProposalWrapper(
            #        entityType="dataset",
            #        changeType=ChangeTypeClass.UPSERT,
            #        entityUrn=entity_urn,
            #        aspectName=aspect,
            #        aspect=global_tags,
            #    )
            # )

    def add_terms_to_dataset(
        self, entity_urn: str, dataset_terms: List[str], field_terms: Dict = {}
    ) -> None:
        aspect = "glossaryTerms"

        glossary_terms = self.graph.get_aspect(
            entity_urn,
            aspect,
            aspect_type_name="com.linkedin.common.GlossaryTerms",
            aspect_type=GlossaryTermsClass,
        )

        if not glossary_terms:
            glossary_terms = GlossaryTermsClass(
                terms=[],
                auditStamp=AuditStampClass(
                    time=int(time.time() * 1000.0), actor="urn:li:corpUser:datahub"
                ),
            )

        tag_map = {}
        for term in glossary_terms.terms:
            tag_map[term.urn] = term

        will_write = False
        if len(tag_map.keys()) != len(glossary_terms.terms):
            # we have dups
            will_write = True
            glossary_terms.terms = [tag_assoc for tag_assoc in tag_map.values()]

        for tag in dataset_terms:
            if tag not in tag_map:
                glossary_terms.terms.append(GlossaryTermAssociationClass(tag))
                will_write = True

        if will_write:
            # TODO: Should return mcp-s to caller instead of performing the write ourselves
            self.graph.emit_mce(
                MetadataChangeEventClass(
                    proposedSnapshot=DatasetSnapshotClass(
                        urn=entity_urn,
                        aspects=[glossary_terms],
                    )
                )
            )
            # self.emit_mcp(
            #    MetadataChangeProposalWrapper(
            #        entityType="dataset",
            #        changeType=ChangeTypeClass.UPSERT,
            #        entityUrn=entity_urn,
            #        aspectName=aspect,
            #        aspect=global_tags,
            #    )
            # )

    def get_corpuser_info(self, urn: str) -> Any:
        return self.get_untyped_aspect(
            urn, "corpUserInfo", "com.linkedin.identity.CorpUserInfo"
        )

    def add_assignees_to_proposal(
        self, proposal_urn: str, owner_urns: List[str]
    ) -> None:
        aspect = "actionRequestInfo"
        action_request_info = self.get_untyped_aspect(
            proposal_urn, aspect, "com.linkedin.actionrequest.ActionRequestInfo"
        )

        assigned_users: List[str] = action_request_info.get("assignedUsers", [])
        assigned_groups: List[str] = action_request_info.get("assignedGroup", [])
        has_changed = False

        for owner_urn in owner_urns:
            owner_urn_parts = owner_urn.split(":")
            owner_urn_type = owner_urn_parts[2]
            if owner_urn_type == "corpuser":
                if owner_urn not in assigned_users:
                    assigned_users.append(owner_urn)
                    has_changed = True
            elif owner_urn_type == "corpGroup":
                if owner_urn not in assigned_groups:
                    assigned_groups.append(owner_urn)
                    has_changed = True

        if has_changed:
            action_request_info["assignedUsers"] = assigned_users
            action_request_info["assignedGroups"] = assigned_groups
            self.graph.emit_mcp(
                MetadataChangeProposal(
                    entityType="actionRequest",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=proposal_urn,
                    aspectName=aspect,
                    aspect=GenericAspectClass(
                        contentType="application/json",
                        value=json.dumps(action_request_info).encode(),
                    ),
                )
            )

    def remove_assignees_to_proposal(
        self, proposal_urn: str, owner_urns_to_remove: List[str]
    ) -> None:
        aspect = "actionRequestInfo"
        action_request_info = self.get_untyped_aspect(
            proposal_urn, aspect, "com.linkedin.actionrequest.ActionRequestInfo"
        )

        assigned_users: List[str] = action_request_info.get("assignedUsers", [])
        assigned_groups: List[str] = action_request_info.get("assignedGroup", [])
        has_changed = False

        for owner_urn in owner_urns_to_remove:
            owner_urn_parts = owner_urn.split(":")
            owner_urn_type = owner_urn_parts[2]
            if owner_urn_type == "corpuser":
                if owner_urn in assigned_users:
                    assigned_users.remove(owner_urn)
                    has_changed = True
            elif owner_urn_type == "corpGroup":
                if owner_urn in assigned_groups:
                    assigned_groups.remove(owner_urn)
                    has_changed = True

        if has_changed:
            action_request_info["assignedUsers"] = assigned_users
            action_request_info["assignedGroups"] = assigned_groups
            self.graph.emit_mcp(
                MetadataChangeProposal(
                    entityType="actionRequest",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=proposal_urn,
                    aspectName=aspect,
                    aspect=GenericAspectClass(
                        contentType="application/json",
                        value=json.dumps(action_request_info).encode(),
                    ),
                )
            )

    def get_untyped_aspect(
        self,
        entity_urn: str,
        aspect: str,
        aspect_type_name: str,
    ) -> Any:
        url = f"{self.graph._gms_server}/aspects/{urllib.parse.quote(entity_urn)}?aspect={aspect}&version=0"
        response = self.graph._session.get(url)
        if response.status_code == 404:
            # not found
            return None
        response.raise_for_status()
        response_json = response.json()
        aspect_json = response_json.get("aspect", {}).get(aspect_type_name)
        if aspect_json:
            return aspect_json
        else:
            raise OperationalError(
                f"Failed to find {aspect_type_name} in response {response_json}"
            )
