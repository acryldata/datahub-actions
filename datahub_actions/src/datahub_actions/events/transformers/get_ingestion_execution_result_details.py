import json
import logging
from typing import Dict, List

from datahub_actions.api.action_core import (
    ActionContext,
    EntityType,
    InvocationParams,
    SemanticChange,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_recipe_from_execution_input(execution_input: Dict) -> Dict:
    task = execution_input.get("task")
    if task != "RUN_INGEST":
        return dict()

    arguments = execution_input.get("arguments")
    if type(arguments) != list:
        return dict()
    for arg in arguments:
        arg_key = arg.get("key")
        if arg_key != "recipe":
            continue
        arg_val = arg.get("value")
        return json.loads(arg_val)
    return dict()


def get_ingestion_source_name_with_execution_id(
    ingestion_sources: List[Dict], execution_id: str
) -> str:

    execution_urn_target = f"urn:li:dataHubExecutionRequest:{execution_id}"

    for source in ingestion_sources:
        curr_name = source.get("name", None)
        if curr_name is None:
            continue
        execution_requests = source.get("executions", {}).get("executionRequests", [])
        for execution_request in execution_requests:
            curr_urn = execution_request.get("urn", None)
            if curr_urn == execution_urn_target:
                return curr_name
    return "UNKNOWN"


def ingestion_execution_result_details(
    semantic_changes: List[SemanticChange],
    action_context: ActionContext,
    invocation_params: InvocationParams,
    params: dict,
) -> List[SemanticChange]:
    result = []
    for semantic_change in semantic_changes:
        if semantic_change.entity_type != EntityType.DATAHUB_EXECUTION_REQUEST.name:
            continue
        execution_id = str(semantic_change.entity_key.get("id"))
        execution_input = action_context.graph.query_execution_result_details(
            execution_id
        )
        recipe = get_recipe_from_execution_input(execution_input)
        source_type = recipe.get("source", {}).get("type", None)
        if source_type is None:
            continue
        semantic_change.attrs["source_type"] = source_type

        ingestion_sources = action_context.graph.query_ingestion_sources()
        source_name = get_ingestion_source_name_with_execution_id(
            ingestion_sources, execution_id
        )
        semantic_change.attrs["source_name"] = source_name

        result.append(semantic_change)

    return result
