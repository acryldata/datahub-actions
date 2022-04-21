import logging
from typing import Dict, List

from datahub_actions.actions.utils.datahub_util import sanitize_user_urn_for_search
from datahub_actions.api.action_core import (
    ActionContext,
    EntityType,
    InvocationParams,
    SemanticChange,
)

DEFAULT_BATCH_SIZE = 100

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _get_dataset_ids_attr(params: Dict) -> str:
    return params.get("dataset_ids_attr", "owned_dataset_ids")


def owned_datasets(
    semantic_changes: List[SemanticChange],
    action_context: ActionContext,
    invocation_params: InvocationParams,
    params: Dict,
) -> List[SemanticChange]:
    result: List[SemanticChange] = []
    for semantic_change in semantic_changes:
        if semantic_change.entity_type == EntityType.USER.name:
            for_user = owned_datasets_for_user(semantic_change, action_context, params)
            result.append(for_user)

    return result


def owned_datasets_for_user(
    semantic_change: SemanticChange,
    action_context: ActionContext,
    params: dict,
) -> SemanticChange:
    user_id = semantic_change.entity_id
    start = 0
    batch_size = DEFAULT_BATCH_SIZE

    owned_datasets = []
    while True:
        sanitized_userid = sanitize_user_urn_for_search(user_id)
        result = action_context.graph.get_by_query(
            query=f"owners:{sanitized_userid}",
            entity=EntityType.DATASET.str_rep,
            start=start,
            count=batch_size,
        )
        if len(result) == 0:
            break
        for item in result:
            owned_datasets.append(item["entity"])
        start += batch_size

    dataset_ids_attr = _get_dataset_ids_attr(params)
    if len(owned_datasets) > 0:
        semantic_change.attrs[dataset_ids_attr] = owned_datasets
    return semantic_change
