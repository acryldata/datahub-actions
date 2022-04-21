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


def _get_proposal_attr(params: Dict) -> str:
    return params.get("proposal_attr", "pending_proposals")


def get_proposals_for_user(
    semantic_change: SemanticChange,
    action_context: ActionContext,
    pending: bool,
) -> List:
    proposals: List = []

    start = 0
    batch_size = 10
    while True:
        result = action_context.graph.query_proposals_for_user(
            semantic_change.entity_id, start=start, count=batch_size, pending=pending
        )
        proposals.extend(result)
        if len(result) == 0:
            break
        start += batch_size

    return proposals


def proposals(
    semantic_changes: List[SemanticChange],
    action_context: ActionContext,
    invocation_params: InvocationParams,
    params: Dict,
) -> List[SemanticChange]:
    pending = params.get("pending", True)
    pending_proposal_attr = _get_proposal_attr(params)
    for semantic_change in semantic_changes:
        proposals = []
        if semantic_change.entity_type == EntityType.USER.name:
            proposals = get_proposals_for_user(semantic_change, action_context, pending)
            semantic_change.attrs[pending_proposal_attr] = proposals

    return semantic_changes
