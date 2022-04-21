import logging
from typing import List

from datahub_actions.api.action_core import (
    ActionContext,
    InvocationParams,
    SemanticChange,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def peek(
    semantic_changes: List[SemanticChange],
    action_context: ActionContext,
    invocation_params: InvocationParams,
    params: dict,
) -> List[SemanticChange]:

    for semantic_change in semantic_changes:
        logger.info("")
        logger.info(f"Semantic change is {semantic_change}")

    return semantic_changes
