import logging
from typing import List

from datahub_actions.api.action_core import (
    ActionContext,
    InvocationParams,
    SemanticChange,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_FILTERING = "any_missing"


def filter_by_zero_len_attrs(
    semantic_changes: List[SemanticChange],
    action_context: ActionContext,
    invocation_params: InvocationParams,
    params: dict,
) -> List[SemanticChange]:

    filter_on: str = params.get("filter_on", DEFAULT_FILTERING)
    zero_length_attrs: List[str] = params.get("zero_length_attrs", [])

    filter_on_any = filter_on == DEFAULT_FILTERING

    result: List[SemanticChange] = []
    for semantic_change in semantic_changes:
        missing = []
        for zero_length_attr in zero_length_attrs:
            if (
                zero_length_attr in semantic_change.attrs
                and len(semantic_change.attrs[zero_length_attr]) == 0
            ):
                if filter_on_any:
                    logger.info(f"Filtered {semantic_change}")
                    break
                else:
                    missing.append(True)
        else:
            if len(missing) == len(zero_length_attrs):
                logger.info(f"Filtered {semantic_change}")
                continue
            result.append(semantic_change)
    return result
