from typing import List

from datahub_actions.api.action_core import (
    ActionContext,
    InvocationParams,
    SemanticChange,
)


def filter_by_missing_attrs(
    semantic_changes: List[SemanticChange],
    action_context: ActionContext,
    invocation_params: InvocationParams,
    params: dict,
) -> List[SemanticChange]:

    missing_key: List[str] = params.get("missing_key", [])

    result: List[SemanticChange] = []
    for semantic_change in semantic_changes:
        for key_attr in missing_key:
            if key_attr not in semantic_change.attrs:
                break
        else:
            result.append(semantic_change)

    return result
