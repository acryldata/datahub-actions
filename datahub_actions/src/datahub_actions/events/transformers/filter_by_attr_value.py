from typing import List

from datahub_actions.api.action_core import (
    ActionContext,
    InvocationParams,
    SemanticChange,
)


def filter_by_attr_value(
    semantic_changes: List[SemanticChange],
    action_context: ActionContext,
    invocation_params: InvocationParams,
    params: dict,
) -> List[SemanticChange]:

    attr_name = str(params.get("attr_name"))
    attr_val_to_keep = str(params.get("attr_val_to_keep"))

    result: List[SemanticChange] = []
    for semantic_change in semantic_changes:
        attrs = semantic_change.attrs
        attr_val = attrs.get(attr_name, None)
        if attr_val == attr_val_to_keep:
            result.append(semantic_change)

    return result
