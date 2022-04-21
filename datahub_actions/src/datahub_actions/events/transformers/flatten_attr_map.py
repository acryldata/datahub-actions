from typing import List

from datahub_actions.actions.utils.collection_util import flatten_dict
from datahub_actions.api.action_core import (
    ActionContext,
    InvocationParams,
    SemanticChange,
)


def flatten_attr_map(
    semantic_changes: List[SemanticChange],
    action_context: ActionContext,
    invocation_params: InvocationParams,
    params: dict,
) -> List[SemanticChange]:
    flatten_attr = params.get("flatten_attr")
    flatten_attr_to = str(params.get("flatten_attr_to"))
    keep_attr = params.get("keep_attr", True)
    for semantic_change in semantic_changes:
        flatten_attr_val = semantic_change.attrs[flatten_attr]
        if type(flatten_attr_val) == list:
            semantic_change.attrs[flatten_attr_to] = [
                flatten_dict(elem) for elem in flatten_attr_val
            ]
        else:
            semantic_change.attrs[flatten_attr_to] = flatten_dict(flatten_attr_val)

        if flatten_attr != flatten_attr_to and (not keep_attr):
            del semantic_change.attrs[flatten_attr]

    return semantic_changes
