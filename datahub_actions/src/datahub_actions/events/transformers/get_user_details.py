from typing import List

from datahub_actions.api.action_core import (
    ActionContext,
    EntityType,
    InvocationParams,
    SemanticChange,
)


def user_details(
    semantic_changes: List[SemanticChange],
    action_context: ActionContext,
    invocation_params: InvocationParams,
    params: dict,
) -> List[SemanticChange]:
    user_email_attr = params.get("user_email", "user_email")
    result = []
    for semantic_change in semantic_changes:
        if semantic_change.entity_type != EntityType.USER.name:
            continue
        corp_user_info = action_context.graph.get_corpuser_info(
            semantic_change.entity_id
        )

        if "email" in corp_user_info:
            semantic_change.attrs[user_email_attr] = corp_user_info["email"]
        result.append(semantic_change)
    return result
