from typing import Dict, List

from datahub_actions.actions.notification.template.registry import (
    register_template_from_dict,
)
from datahub_actions.api.action_core import (
    ActionContext,
    InvocationParams,
    SemanticChange,
)


def register_template(
    semantic_changes: List[SemanticChange],
    action_context: ActionContext,
    invocation_params: InvocationParams,
    params: Dict,
) -> List[SemanticChange]:
    register_template_from_dict(invocation_params, params)
    return semantic_changes
