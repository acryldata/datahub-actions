import logging
from datetime import datetime
from typing import Dict, List

from datahub_actions.api.action_core import (
    ActionContext,
    InvocationParams,
    SemanticChange,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def filter_attr_by_time(
    semantic_changes: List[SemanticChange],
    action_context: ActionContext,
    invocation_params: InvocationParams,
    params: Dict,
) -> List[SemanticChange]:
    attr = str(params.get("attr"))
    time_field = str(params.get("time_field"))
    new_attr = str(params.get("new_attr"))
    window_ms = int(params.get("window_seconds", 0)) * 1000
    window_filter_type = str(params.get("window_filter_type", "BEFORE_BEGIN"))
    time_since_epoch = int(datetime.now().timestamp()) * 1000

    for semantic_change in semantic_changes:
        result = []
        attr_val = semantic_change.attrs[attr]
        for elem in attr_val:
            time_attr_val = elem[time_field]
            if window_filter_type == "SINCE_BEGIN":
                filter_predicate = time_attr_val > time_since_epoch - window_ms
            else:
                filter_predicate = time_attr_val <= time_since_epoch - window_ms

            if filter_predicate:
                result.append(elem)
        semantic_change.attrs[new_attr] = result

    return semantic_changes
