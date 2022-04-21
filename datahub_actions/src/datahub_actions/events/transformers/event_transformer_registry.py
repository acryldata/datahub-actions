from typing import Callable, List, Optional

from datahub_actions.api.action_core import (
    ActionContext,
    InvocationParams,
    SemanticChange,
)
from datahub_actions.events.transformers.filter_attr_by_time import filter_attr_by_time
from datahub_actions.events.transformers.filter_by_attr_value import (
    filter_by_attr_value,
)
from datahub_actions.events.transformers.filter_by_missing_attrs import (
    filter_by_missing_attrs,
)
from datahub_actions.events.transformers.filter_by_zero_len_attrs import (
    filter_by_zero_len_attrs,
)
from datahub_actions.events.transformers.flatten_attr_map import flatten_attr_map
from datahub_actions.events.transformers.get_datasets_failing_constraint import (
    datasets_failing_constraint,
)
from datahub_actions.events.transformers.get_ingestion_execution_result_details import (
    ingestion_execution_result_details,
)
from datahub_actions.events.transformers.get_owned_datasets import owned_datasets
from datahub_actions.events.transformers.get_proposals import proposals
from datahub_actions.events.transformers.get_user_details import user_details
from datahub_actions.events.transformers.peek import peek
from datahub_actions.events.transformers.register_template import register_template

REGISTRY = {
    "user_details": user_details,
    "ingestion_execution_result_details": ingestion_execution_result_details,
    "flatten_attr_map": flatten_attr_map,
    "filter_attr_by_time": filter_attr_by_time,
    "proposals": proposals,
    "owned_datasets": owned_datasets,
    "datasets_failing_constraint": datasets_failing_constraint,
    "filter_by_attr_value": filter_by_attr_value,
    "filter_by_missing_attrs": filter_by_missing_attrs,
    "filter_by_zero_len_attrs": filter_by_zero_len_attrs,
    "filter_by_time": filter_attr_by_time,
    "register_template": register_template,
    "peek": peek,
}


def get_transformer_for_key(
    transformer_key: str,
) -> Optional[
    Callable[
        [List[SemanticChange], ActionContext, InvocationParams, dict],
        List[SemanticChange],
    ]
]:
    return REGISTRY.get(transformer_key)
