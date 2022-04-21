import logging
from typing import Dict, List

from datahub_actions.api.action_core import (
    ActionContext,
    InvocationParams,
    SemanticChange,
)
from datahub_actions.events.transformers.get_owned_datasets import owned_datasets

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def datasets_failing_constraint(
    semantic_changes: List[SemanticChange],
    action_context: ActionContext,
    invocation_params: InvocationParams,
    params: Dict,
) -> List[SemanticChange]:
    datasets_fail_constraint_attr = params.get(
        "new_attr", "datasets_failing_constraint"
    )

    dataset_fail_cache: Dict[str, bool] = dict()

    params["dataset_ids_attr"] = "dataset_ids"
    semantic_changes = owned_datasets(
        semantic_changes, action_context, invocation_params, params
    )
    for semantic_change in semantic_changes:
        datasets = semantic_change.attrs.get("dataset_ids", [])
        datasets_failing_constraints = []
        for dataset in datasets:
            if dataset not in dataset_fail_cache:
                constraints = action_context.graph.query_constraints_for_dataset(
                    dataset
                )

                dataset_fail_cache[dataset] = len(constraints) > 0

            if dataset_fail_cache[dataset]:
                datasets_failing_constraints.append(dataset)
        semantic_change.attrs[
            datasets_fail_constraint_attr
        ] = datasets_failing_constraints
    return semantic_changes
