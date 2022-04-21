from typing import Iterable, List, Optional

from datahub_actions.api.action_core import ActionContext, EntityType, SemanticChange

DEFAULT_BATCH_SIZE = 1


def get_datahub_users_list(
    action_context: ActionContext, params: Optional[dict]
) -> Iterable[List[SemanticChange]]:
    start = 0
    if params is not None:
        batch_size = params.get("batch_size", DEFAULT_BATCH_SIZE)
    else:
        batch_size = DEFAULT_BATCH_SIZE

    while True:
        result = action_context.graph.get_by_query(
            query="*", entity=EntityType.USER.str_rep, start=start, count=batch_size
        )
        if len(result) == 0:
            break

        yield [
            SemanticChange(
                change_type="UserDetailEvent",
                entity_type=EntityType.USER.name,
                entity_id=entity["entity"],
                entity_key={},
                attrs={},
            )
            for entity in result
        ]
        start += batch_size
