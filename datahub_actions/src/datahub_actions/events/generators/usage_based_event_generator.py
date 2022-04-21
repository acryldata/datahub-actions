import logging
from dataclasses import dataclass
from heapq import heappush, heappushpop
from typing import Callable, Iterable, List, Optional

from datahub.metadata.schema_classes import DatasetKeyClass

from datahub_actions.api.action_core import ActionContext, EntityType, SemanticChange

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_PLATFORMS = ["snowflake"]
DEFAULT_TOP_N = 10
BATCH_SIZE = 1000


def low_usage_entities_event_generator(
    action_context: ActionContext, params: Optional[dict]
) -> Iterable[List[SemanticChange]]:
    result = fetch_top_entities_based_on_usage(
        action_context, params, lambda entity: -entity.usage_count
    )
    yield [
        SemanticChange(
            change_type="LowUsageEvent",
            entity_type=EntityType.DATASET.name,
            entity_id=entity.urn,
            entity_key={},
            attrs={**entity.attr, "count": entity.usage_count},
        )
        for entity in result
    ]


def high_usage_entities_event_generator(
    action_context: ActionContext, params: Optional[dict]
) -> Iterable[List[SemanticChange]]:
    result = fetch_top_entities_based_on_usage(
        action_context, params, lambda entity: entity.usage_count
    )
    yield [
        SemanticChange(
            change_type="HighUsageEvent",
            entity_type=EntityType.DATASET.name,
            entity_id=entity.urn,
            entity_key={},
            attrs={**entity.attr, "count": entity.usage_count},
        )
        for entity in result
    ]


@dataclass
class HydratedEntity:
    urn: str
    usage_count: int
    attr: dict


def fetch_top_entities_based_on_usage(
    action_context: ActionContext,
    params: Optional[dict],
    scorer: Callable[[HydratedEntity], float],
) -> List[HydratedEntity]:
    if not params:
        params = {}
    platforms = params.get("platforms", DEFAULT_PLATFORMS)
    top_n = params.get("top_n", DEFAULT_TOP_N)
    top_entities: List = []
    start = 0
    while not top_entities:
        logger.info(f"Querying from {start} to {start + BATCH_SIZE}")
        urns = get_datasets_with_platform(action_context, platforms, start, BATCH_SIZE)
        logger.info(f"Getting usage stats for {len(urns)} entities")
        hydrated_entities = fetch_usage_stats(action_context, urns)
        for entity in hydrated_entities:
            scored_entity = (scorer(entity), entity.urn, entity)
            if len(top_entities) < top_n:
                heappush(top_entities, scored_entity)
            else:
                heappushpop(top_entities, scored_entity)
        if len(urns) < BATCH_SIZE:
            break
        start += BATCH_SIZE
    top_entities.sort(reverse=True)
    logger.info(f"Received usage stats for {len(top_entities)} entities")
    return [
        HydratedEntity(
            urn=hydrated_entity.urn,
            usage_count=hydrated_entity.usage_count,
            attr={"name": get_name(action_context, hydrated_entity.urn)},
        )
        for (_, _, hydrated_entity) in top_entities
    ]


def get_name(action_context: ActionContext, urn: str) -> str:
    datasetKey = action_context.graph.graph.get_aspect(
        urn,
        "datasetKey",
        aspect_type_name="com.linkedin.metadata.key.DatasetKey",
        aspect_type=DatasetKeyClass,
    )
    if datasetKey:
        return datasetKey.name
    return ""


def get_datasets_with_platform(
    action_context: ActionContext, platforms: List[str], start: int, end: int
) -> List[str]:
    platform_filters = {
        "or": [
            {"and": [{"field": "platform", "value": platform}]}
            for platform in platforms
        ]
    }
    results = action_context.graph.get_by_query(
        "*", EntityType.DATASET.str_rep, start, end, platform_filters
    )
    return [result["entity"] for result in results]


def fetch_usage_stats(
    action_context: ActionContext, urns: List[str]
) -> List[HydratedEntity]:
    request = {
        "entityName": EntityType.DATASET.str_rep,
        "aspectName": "datasetUsageStatistics",
        "metrics": [{"fieldPath": "totalSqlQueries", "aggregationType": "SUM"}],
        "buckets": [{"type": "STRING_GROUPING_BUCKET", "key": "urn"}],
    }
    filters = {"or": [{"and": [{"field": "urn", "value": urn}]} for urn in urns]}
    request["filter"] = filters
    end_point = f"{action_context.graph.graph.config.server}/analytics?action=getTimeseriesStats"
    resp = action_context.graph.graph._post_generic(end_point, request)
    usage_stats = {row[0]: row[1] for row in resp["value"]["table"]["rows"]}
    return [
        HydratedEntity(urn=urn, usage_count=int(usage_stats.get(urn, 0)), attr={})
        for urn in urns
    ]
