import logging
from typing import Any, Dict, List, Optional, Set

from datahub_actions.api.action_core import (
    MclChangeType,
    RawMetadataChange,
    SemanticChange,
    Subscription,
)
from datahub_actions.events.add_owner_event import AddOwner, AddOwnerEvent
from datahub_actions.events.add_tag_event import AddTag, AddTagEvent
from datahub_actions.events.add_term_event import AddTerm, AddTermEvent
from datahub_actions.events.datahub_execution_request_result_event import (
    DataHubExecutionRequestResult,
    DataHubExecutionRequestResultEvent,
)
from datahub_actions.events.remove_owner_event import RemoveOwner, RemoveOwnerEvent
from datahub_actions.utils.delta_extractor_mcl import get_aspect_val_as_json

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# To add a new event need to do the following
#   - Add Helper class
#   - Ensure Event Name is in the subscription
#   - Add mapping here
#   - Add extractor in delta_extractor_mcl file and
#       add mapping in RECOGNIZED_ASPECT_TO_EXTRACT_DELTA
#   - Add unit tests in the corresponding files
RECOGNIZED_EVENTS_TO_CHANGE_MAPPING: Dict[str, Dict[str, Any]] = {
    AddOwnerEvent.__name__: {
        "helper_class": AddOwner,
        "change_type": MclChangeType.ADDED.name,
    },
    RemoveOwnerEvent.__name__: {
        "helper_class": RemoveOwner,
        "change_type": MclChangeType.REMOVED.name,
    },
    AddTagEvent.__name__: {
        "helper_class": AddTag,
        "change_type": MclChangeType.ADDED.name,
    },
    AddTermEvent.__name__: {
        "helper_class": AddTerm,
        "change_type": MclChangeType.ADDED.name,
    },
    DataHubExecutionRequestResultEvent.__name__: {
        "helper_class": DataHubExecutionRequestResult,
        "change_type": MclChangeType.CHANGED.name,
    },
}


def get_event_map_for_event(event_name: str) -> Any:
    return RECOGNIZED_EVENTS_TO_CHANGE_MAPPING[event_name]


def get_aspect_names_for_event(event_name: str) -> List[str]:
    return RECOGNIZED_EVENTS_TO_CHANGE_MAPPING[event_name][
        "helper_class"
    ].get_aspect_names()


def get_change_type_for_event(event_name: str) -> str:
    return RECOGNIZED_EVENTS_TO_CHANGE_MAPPING[event_name]["change_type"]


def get_helper_for_event(event_name: str) -> Any:
    return get_event_map_for_event(event_name)["helper_class"]


def get_subscriptions_for_event(event_name: Optional[str]) -> List[Subscription]:
    if event_name is None:
        return []
    return get_helper_for_event(event_name).get_subscriptions()


def get_recognized_aspects() -> Set[str]:
    result = set()
    for event_name in RECOGNIZED_EVENTS_TO_CHANGE_MAPPING.keys():
        aspect_names = get_aspect_names_for_event(event_name)
        if aspect_names is None:
            continue
        for aspect_name in aspect_names:
            if aspect_name is not None:
                result.add(aspect_name)
    return result


def get_recognized_events() -> Set[str]:
    return set(RECOGNIZED_EVENTS_TO_CHANGE_MAPPING.keys())


def diff_is_of_event(list_of_diff: List, event_name: str) -> bool:
    urn_key = get_helper_for_event(event_name).get_urn_key()
    if urn_key == "":
        return True
    diff_of_event = list(filter(lambda x: urn_key in x, list_of_diff))
    return len(diff_of_event) > 0


def get_events_to_aspect_mapping(
    raw_change: RawMetadataChange,
) -> Dict[str, List[Dict]]:
    aspect_name = raw_change.aspect_name
    if aspect_name is None:
        return {}
    if aspect_name not in get_recognized_aspects():
        return {}

    result = {}
    for event_name in sorted(list(get_recognized_events())):

        aspect_names_val = get_aspect_names_for_event(event_name)
        if aspect_names_val is not None and aspect_name not in aspect_names_val:
            continue

        change_type = get_change_type_for_event(event_name)
        if change_type == MclChangeType.ADDED.name and diff_is_of_event(
            raw_change.added, event_name
        ):
            result[event_name] = raw_change.added
        elif change_type == MclChangeType.REMOVED.name and diff_is_of_event(
            raw_change.removed, event_name
        ):
            result[event_name] = raw_change.removed
        elif change_type == MclChangeType.CHANGED.name and diff_is_of_event(
            raw_change.changed, event_name
        ):
            result[event_name] = raw_change.changed

    return result


def get_subscriptions_to_semantic_events(
    raw_change: RawMetadataChange, subscriptions: List[Subscription]
) -> Dict[Subscription, List[SemanticChange]]:
    events_to_aspects_mapping = get_events_to_aspect_mapping(raw_change)
    result: Dict[Subscription, List[SemanticChange]] = {}

    entity_key_aspect = get_aspect_val_as_json(
        raw_change.original_event.get("entityKeyAspect", None)
    )
    if entity_key_aspect is None:
        entity_key_aspect = {}

    for subscription in subscriptions:
        if subscription is None or subscription.event_name is None:
            continue
        aspects_list = events_to_aspects_mapping.get(subscription.event_name)
        if aspects_list is None:
            continue
        if subscription not in result:
            result[subscription] = []
        semantic_change_list = result.get(subscription, [])
        for aspect_dict in aspects_list:
            semantic_change_list.append(
                SemanticChange(
                    change_type=subscription.event_name,
                    entity_type=raw_change.entity_type.name,
                    entity_id=raw_change.entity_urn,
                    entity_key=entity_key_aspect,
                    attrs=aspect_dict,
                )
            )
    return result
