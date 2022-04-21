# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import re
from typing import Any, Dict, List, Tuple, Union

from dictdiffer import diff

from datahub_actions.api.action_core import EntityType, RawMetadataChange
from datahub_actions.utils.delta_extractor_mcl import get_helper_for_asepct

logger = logging.getLogger(__name__)


class MetadataChangeProcessor:
    def __init__(self) -> None:
        pass

    @staticmethod
    def get_changes(result):
        added = []
        removed = []
        changed = []
        for i in result:
            if i[0] == "add":
                added.append((i[1], i[2]))
            elif i[0] == "remove":
                removed.append((i[1], i[2]))
            elif i[0] == "change":
                changed.append((i[1], i[2]))
        return added, removed, changed

    @staticmethod
    def tuples_to_dict(input: List[Tuple[str, Dict]]) -> Dict[str, Dict]:
        output = {}
        for key, value in input:
            output[key] = value
        return output

    @staticmethod
    def calculate_delta_from_mcl(change_event: Dict[str, Any]) -> RawMetadataChange:
        entity_urn = change_event.get("entityUrn")
        if entity_urn is None:
            entity_urn = ""
        entity_type_str = str(change_event.get("entityType"))
        entity_type = EntityType.from_string(entity_type_str)

        aspect_name = str(change_event.get("aspectName"))
        helper_method = get_helper_for_asepct(aspect_name)
        added: List[Dict] = []
        removed: List[Dict] = []
        changed: List[Dict] = []
        if helper_method is None:
            logger.warning(
                f"Delta extraction for aspect {aspect_name} not implemented change_event={change_event}"
            )
        else:
            added, removed, changed = helper_method(
                change_event.get("aspect"), change_event.get("previousAspectValue")  # type: ignore
            )

        return RawMetadataChange(
            entity_urn=entity_urn,
            entity_type=entity_type,
            added=added,
            removed=removed,
            changed=changed,
            original_event=change_event,
            aspect_name=aspect_name,
        )

    @staticmethod
    def calculate_delta(change_event: Any) -> RawMetadataChange:

        entityUrn = change_event["newSnapshot"][1]["urn"]
        urn_match = re.search(r"^urn:li:([a-zA-Z]+):", entityUrn)
        assert urn_match, "must find an urn"
        entity_type_str = urn_match.group(1)
        entity_type = EntityType.from_string(entity_type_str)
        if change_event["oldSnapshot"] is None:
            # everything is added!
            old_aspects: Dict = {}
            new_aspects = MetadataChangeProcessor.tuples_to_dict(
                change_event["newSnapshot"][1]["aspects"]
            )
            result = list(diff(old_aspects, new_aspects))
            added, removed, changed = MetadataChangeProcessor.get_changes(result)

            return RawMetadataChange(
                entity_urn=entityUrn,
                entity_type=entity_type,
                added=added,
                removed=removed,
                changed=changed,
                original_event=change_event,
            )
        else:
            old_aspects = MetadataChangeProcessor.tuples_to_dict(
                change_event["oldSnapshot"][1]["aspects"]
            )
            new_aspects = MetadataChangeProcessor.tuples_to_dict(
                change_event["newSnapshot"][1]["aspects"]
            )
            result = list(diff(old_aspects, new_aspects))
            added, removed, changed = MetadataChangeProcessor.get_changes(result)
            return RawMetadataChange(
                entity_urn=entityUrn,
                entity_type=entity_type,
                added=added,
                removed=removed,
                changed=changed,
                original_event=change_event,
            )

            breakpoint()
            print(f"{change_event}")


def apply_path_spec_to_delta(delta_node: Any, path_spec: List[str]) -> List[Any]:
    # first consume path spec that is common between delta_node and path_spec
    delta_path = delta_node[0]
    if delta_path != "" and delta_path != []:
        if isinstance(delta_path, str):
            # simple top-level path
            if path_spec[0] == delta_path:
                return apply_path_spec_to_delta(("", delta_node[1]), path_spec[1:])
            else:
                # failed match
                logger.debug(f"Match failed at {delta_node} for {path_spec}")
                return []
        elif isinstance(delta_path, list):
            if path_spec[0] == delta_path[0] or path_spec[0].startswith("*"):
                return apply_path_spec_to_delta(
                    (delta_path[1:], delta_node[1]), path_spec[1:]
                )
            else:
                # failed match
                logger.debug(f"Match failed at {delta_node} for {path_spec}")
                return []
        else:
            raise Exception("Unexpected code path")
    else:
        return apply_path_spec(delta_node[1], path_spec)


def apply_path_spec(node: Union[Dict, List], path_spec: List[str]) -> List[Any]:
    results: List[Any] = []
    if not node:
        return results
    if len(path_spec) == 1:
        # at this level we should either have a Dict or a list of tuples (k,v)
        if not isinstance(node, Dict):
            if isinstance(node, List):
                node = MetadataChangeProcessor.tuples_to_dict(node)
            else:
                # Optionals are represented as unions, which translate to Tuples
                # assert isinstance(node[0], str)
                assert len(node) == 2
                node = node[1]
        assert isinstance(node, Dict)
        val = node.get(path_spec[0])
        if val:
            return [val]
        else:
            return []
    else:
        if not path_spec[0].startswith("*"):
            if not isinstance(node, Dict):
                if isinstance(node, List):
                    node = MetadataChangeProcessor.tuples_to_dict(node)
                else:
                    # Optionals are represented as unions, which translate to Tuples
                    # assert isinstance(node[0], str)
                    assert len(node) == 2
                    node = node[1]
            assert isinstance(node, Dict)
            node = node.get(path_spec[0], {})
            return apply_path_spec(node, path_spec[1:])
        else:
            # breakpoint()
            assert isinstance(node, list)
            for n in node:
                results.extend(apply_path_spec(n, path_spec[1:]))
            return results
