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
from typing import Callable, Iterable, List, Optional, Set

from datahub_actions.api.action_core import ActionContext, SemanticChange
from datahub_actions.events.generators.datahub_user_list_generator import (
    get_datahub_users_list,
)
from datahub_actions.events.generators.usage_based_event_generator import (
    high_usage_entities_event_generator,
    low_usage_entities_event_generator,
)

logger = logging.getLogger(__name__)


EVENT_GENERATOR_REGISTRY = {
    "datahub_user_list_generator": get_datahub_users_list,
    "low_usage_entities_event_generator": low_usage_entities_event_generator,
    "high_usage_entities_event_generator": high_usage_entities_event_generator,
}


def get_all_registered_event_generators() -> Set[str]:
    return set(EVENT_GENERATOR_REGISTRY.keys())


def get_event_generator_for_key(
    genertor_name: str,
) -> Optional[
    Callable[[ActionContext, Optional[dict]], Iterable[List[SemanticChange]]]
]:
    return EVENT_GENERATOR_REGISTRY.get(genertor_name)
