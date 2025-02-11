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
from typing import Any, Callable


from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent

logger = logging.getLogger(__name__)

class MCEProcessor:
    """
    A utility class to register and process MetadataChangeLog events.
    """
    EntityChangeEvent_v1:str = "EntityChangeEvent_v1"

    def __init__(self) -> None:
        self.entity_aspect_processors: dict[str, dict[str, Callable]] = {}
        pass

    def is_mce(self, event: EventEnvelope) -> bool:
        return event.event_type == MCEProcessor.EntityChangeEvent_v1

    def register_processor(
        self, entity_type: str, category: str, processor: Callable
    ) -> None:
        if entity_type not in self.entity_aspect_processors:
            self.entity_aspect_processors[entity_type] = {}
        self.entity_aspect_processors[entity_type][category] = processor

    def process(self, event: EventEnvelope) -> Any:
        if isinstance(event.event, EntityChangeEvent):
            semantic_event = event.event
            entity_type = semantic_event.entityType
            category = semantic_event.category

            if (
                    entity_type in self.entity_aspect_processors
                    and category in self.entity_aspect_processors[entity_type]
            ):
                logger.info(f"Processing MetadataChangeLogClass with processors with entity type {entity_type} category {category}")
                return self.entity_aspect_processors[entity_type][category](
                    event=event
                )

        return None