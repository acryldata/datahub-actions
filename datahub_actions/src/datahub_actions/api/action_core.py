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

import datetime
import logging
import uuid
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional

import pydantic
import pytz
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from pydantic import BaseModel

# TODO : Move to an import of an interface eventually
from datahub_actions.api.action_graph import AcrylDataHubGraph

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    UPDATE = 1
    UPSERT = 2


class MclChangeType(Enum):
    # TODO We should have a single ChangeType enum.
    # But not enough unit tests so didn't want to break anything
    ADDED = auto()
    REMOVED = auto()
    CHANGED = auto()


@dataclass
class Subscription:
    """Represents a subscription to a metadata change"""

    change_type: Optional[ChangeType] = None
    path_spec: Optional[str] = None
    event_name: Optional[str] = None

    @pydantic.root_validator()
    def on_event_is_valid(cls, values):
        change_type = values.get("change_type")
        path_spec = values.get("path_spec")
        event_name = values.get("event_name")

        # TODO Validator is not working
        if change_type is None:
            if path_spec is None:
                if event_name is None:
                    raise ValueError("All 3 cannot be None")
            else:
                raise ValueError(
                    "Either both change_type and path_spec should be present or neither"
                )

        return values

    def __hash__(self):
        return hash((self.change_type, self.path_spec, self.event_name))


class EventTransformer(BaseModel):
    name: str
    params: Optional[Dict]


class ScheduleConfig(BaseModel):
    hour: int = 8
    seconds_since_last_ran: int = 24 * 60 * 60
    timezone: str = "UTC"
    event_generator: str
    event_generator_params: Optional[Dict]
    last_ran: Optional[datetime.datetime]

    @pydantic.validator("hour", always=True)
    def system_is_valid(cls, v, values, **kwargs):
        if 0 > v or v > 23:
            raise ValueError("Only values between 0 and 24 are supported")
        return v

    @pydantic.validator("timezone", always=True)
    def timezone_is_valid(cls, v, values, **kwargs):
        try:
            pytz.timezone(v)
        except Exception:
            raise ValueError(
                f"Invalid timezone {v}. Valid values are {pytz.all_timezones}"
            )
        return v


@dataclass
class ActionContext:
    graph: AcrylDataHubGraph


@dataclass
class TemplateConfig:
    tabulated: bool
    url: bool
    attr_name: str
    func: Optional[Callable[[Any, Dict], str]] = None
    func_params: Optional[Dict] = None


class InvocationParams:
    invocation_id: str = uuid.uuid4().hex
    template_registry: Dict = dict()

    def register_template(self, template: str, template_config: TemplateConfig) -> Any:
        self.template_registry[template] = template_config

    def get_all_templates(self):
        for template, template_entry in self.template_registry.items():
            yield (template, template_entry)


class DataHubEvent(Dict):
    pass


class EntityType(Enum):
    DATASET = "dataset"
    DATAHUB_EXECUTION_REQUEST = "dataHubExecutionRequest"
    FIELD = "field"
    DASHBOARD = "dashboard"
    CHART = "chart"
    MODEL = "model"
    FEATURE = "feature"
    TAG = "tag"
    USER = "corpuser"
    UNKNOWN = "UNKNOWN_STRING"

    def __init__(self, str_rep: str):
        self.str_rep = str_rep

    @staticmethod
    def from_string(name: str) -> "EntityType":
        for enum_val in EntityType:
            if name == enum_val.str_rep:
                return enum_val
        return EntityType.UNKNOWN


@dataclass
class RawMetadataChange:
    entity_urn: str
    entity_type: EntityType
    added: List
    removed: List
    changed: List
    original_event: Any
    aspect_name: Optional[str] = None


@dataclass
class SemanticChange:
    change_type: str
    entity_type: str
    entity_id: str
    entity_key: dict
    attrs: dict


class DataHubAction(metaclass=ABCMeta):
    """The base class for all Actions"""

    @abstractmethod
    def name(self) -> str:
        """All actions must have a name"""
        pass

    @classmethod
    @abstractmethod
    def create(cls, config_dict: dict, ctx: ActionContext) -> "DataHubAction":
        """Factory method to create an instance of an Action"""
        pass

    @abstractmethod
    def subscriptions(self) -> List[Subscription]:
        """Return a list of Subscriptions that the Action wants the server to watch on its behalf"""
        pass

    def subscription_to_all(self) -> bool:
        """This overrides subscriptions method"""
        return False

    def schedule_configs(self) -> List[ScheduleConfig]:
        """Returns schedule configs for Action"""
        return []

    @abstractmethod
    def act(
        self,
        match: Optional[Subscription],
        raw_change: RawMetadataChange,
        semantic_changes: Optional[List[SemanticChange]],
    ) -> List[MetadataChangeProposalWrapper]:
        """The main eval method that the framework will call on the Action. Returning from this method implies that the action was successfully processed by the function"""
        pass

    def handle_scheduled_events(self, schedule_config: ScheduleConfig) -> Any:
        pass
