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

from pydantic import BaseModel

from datahub_actions.action.action import Action
from datahub_actions.event.event import EventEnvelope
from datahub_actions.pipeline.context import ActionContext

logger = logging.getLogger(__name__)


class HelloWorldConfig(BaseModel):
    # Whether to print the message in upper case.
    to_upper: bool


# A basic example of a DataHub action that prints all
# events received to the console.
class HelloWorldAction(Action):
    @classmethod
    def create(cls, config_dict: dict, ctx: ActionContext) -> "Action":
        action_config = HelloWorldConfig.parse_obj(config_dict or {})
        return cls(action_config, ctx)

    def __init__(self, config: HelloWorldConfig, ctx: ActionContext):
        self.config = config

    def act(self, event: EventEnvelope) -> None:
        message = f"Hello world! Received event {event}\n"
        if self.config.to_upper:
            print(message.upper())
        else:
            print(message)

    def close(self) -> None:
        pass
