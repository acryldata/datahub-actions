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

import pytest
from pydantic import ValidationError

from datahub_actions.plugin.action.dingtalk.dingtalk import DingtalkNotification
from tests.unit.test_helpers import (
    entity_change_event_env,
    metadata_change_log_event_env,
    pipeline_context,
)


def test_create():
    # Create with no config
    DingtalkNotification.create({}, pipeline_context)

    # Create with to_upper config
    DingtalkNotification.create({"to_upper": True}, pipeline_context)
    DingtalkNotification.create({"to_upper": True}, pipeline_context)

    # Create with unknown config
    DingtalkNotification.create({"to_lower": True}, pipeline_context)

    # Create with invalid type config
    with pytest.raises(ValidationError, match="to_upper"):
        DingtalkNotification.create({"to_upper": "not"}, pipeline_context)


def test_act():
    # Simply verify that it works without exceptions.
    action = DingtalkNotification.create({}, pipeline_context)
    action.act(metadata_change_log_event_env)
    action.act(entity_change_event_env)


def test_close():
    # Nothing to Test
    pass
