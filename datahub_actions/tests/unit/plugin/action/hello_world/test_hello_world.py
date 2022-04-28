import json
import pytest

from pydantic import ValidationError
from datahub.metadata.schema_classes import DictWrapper

from datahub_actions.plugin.action.hello_world.hello_world import (
    HelloWorldAction,
    HelloWorldConfig,
)
from tests.unit.test_helpers import (
    metadata_change_log_event_env,
    entity_change_event_env,
    pipeline_context
)


def test_create():
    # Create with no config
    HelloWorldAction.create({}, pipeline_context)

    # Create with to_upper config
    HelloWorldAction.create({ "to_upper": True }, pipeline_context)
    HelloWorldAction.create({ "to_upper": True }, pipeline_context)

    # Create with unknown config
    HelloWorldAction.create({ "to_lower": True }, pipeline_context)

    # Create with invalid type config
    with pytest.raises(ValidationError, match="to_upper"):
        HelloWorldAction.create({ "to_upper": "not" }, pipeline_context)


def test_act():
    # Simply verify that it works without exceptions.
    action = HelloWorldAction.create({}, pipeline_context)
    action.act(metadata_change_log_event_env)
    action.act(entity_change_event_env)

def test_close():
    # Nothing to Test
    pass
