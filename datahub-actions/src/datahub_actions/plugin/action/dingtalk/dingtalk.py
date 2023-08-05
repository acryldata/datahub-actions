import json
import logging

import requests
from datahub.configuration.common import ConfigModel
from datahub.metadata.schema_classes import EntityChangeEventClass as EntityChangeEvent
from pydantic.types import SecretStr
from ratelimit import limits, sleep_and_retry

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.utils.datahub_util import DATAHUB_SYSTEM_ACTOR_URN
from datahub_actions.utils.social_util import (
    get_message_from_entity_change_event,
    get_welcome_message,
)

logger = logging.getLogger(__name__)


@sleep_and_retry
@limits(calls=1, period=1)  # 1 call per second
def post_message(webhook_url, content):
    headers = {"Content-Type": "application/json"}
    data = {
        "msgtype": "text",
        "text": {
            "content": DingtalkNotificationConfig.keyword.get_secret_value() + content
        },
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        logger.info("Message send successfully")
    else:
        logger.info("Message send failed")


class DingtalkNotificationConfig(ConfigModel):
    webhook_url: SecretStr
    keyword: SecretStr
    default_channel: str
    base_url: str = "http://localhost:9002/"
    suppress_system_activity: bool = True


class DingtalkNotification(Action):
    def name(self):
        return "DingtalkNotification"

    def close(self) -> None:
        pass

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = DingtalkNotificationConfig.parse_obj(config_dict or {})
        logger.info(f"Dingtalk notification action configured with {action_config}")
        return cls(action_config, ctx)

    def __init__(self, action_config: DingtalkNotificationConfig, ctx: PipelineContext):
        self.action_config = action_config
        self.ctx = ctx
        post_message(
            self.action_config.webhook_url.get_secret_value(),
            get_welcome_message(self.action_config.base_url).text,
        )

    def act(self, event: EventEnvelope) -> None:
        try:
            message = json.dumps(json.loads(event.as_json()), indent=4)
            logger.debug(f"Received event: {message}")
            if event.event_type == "EntityChangeEvent_v1":
                assert isinstance(event.event, EntityChangeEvent)
                if (
                    event.event.auditStamp.actor == DATAHUB_SYSTEM_ACTOR_URN
                    and self.action_config.suppress_system_activity
                ):
                    return None

                semantic_message = get_message_from_entity_change_event(
                    event.event,
                    self.action_config.base_url,
                    self.ctx.graph.graph if self.ctx.graph else None,
                    channel="dingtalk",
                )
                post_message(
                    self.action_config.webhook_url.get_secret_value(), semantic_message
                )
            else:
                logger.debug("Skipping message because it didn't match our filter")
        except Exception as e:
            logger.debug("Failed to process event", e)
