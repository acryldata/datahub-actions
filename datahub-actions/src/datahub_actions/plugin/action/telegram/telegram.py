import json
import logging

import telebot
from datahub.configuration.common import ConfigModel
from datahub.metadata.schema_classes import \
    EntityChangeEventClass as EntityChangeEvent
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.utils.datahub_util import DATAHUB_SYSTEM_ACTOR_URN
from datahub_actions.utils.social_util import (
    StructuredMessage, get_message_from_entity_change_event,
    get_telegram_welcome_message, pretty_any_text)
from pydantic import SecretStr
from ratelimit import limits, sleep_and_retry

logger = logging.getLogger(__name__)


@sleep_and_retry
@limits(calls=1, period=1)
def post_message(client, chat_id, text):
    client.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML"
    )


class TelegramNotificationConfig(ConfigModel):
    bot_token: SecretStr
    chat_id: SecretStr
    base_url: str = "http://127.0.0.1:9002/"
    suppress_system_activity: bool = True


class TelegramNotificationAction(Action):
    def name(self):
        return "SlackNotificationAction"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = TelegramNotificationConfig.parse_obj(config_dict or {})
        logger.info(f"Telegram notification action configured with {action_config}")
        return cls(action_config, ctx)

    def __init__(self, action_config: TelegramNotificationConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.action_config = action_config
        self.bot = telebot.TeleBot(self.action_config.bot_token.get_secret_value())

        self.bot.send_message(
            chat_id=self.action_config.chat_id.get_secret_value(),
            text=get_telegram_welcome_message(self.action_config.base_url),
            parse_mode="HTML"
        )

    def act(self, event: EventEnvelope) -> None:
        try:
            message = json.dumps(
                json.loads(event.as_json()),
                indent=4
            )
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
                    channel="telegram"
                )

                if semantic_message:
                    post_message(
                        client=self.bot,
                        chat_id=self.action_config.chat_id.get_secret_value(),
                        text=semantic_message
                    )
            else:
                logger.debug("Skipping message because it didn't match out filter")
        except Exception as e:
            logger.debug(f"Failed to process event", e)

    def close(self) -> None:
        pass