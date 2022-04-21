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
from dataclasses import dataclass
from typing import Dict, List, Optional

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from pydantic import SecretStr
from requests import sessions

from datahub_actions.api.action_core import (
    ActionContext,
    DataHubAction,
    SemanticChange,
    Subscription,
)
from datahub_actions.events.add_tag_event import AddTag, AddTagEvent
from datahub_actions.events.add_term_event import AddTerm, AddTermEvent
from datahub_actions.utils.change_processor import RawMetadataChange
from datahub_actions.utils.datahub_util import (
    make_datahub_url,
    pretty_anything_urn,
    pretty_dataset_urn,
    strip_urn,
)

logger = logging.getLogger(__name__)


@dataclass
class SlackNotification:
    main_message: str
    attachment: str

    def get_payload(self) -> Dict:
        return {
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": self.main_message},
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": self.attachment},
                },
                {"type": "divider"},
            ]
        }


def make_link(text: str, urn: str, base_url: str) -> str:
    url = make_datahub_url(urn, base_url)
    return f"<{url}|{text}>"


class SlackNotificationConfig(ConfigModel):
    # default webhook posts to #actions-dev-slack-notifications on Acryl Data Slack space
    webhook: SecretStr = SecretStr(
        "https://hooks.slack.com/services/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"
    )
    base_url: str = "http://localhost:9002/"


class SlackNotificationAction(DataHubAction):
    def name(self):
        return "SlackNotificationAction"

    @classmethod
    def create(cls, config_dict: dict, ctx: ActionContext) -> "DataHubAction":
        action_config = SlackNotificationConfig.parse_obj(config_dict or {})
        logger.info(f"Slack notification action configured with {action_config}")
        return cls(action_config, ctx)

    def __init__(self, action_config: SlackNotificationConfig, ctx: ActionContext):
        self.action_config = action_config
        self.ctx = ctx
        self.session = sessions.Session()
        hostname = "unknown-host"
        try:
            import os

            hostname = os.uname()[1]
        except Exception as e:
            logger.warn(f"Failed to acquire hostname with {e}")
            pass

        if True:
            current_time: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            current_timezone: str = str(datetime.datetime.now().astimezone().tzinfo)
            self.session.post(
                url=self.action_config.webhook.get_secret_value(),
                json=SlackNotification(
                    main_message="Hi everyone! I'm the DataHub Slack bot :star:",
                    attachment=f"Host: {hostname}, Time: {current_time} ({current_timezone}).\nI'll be watching for term and tag additions and keep you updated when anything changes :zap:",
                ).get_payload(),
            )

    def subscriptions(self) -> List[Subscription]:
        return AddTerm.get_subscriptions()

    def act(
        self,
        match: Optional[Subscription],
        raw_change: RawMetadataChange,
        semantic_changes: Optional[List[SemanticChange]],
    ) -> List[MetadataChangeProposalWrapper]:
        """This method sends slack notifications"""

        add_term_events: List[AddTermEvent] = AddTerm.from_semantic_changes(
            match, raw_change, semantic_changes
        )
        add_tag_events: List[AddTagEvent] = AddTag.from_semantic_changes(
            match, raw_change, semantic_changes
        )

        if add_term_events or add_tag_events:
            entity_urn: str = raw_change.entity_urn
            downstreams = self.ctx.graph.get_downstreams(entity_urn)

            main_message = (
                self.get_message_for_term_additions(add_term_events, entity_urn)
                if add_term_events
                else ""
            )

            main_message = (
                self.get_message_for_tag_additions(add_tag_events, entity_urn)
                if add_tag_events
                else main_message
            )

            if downstreams and len(downstreams) > 1:
                attachment = f"This dataset has {len(downstreams)} downstreams.\n"
                for downstream in downstreams:
                    attachment = (
                        attachment
                        + f"- {make_link(pretty_anything_urn(downstream), downstream, self.action_config.base_url)}\n"
                    )
            elif downstreams:
                downstream = downstreams[0]
                attachment = f"This dataset has {len(downstreams)} downstream. {make_link(pretty_anything_urn(downstream), downstream, self.action_config.base_url)}"
            else:
                attachment = "No downstreams"

            # logger.info(SlackNotification(main_message, attachment).get_payload())
            self.session.post(
                url=self.action_config.webhook.get_secret_value(),
                json=SlackNotification(main_message, attachment).get_payload(),
            )

        return []

    def get_message_for_tag_additions(
        self, add_tag_events: List[AddTagEvent], entity_urn: str
    ) -> str:
        if len(add_tag_events) == 1:
            tag_id = add_tag_events[0].tag_id
            main_message = f"Detected {make_link(strip_urn('urn:li:tag:', tag_id), tag_id, self.action_config.base_url)} added to {make_link(pretty_dataset_urn(entity_urn), entity_urn, self.action_config.base_url)}"
        else:
            main_message = f"Detected {len(add_tag_events)} terms {[strip_urn('urn:li:tag:', add_tag_event.tag_id) for add_tag_event in add_tag_events]} added to {make_link(pretty_dataset_urn(entity_urn), entity_urn, self.action_config.base_url)}."
        return main_message

    def get_message_for_term_additions(
        self, add_term_events: List[AddTermEvent], entity_urn: str
    ) -> str:
        if len(add_term_events) == 1:
            term_id = add_term_events[0].term_id
            main_message = f"Detected {make_link(strip_urn('urn:li:glossaryTerm:', term_id), term_id, self.action_config.base_url)} added to {make_link(pretty_dataset_urn(entity_urn), entity_urn, self.action_config.base_url)}"
        else:
            main_message = f"Detected {len(add_term_events)} terms {[strip_urn('urn:li:glossaryTerm:', add_term_event.term_id) for add_term_event in add_term_events]} added to {make_link(pretty_dataset_urn(entity_urn), entity_urn, self.action_config.base_url)}."
        return main_message
