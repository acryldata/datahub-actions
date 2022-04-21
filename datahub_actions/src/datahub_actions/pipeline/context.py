from dataclasses import dataclass

from datahub_actions.api.action_core import AcrylDataHubGraph


@dataclass
class ActionContext:
    graph: AcrylDataHubGraph
