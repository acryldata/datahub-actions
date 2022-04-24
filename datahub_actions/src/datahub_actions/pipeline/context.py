from dataclasses import dataclass

from datahub_actions.api.action_core import AcrylDataHubGraph


@dataclass
class ActionContext:
    # The name of the running pipeline.
    pipeline_name: str

    # An instance of a DataHub client.
    graph: AcrylDataHubGraph
