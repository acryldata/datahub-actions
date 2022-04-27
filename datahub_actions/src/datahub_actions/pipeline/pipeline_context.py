from dataclasses import dataclass
from typing import Optional

from datahub_actions.api.action_graph import AcrylDataHubGraph


@dataclass
class PipelineContext:
    """
    Context which is provided to each component in a Pipeline.
    """

    # The name of the running pipeline.
    pipeline_name: str

    # An instance of a DataHub client.
    graph: Optional[AcrylDataHubGraph]
