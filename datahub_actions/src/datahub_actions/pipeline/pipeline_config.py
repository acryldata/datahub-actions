from enum import Enum
from typing import Any, Dict, List, Optional, Union

from datahub.configuration import ConfigModel
from datahub.ingestion.graph.client import DatahubClientConfig
from pydantic import BaseModel


class FailureMode(str, Enum):
    # Log the failed event to the failed events log. Then throw an pipeline exception to stop the pipeline.
    THROW = "THROW"
    # Log the failed event to the failed events log. Then continue processing the event stream.
    CONTINUE = "CONTINUE"


class SourceConfig(ConfigModel):
    type: str
    config: Optional[Dict[str, Any]]


class TransformConfig(ConfigModel):
    type: str
    config: Optional[Dict[str, Any]]


class FilterConfig(ConfigModel):
    event_type: Union[str, List[str]]
    event: Optional[Dict[str, Any]]


class ActionConfig(ConfigModel):
    type: str
    config: Optional[dict]


class PipelineOptions(BaseModel):
    retry_count: Optional[int]
    failure_mode: Optional[FailureMode]
    failed_events_dir: Optional[str]  # The path where failed events should be logged.

    class Config:
        use_enum_values = True


class PipelineConfig(BaseModel):
    """
    Configuration required to create a new Actions Pipeline.

    This exactly matches the structure of the YAML file used
    to configure a Pipeline.
    """

    name: str
    source: SourceConfig
    filter: Optional[FilterConfig]
    transform: Optional[List[TransformConfig]]
    action: ActionConfig
    datahub: Optional[DatahubClientConfig]
    options: Optional[PipelineOptions]

    class Config:
        extra = "allow"
