from typing import List, Union

from datahub.configuration.common import ConfigModel


class AspectReference(ConfigModel):
    name: str
    field: str


class AspectLookup(ConfigModel):
    type: str


class RelationshipReference(ConfigModel):
    name: str
    is_source: bool = False


class RelationshipsLookup(ConfigModel):
    type: str
    relationships: List[RelationshipReference]


class MCLLookup(ConfigModel):
    type: str
    field: str
    use_previous_value: bool = False


EntityLookup = Union[AspectLookup, RelationshipsLookup, MCLLookup]


class MclTriggerRule(ConfigModel):
    trigger: AspectLookup
    sourceUrnResolution: List[EntityLookup]
    targetUrnResolution: List[EntityLookup]
