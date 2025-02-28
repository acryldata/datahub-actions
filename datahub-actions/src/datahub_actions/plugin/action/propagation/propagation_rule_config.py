from enum import Enum
from typing import Dict, List, Optional, Union

from datahub.configuration.common import ConfigModel


class PropagationRelationships(str, Enum):
    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"
    SIBLING = "sibling"


class AspectReference(ConfigModel):
    name: str
    field: str


class AspectLookup(ConfigModel):
    type: str
    aspect_name: str
    field: str
    # Whether to look at the MCL's previous or new aspect value.
    # Will usually be new aspect value.
    use_previous_aspect: Optional[bool] = False


class RelationshipReference(ConfigModel):
    name: str
    is_source: bool = False


class RelationshipLookup(ConfigModel):
    type: PropagationRelationships
    relationshipName: Optional[str] = "DownstreamOf"
    # Whether to search for urn as search or destination.
    # Will usually be destination.
    isSource: Optional[bool] = False


class MCLLookup(ConfigModel):
    type: str
    field: str
    use_previous_value: bool = False


EntityLookup = Union[AspectLookup, RelationshipLookup]


class PropagatedMetadata(Enum):
    TAGS = ("tags",)
    TERMS = ("terms",)
    DOCUMENTATION = ("documentation",)
    DOMAIN = ("domain",)
    STRUCTURED_PROPERTIES = ("structuredProperties",)


class MclTriggerRule(ConfigModel):
    trigger: AspectLookup
    sourceUrnResolution: List[EntityLookup]
    targetUrnResolution: List[EntityLookup]


class PropagationRule(ConfigModel):
    metadataPropagated: Dict[PropagatedMetadata, Dict] = {}
    entityTypes: List[str]
    targetUrnResolution: List[EntityLookup]
