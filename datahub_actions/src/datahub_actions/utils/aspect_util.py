import json
from typing import Type, TypeVar

from datahub.emitter.serialization_helper import post_json_transform
from datahub.metadata.schema_classes import (
    DictWrapper,
    GenericAspectClass,
    GenericPayloadClass,
)

TC = TypeVar("TC", bound=DictWrapper)


def parse_generic_aspect(cls: Type[TC], aspect: GenericAspectClass) -> TC:
    if aspect.contentType != "application/json":
        raise Exception(
            f"Failed to parse aspect. Unsupported content-type {aspect.contentType} provided!"
        )
    return cls.from_obj(post_json_transform(json.loads(aspect.get("value"))))


def parse_generic_payload(cls: Type[TC], payload: GenericPayloadClass) -> TC:
    if payload.contentType != "application/json":
        raise Exception(
            f"Failed to parse payload. Unsupported content-type {payload.contentType} provided!"
        )
    return cls.from_obj(post_json_transform(json.loads(payload.get("value"))))
