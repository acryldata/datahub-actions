import logging
import re

ENTITY_TYPE_TO_URL_PATH_MAP = {
    "glossaryTerm": "glossary",
    "dataset": "dataset",
    "tag": "tag",
    "corpuser": "user",
}
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def strip_urn(prefix: str, urn: str) -> str:
    return urn.lstrip(prefix)


def pretty_anything_urn(urn: str) -> str:
    return urn.replace("urn:li:", "")


def sanitize_urn(urn: str) -> str:
    return urn.replace(":", r"\:").replace("(", r"\(").replace(")", r"\)")


def sanitize_user_urn_for_search(urn: str) -> str:
    return sanitize_urn(pretty_user_urn(urn)).replace(".", r"\.")


def pretty_dataset_urn(urn: str) -> str:
    strip_dataset = strip_urn("urn:li:dataset:", urn)
    dataset_parts = strip_dataset[1:-1].split(",")
    platform = strip_urn("urn:li:dataPlatform:", dataset_parts[0])
    name = dataset_parts[1]
    return f"{platform}:{name}"


def pretty_user_urn(urn: str) -> str:
    return strip_urn("urn:li:corpuser:", urn)


def entity_type_from_urn(urn: str) -> str:
    entity_match = re.search(r"urn:li:([a-zA-Z]*):", urn)
    assert entity_match
    entity_type = entity_match.group(1)
    return entity_type


def make_datahub_url(urn: str, base_url: str) -> str:
    entity_type = entity_type_from_urn(urn)
    urn = urn.replace("/", "%2F")
    return f"{base_url}/{ENTITY_TYPE_TO_URL_PATH_MAP[entity_type]}/{urn}/"
