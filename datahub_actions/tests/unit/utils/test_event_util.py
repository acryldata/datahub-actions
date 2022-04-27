import json

import pytest
from datahub.metadata.schema_classes import (
    DomainsClass,
    GenericAspectClass,
    GenericPayloadClass,
)

from datahub_actions.utils.event_util import parse_generic_aspect, parse_generic_payload


def test_parse_generic_aspect():
    # Success case
    test_domain_urn = "urn:li:domain:test"
    domains_obj = {"domains": [test_domain_urn]}
    generic_aspect = GenericAspectClass(
        value=json.dumps(domains_obj).encode(), contentType="application/json"
    )
    assert parse_generic_aspect(DomainsClass, generic_aspect) == DomainsClass(
        [test_domain_urn]
    )

    bad_aspect = GenericAspectClass(
        value=json.dumps(domains_obj).encode(), contentType="avrojson"
    )
    with pytest.raises(Exception, match="Unsupported content-type"):
        parse_generic_aspect(DomainsClass, bad_aspect)


def test_parse_generic_payload():
    # Success case
    test_domain_urn = "urn:li:domain:test"
    domains_obj = {"domains": [test_domain_urn]}
    generic_payload = GenericPayloadClass(
        value=json.dumps(domains_obj).encode(), contentType="application/json"
    )
    assert parse_generic_payload(DomainsClass, generic_payload) == DomainsClass(
        [test_domain_urn]
    )

    bad_payload = GenericPayloadClass(
        value=json.dumps(domains_obj).encode(), contentType="avrojson"
    )
    with pytest.raises(Exception, match="Unsupported content-type"):
        parse_generic_payload(DomainsClass, bad_payload)
