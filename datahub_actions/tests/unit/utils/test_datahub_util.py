from datahub_actions.utils.datahub_util import (
    entity_type_from_urn,
    pretty_anything_urn,
    pretty_dataset_urn,
    pretty_user_urn,
    sanitize_urn,
    sanitize_user_urn_for_search,
    strip_urn,
)


def test_pretty_dataset_urn():
    assert (
        pretty_dataset_urn(
            "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"
        )
        == "hive:SampleHiveDataset"
    )


def test_pretty_dataset_urn_with_instances():
    assert (
        pretty_dataset_urn(
            "urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datahubretentionindex_v2,PROD)"
        )
        == "elasticsearch:prod_index.datahubretentionindex_v2"
    )


def test_pretty_anything_urn():
    assert (
        pretty_anything_urn(
            "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"
        )
        == "dataset:(dataPlatform:hive,SampleHiveDataset,PROD)"
    )


def test_pretty_anything_urn_with_instances():
    assert (
        pretty_anything_urn(
            "urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.logging_events,PROD)"
        )
        == "dataset:(dataPlatform:hive,warehouse.logging_events,PROD)"
    )


def test_strip_urn():
    assert (
        strip_urn(
            "urn:li:dataset:",
            "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        )
        == "(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"
    )


def test_entity_type_from_urn():
    assert (
        entity_type_from_urn(
            "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"
        )
        == "dataset"
    )


def test_entity_type_from_with_instances():
    assert (
        entity_type_from_urn(
            "urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datahubretentionindex_v2,PROD)"
        )
        == "dataset"
    )


def test_sanitize_urn():
    assert (
        sanitize_urn("urn:li:corpuser:aseem.bansal")
        == "urn\\:li\\:corpuser\\:aseem.bansal"
    )


def test_pretty_user_urn():
    assert pretty_user_urn("urn:li:corpuser:aseem.bansal") == "aseem.bansal"


def test_sanitize_for_search():
    assert sanitize_user_urn_for_search("urn:li:corpuser:datahub") == "datahub"
    assert (
        sanitize_user_urn_for_search("urn:li:corpuser:aseem.bansal") == r"aseem\.bansal"
    )
