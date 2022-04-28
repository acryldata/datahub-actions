from datahub_actions.plugin.source.kafka.kafka_event_source import KafkaEventSource
from tests.unit.test_helpers import TestMessage


def test_handle_mcl():
    msg = TestMessage(
        {
            "value": {
                "entityType": "dataset",
                "changeType": "ADD",
            },
        }
    )
    result = list(KafkaEventSource.handle_mcl(msg))[0]
    assert result is not None
