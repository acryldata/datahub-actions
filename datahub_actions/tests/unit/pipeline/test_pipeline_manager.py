import pytest

from datahub_actions.pipeline.pipeline import Pipeline
from datahub_actions.pipeline.pipeline_manager import PipelineManager

pipeline_manager = PipelineManager()


@pytest.mark.dependency()
def test_start_pipeline():

    # Create test pipeline
    config = _build_valid_pipeline_config()
    pipeline = Pipeline.create(config)

    # Now, simply start the pipeline
    pipeline_manager.start_pipeline("test", pipeline)

    # Verify that the pipeline is running
    assert len(pipeline_manager.pipeline_registry.keys()) == 1


@pytest.mark.dependency(depends=["test_start_pipeline"])
def test_stop_pipeline():
    pipeline_manager = PipelineManager()

    # Verify that the pipeline is running
    assert len(pipeline_manager.pipeline_registry.keys()) == 1

    # Stop the pipeline
    pipeline_manager.stop_pipeline("test")

    # Verify that no pipelines are running
    assert len(pipeline_manager.pipeline_registry.keys()) == 0


@pytest.mark.dependency(depends=["test_stop_pipeline"])
def test_stop_all():

    # Create test pipeline
    config = _build_valid_pipeline_config()
    pipeline_1 = Pipeline.create(config)
    pipeline_2 = Pipeline.create(config)

    # Now, start the pipelines
    pipeline_manager.start_pipeline("test_1", pipeline_1)
    pipeline_manager.start_pipeline("test_2", pipeline_2)

    # Verify that the pipelines are running
    assert len(pipeline_manager.pipeline_registry.keys()) == 2

    # Stop all pipelines
    pipeline_manager.stop_all()

    # Verify that no pipelines are running
    assert len(pipeline_manager.pipeline_registry.keys()) == 0


def _build_valid_pipeline_config() -> dict:
    return {
        "name": "stoppable-pipeline",
        "source": {"type": "stoppable_event_source", "config": {}},
        "transform": [{"type": "test_transformer", "config": {"config1": "value1"}}],
        "action": {"type": "test_action", "config": {"config1": "value1"}},
        "options": {
            "retry_count": 3,
            "failure_mode": "CONTINUE",
            "failed_events_dir": "/tmp/datahub/test",
        },
    }
