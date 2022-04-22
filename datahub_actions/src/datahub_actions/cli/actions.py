import logging
import pathlib
import signal
import time
from typing import Any, List

import click
from datahub.configuration.config_loader import load_config_file

import datahub_actions as datahub_actions_package
from datahub_actions.pipeline.actions_manager import ActionsManager
from datahub_actions.pipeline.pipeline import Pipeline

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Instantiate a singleton instance of the Actions Manager.
actions_manager = ActionsManager()
actions_manager.init()


def config_to_pipelines(config: dict) -> List[Pipeline]:
    raise Exception("Config based pipeline creation not yet supported.")


def pipeline_config_to_pipeline(pipeline_config: dict) -> Pipeline:
    logger.debug(
        f"Attempting to create Actions Pipeline using config {pipeline_config}"
    )
    try:
        return Pipeline.create(pipeline_config)
    except Exception as e:
        raise Exception(
            f"Failed to instantiate Actions Pipeline using config {pipeline_config}. Exiting.."
        ) from e


@click.command(
    name="actions",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option("-p", "--pipeline", required=False, type=str, multiple=True)
@click.option("-c", "--config", required=False, type=str)
@click.pass_context
def actions(ctx: Any, pipeline: List[str], config: str) -> None:
    """Execute one or more Actions Pipelines"""

    logger.info(
        "DataHub Actions version: %s", datahub_actions_package.nice_version_name()
    )

    # Statically configured to be registered with the Actions Manager.
    pipelines: List[Pipeline] = []

    logger.debug("Creating Actions Pipelines...")

    # If the master config was provided, simply convert it into a list of pipelines.
    if config is not None:
        config_file = pathlib.Path(config)
        config_dict = load_config_file(config_file)
        for pipeline in config_to_pipelines(config_dict):
            pipelines.append(pipeline)

    # If individual pipeline config was provided, create a pipeline from it.
    if pipeline is not None:
        for pipeline_config in pipeline:
            pipeline_config_file = pathlib.Path(pipeline_config)
            pipeline_config_dict = load_config_file(pipeline_config_file)
            # Now, instantiate the pipeline.
            pipelines.append(pipeline_config_to_pipeline(pipeline_config_dict))

    logger.debug("Starting Actions Pipelines...")

    # Start each pipeline.
    for p in pipelines:
        actions_manager.start_pipeline(p.name, p)

    logger.debug("Started all Action Pipelines.")

    # Now, simply run forever.
    while True:
        # Todo: improve this. 
        time.sleep(5)


# Handle shutdown signal.
def handle_shutdown(signum, frame):
    logger.info("Terminating all running Action Pipelines...")
    actions_manager.terminate_all()
    exit(1)


signal.signal(signal.SIGINT, handle_shutdown)
