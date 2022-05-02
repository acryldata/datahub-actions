# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import pathlib
import signal
import sys
import time
from typing import Any, List

import click
from click_default_group import DefaultGroup
from datahub.configuration.config_loader import load_config_file

import datahub_actions as datahub_actions_package
from datahub_actions.pipeline.pipeline import Pipeline
from datahub_actions.pipeline.pipeline_manager import PipelineManager

logger = logging.getLogger(__name__)


# Instantiate a singleton instance of the Pipeline Manager.
pipeline_manager = PipelineManager()


def pipeline_config_to_pipeline(pipeline_config: dict) -> Pipeline:
    logger.debug(
        f"Attempting to create Actions Pipeline using config {pipeline_config}"
    )
    try:
        return Pipeline.create(pipeline_config)
    except Exception as e:
        raise Exception(
            f"Failed to instantiate Actions Pipeline using config {pipeline_config}"
        ) from e


@click.group(cls=DefaultGroup, default="run")
def actions() -> None:
    """Execute one or more Actions Pipelines"""
    pass


@actions.command(
    name="run",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option("-c", "--config", required=True, type=str, multiple=True)
@click.option("--debug/--no-debug", default=False)
@click.pass_context
def run(ctx: Any, config: List[str], debug: bool) -> None:
    """Execute one or more Actions Pipelines"""

    logger.info(
        "DataHub Actions version: %s", datahub_actions_package.nice_version_name()
    )

    if debug:
        # Set root logger settings to debug mode.
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        # Set root logger settings to info mode.
        logging.getLogger().setLevel(logging.INFO)

    # Statically configured to be registered with the pipeline Manager.
    pipelines: List[Pipeline] = []

    logger.debug("Creating Actions Pipelines...")

    # If individual pipeline config was provided, create a pipeline from it.
    if config is not None:
        for pipeline_config in config:
            pipeline_config_file = pathlib.Path(pipeline_config)
            pipeline_config_dict = load_config_file(pipeline_config_file)
            pipelines.append(
                pipeline_config_to_pipeline(pipeline_config_dict)
            )  # Now, instantiate the pipeline.

    logger.debug("Starting Actions Pipelines")

    # Start each pipeline.
    for p in pipelines:
        pipeline_manager.start_pipeline(p.name, p)
        logger.info(f"Action Pipeline with name '{p.name}' is now running.")

    # Now, simply run forever.
    while True:
        time.sleep(5)


@actions.command()
def version() -> None:
    """Print version number and exit."""
    click.echo(
        f"DataHub Actions version: {datahub_actions_package.nice_version_name()}"
    )
    click.echo(f"Python version: {sys.version}")


# Handle shutdown signal. (ctrl-c)
def handle_shutdown(signum, frame):
    logger.info("Stopping all running Action Pipelines...")
    pipeline_manager.stop_all()
    exit(1)


signal.signal(signal.SIGINT, handle_shutdown)
