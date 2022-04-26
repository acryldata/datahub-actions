import logging
import platform
import sys

import click
import stackprinter
from datahub.configuration import SensitiveError
from datahub.entrypoints import datahub

import datahub_actions as datahub_package
from datahub_actions.cli.actions import actions

logger = logging.getLogger(__name__)

# Configure logger.
BASE_LOGGING_FORMAT = (
    "[%(asctime)s] %(levelname)-8s {%(name)s:%(lineno)d} - %(message)s"
)
logging.basicConfig(format=BASE_LOGGING_FORMAT)

MAX_CONTENT_WIDTH = 120

# Add "actions" command to datahub.
datahub.add_command(actions)


def main(**kwargs):
    # This wrapper prevents click from suppressing errors.
    try:
        sys.exit(datahub(standalone_mode=False, **kwargs))
    except click.exceptions.Abort:
        # Click already automatically prints an abort message, so we can just exit.
        sys.exit(1)
    except click.ClickException as error:
        error.show()
        sys.exit(1)
    except Exception as exc:
        kwargs = {}
        sensitive_cause = SensitiveError.get_sensitive_cause(exc)
        if sensitive_cause:
            kwargs = {"show_vals": None}
            exc = sensitive_cause

        logger.error(
            stackprinter.format(
                exc,
                line_wrap=MAX_CONTENT_WIDTH,
                truncate_vals=10 * MAX_CONTENT_WIDTH,
                suppressed_paths=[r"lib/python.*/site-packages/click/"],
                **kwargs,
            )
        )
        logger.info(
            f"DataHub Actions version: {datahub_package.__version__} at {datahub_package.__file__}"
        )
        logger.info(
            f"Python version: {sys.version} at {sys.executable} on {platform.platform()}"
        )
        sys.exit(1)
