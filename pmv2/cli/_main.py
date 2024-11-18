"""Click entrypoint is defined here."""

import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from typing import Literal

import click
import structlog
from dotenv import load_dotenv

from pmv2._version import VERSION
from pmv2.urban_client import UrbanClient, make_http_client

load_dotenv(os.environ.get("ENVFILE", ".env"))


@dataclass
class Config:
    """pmv2 main group config."""

    urban_client: UrbanClient
    logger: structlog.stdlib.BoundLogger


pass_config = click.make_pass_decorator(Config)

_LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def _configure_logging(log_level: _LogLevel, files: dict[str, _LogLevel] | None = None) -> structlog.stdlib.BoundLogger:
    level_name_mapping = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    if files is None:
        files = {}
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    logger.setLevel(level_name_mapping[log_level])

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(processor=structlog.dev.ConsoleRenderer(colors=True))
    )

    root_logger = logging.getLogger()
    root_logger.addHandler(console_handler)

    for filename, level in files.items():
        file_handler = logging.FileHandler(filename=filename, encoding="utf-8")
        file_handler.setFormatter(structlog.stdlib.ProcessorFormatter(processor=structlog.processors.JSONRenderer()))
        file_handler.setLevel(level_name_mapping[level])
        root_logger.addHandler(file_handler)

    root_logger.setLevel("INFO")

    return logger


@click.group("pmv2")
@click.version_option(VERSION)
@click.pass_context
@click.option(
    "--host",
    type=str,
    envvar="HOST",
    show_envvar=True,
    required=True,
    help="Host of Urban API instance to use for requests",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    default="DEBUG",
    envvar="LOG_LEVEL",
    show_envvar=True,
    show_default=True,
    help="Level for logging",
)
def main(ctx: click.Context, host: str, log_level):
    """Platform manipulation command line script."""
    logger = _configure_logging(log_level, {"./pmv2.log": "DEBUG"})

    urban_client = make_http_client(host, logger)
    if not asyncio.run(urban_client.is_alive()):
        logger.warning("urban_api unavailable", host=host)
    ctx.obj = Config(urban_client, logger)
