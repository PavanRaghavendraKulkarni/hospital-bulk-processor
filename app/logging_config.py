"""Structured logging via structlog.

Context such as batch_id is bound with ``structlog.contextvars.bind_contextvars`` and is
then attached automatically to every log line emitted in that task/request.
"""

import logging
import sys

import structlog


def configure_logging(level: str = "INFO", json: bool = True) -> None:
    log_level = logging.getLevelName(level.upper())
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=log_level)

    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    return structlog.get_logger(name)
