"""
Structured JSON logging setup for all pipeline services.
Outputs log lines Logstash / Loki can parse without regex hacks.
"""
import logging
import sys
import os
import structlog
from datetime import datetime, timezone


def configure_logging(service_name: str, log_level: str = "INFO") -> None:
    """Configure structured JSON logging for a service."""

    level = getattr(logging, log_level.upper(), logging.INFO)

    # stdlib handler that writes JSON to stdout
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )

    # structlog processors chain
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]

    structlog.configure(
        processors=shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Bind service-wide fields so every log line includes them
    structlog.contextvars.bind_contextvars(
        service=service_name,
        environment=os.getenv("ENVIRONMENT", "production"),
    )


def get_logger(name: str):
    """Get a structlog logger — use this everywhere instead of logging.getLogger."""
    return structlog.get_logger(name)
