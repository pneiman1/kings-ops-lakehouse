"""
Structured logging with run correlation.

Every log line this platform emits is a single-line JSON object carrying a
``run_id`` that is stable for the lifetime of one job run. That one property is
what turns "the pipeline failed last night" into a query. Human-readable
multi-line log output is pleasant to read one at a time and useless in
aggregate; the event log tranche (T7) parses these records directly.

No Databricks imports here either — logging must work identically in pytest,
in a notebook, and in a job.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

_RUN_ID_ENV = "KINGS_RUN_ID"


def get_run_id() -> str:
    """Stable correlation ID for this process.

    Prefers the Databricks job run ID when present so platform logs and Databricks'
    own run history can be joined. Falls back to a generated UUID for local and
    interactive execution.
    """
    for candidate in (_RUN_ID_ENV, "DATABRICKS_JOB_RUN_ID", "DATABRICKS_RUN_ID"):
        value = os.environ.get(candidate)
        if value:
            return value
    generated = str(uuid.uuid4())
    os.environ[_RUN_ID_ENV] = generated
    return generated


class JsonFormatter(logging.Formatter):
    """Emit one JSON object per record.

    ``extra`` keys are merged into the top level rather than nested, so the event
    log parser can project them as columns without unwrapping a struct.
    """

    RESERVED = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "taskName", "message", "asctime",
    }

    def __init__(self, *, environment: str = "unknown", component: str = "unknown") -> None:
        super().__init__()
        self.environment = environment
        self.component = component
        self.run_id = get_run_id()

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "run_id": self.run_id,
            "environment": self.environment,
            "component": self.component,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in self.RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(
    environment: str = "unknown",
    component: str = "unknown",
    level: str = "INFO",
) -> logging.Logger:
    """Install the JSON handler on the ``kings_ops`` logger tree.

    Idempotent: repeated calls replace the handler rather than stacking them,
    because notebook cells get re-run and duplicated log lines destroy the
    reliability of any count-based alert built on top of them.
    """
    logger = logging.getLogger("kings_ops")
    logger.setLevel(level)
    logger.propagate = False
    for existing in list(logger.handlers):
        logger.removeHandler(existing)

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter(environment=environment, component=component))
    logger.addHandler(handler)
    return logger


def get_logger(name: str) -> logging.Logger:
    """Child logger under the configured ``kings_ops`` root."""
    return logging.getLogger(f"kings_ops.{name}")


@contextmanager
def log_stage(logger: logging.Logger, stage: str, **context: Any) -> Iterator[dict[str, Any]]:
    """Time a named stage and emit start/finish records with a duration.

    The yielded dict is mutable: a caller records ``metrics["rows_written"] = n``
    and the value lands on the completion record. That is what makes rows-per-stage
    queryable later without a separate metrics pipeline.

    Failures are logged with the elapsed time and re-raised — this context manager
    observes, it does not swallow.
    """
    metrics: dict[str, Any] = {}
    started = time.perf_counter()
    logger.info("stage.start", extra={"stage": stage, **context})
    try:
        yield metrics
    except Exception:
        logger.exception(
            "stage.failed",
            extra={
                "stage": stage,
                "duration_seconds": round(time.perf_counter() - started, 3),
                **context,
                **metrics,
            },
        )
        raise
    logger.info(
        "stage.complete",
        extra={
            "stage": stage,
            "duration_seconds": round(time.perf_counter() - started, 3),
            **context,
            **metrics,
        },
    )
