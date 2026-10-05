import json
import logging

from app.logging_config import (
    JsonLogFormatter,
    configure_logging,
    current_correlation_id,
)


def test_worker_json_log_formatter():
    formatter = JsonLogFormatter("event-worker")
    record = logging.LogRecord(
        name="worker.logger",
        level=logging.ERROR,
        pathname=__file__,
        lineno=15,
        msg="Worker error: %s",
        args=("timeout",),
        exc_info=None,
    )
    token = current_correlation_id.set("worker-corr-999")
    try:
        output = formatter.format(record)
    finally:
        current_correlation_id.reset(token)

    data = json.loads(output)
    assert data["service"] == "event-worker"
    assert data["level"] == "ERROR"
    assert data["message"] == "Worker error: timeout"
    assert data["correlation_id"] == "worker-corr-999"


def test_worker_configure_logging():
    logger = configure_logging("event-worker")
    assert logger.name == "event-worker"
