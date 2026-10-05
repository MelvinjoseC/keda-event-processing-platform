import json
import logging

from app.logging_config import (
    JsonLogFormatter,
    configure_logging,
    current_correlation_id,
)


def test_json_log_formatter_format():
    formatter = JsonLogFormatter("test-service")
    record = logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="Test message %s",
        args=("arg1",),
        exc_info=None,
    )
    token = current_correlation_id.set("corr-test-123")
    try:
        output = formatter.format(record)
    finally:
        current_correlation_id.reset(token)

    data = json.loads(output)
    assert data["service"] == "test-service"
    assert data["level"] == "INFO"
    assert data["message"] == "Test message arg1"
    assert data["correlation_id"] == "corr-test-123"
    assert "timestamp" in data


def test_json_log_formatter_without_correlation_id():
    formatter = JsonLogFormatter("test-service")
    record = logging.LogRecord(
        name="test.logger",
        level=logging.WARNING,
        pathname=__file__,
        lineno=20,
        msg="Warning without corr id",
        args=(),
        exc_info=None,
    )
    output = formatter.format(record)
    data = json.loads(output)
    assert data["service"] == "test-service"
    assert data["level"] == "WARNING"
    assert "correlation_id" not in data


def test_configure_logging_returns_logger():
    logger = configure_logging("test-svc")
    assert logger.name == "test-svc"
