#!/usr/bin/env python3
"""
Unit tests for pipeline_logging.py

Covers:
  - Redactor: redact_string, redact_value, redact_dict
  - LogKeyLevelFilter: allow/reject by log_key and level
  - infer_operation: method/change/doc → operation string
  - log_event: structured logging helper
  - RedactingFormatter: message formatting and redaction
  - configure_logging: legacy and full SG-style modes
  - ManagedRotatingFileHandler: directory creation, cleanup
"""

import logging
import os
import sys
import tempfile
import time
import unittest
from unittest.mock import MagicMock

# Ensure the module under test is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pipeline.pipeline_logging as pl


# ===================================================================
# Redactor
# ===================================================================


class TestRedactorRedactString(unittest.TestCase):
    """Tests for Redactor.redact_string()."""

    def test_none_level_returns_unchanged(self):
        r = pl.Redactor("none")
        self.assertEqual(
            r.redact_string("http://user:pass@host/db"), "http://user:pass@host/db"
        )

    def test_partial_redacts_url_userinfo(self):
        r = pl.Redactor("partial")
        result = r.redact_string("http://user:pass@host/db")
        self.assertEqual(result, "http://<ud>***:***</ud>@host/db")

    def test_full_redacts_url_userinfo(self):
        r = pl.Redactor("full")
        result = r.redact_string("http://user:pass@host/db")
        self.assertEqual(result, "http://<ud>XXXXX</ud>@host/db")

    def test_partial_redacts_bearer_token_shows_last_4(self):
        r = pl.Redactor("partial")
        result = r.redact_string("Bearer abcdefgh1234")
        self.assertIn("1234", result)
        self.assertTrue(result.startswith("Bearer "))
        # The original token should not appear in full
        self.assertNotIn("abcdefgh1234", result)

    def test_full_redacts_bearer_token_completely(self):
        r = pl.Redactor("full")
        result = r.redact_string("Bearer abcdefgh1234")
        self.assertEqual(result, "Bearer <ud>XXXXX</ud>")

    def test_partial_url_with_https(self):
        r = pl.Redactor("partial")
        result = r.redact_string("https://admin:secret@example.com:4984/db")
        self.assertEqual(result, "https://<ud>***:***</ud>@example.com:4984/db")

    def test_string_without_sensitive_data_unchanged(self):
        r = pl.Redactor("partial")
        self.assertEqual(r.redact_string("hello world"), "hello world")


class TestRedactorRedactValue(unittest.TestCase):
    """Tests for Redactor.redact_value()."""

    def test_none_level_returns_value_unchanged(self):
        r = pl.Redactor("none")
        self.assertEqual(r.redact_value("password", "secret123"), "secret123")

    def test_non_sensitive_key_returns_unchanged(self):
        r = pl.Redactor("partial")
        self.assertEqual(r.redact_value("doc_id", "doc-abc"), "doc-abc")

    def test_sensitive_key_partial_redacts(self):
        r = pl.Redactor("partial")
        result = r.redact_value("password", "secret123")
        self.assertEqual(result, "<ud>s*******3</ud>")

    def test_sensitive_key_full_redacts(self):
        r = pl.Redactor("full")
        result = r.redact_value("password", "secret123")
        self.assertEqual(result, "<ud>XXXXX</ud>")

    def test_short_value_fully_redacted_in_partial(self):
        r = pl.Redactor("partial")
        result = r.redact_value("password", "ab")
        self.assertEqual(result, "<ud>XXXXX</ud>")

    def test_single_char_value_fully_redacted_in_partial(self):
        r = pl.Redactor("partial")
        result = r.redact_value("token", "x")
        self.assertEqual(result, "<ud>XXXXX</ud>")

    def test_sensitive_key_api_key(self):
        r = pl.Redactor("partial")
        result = r.redact_value("api_key", "mykey123")
        self.assertEqual(result, "<ud>m******3</ud>")

    def test_sensitive_key_authorization(self):
        r = pl.Redactor("full")
        result = r.redact_value("authorization", "Bearer xyz")
        self.assertEqual(result, "<ud>XXXXX</ud>")


class TestRedactorRedactDict(unittest.TestCase):
    """Tests for Redactor.redact_dict()."""

    def test_none_level_returns_dict_unchanged(self):
        r = pl.Redactor("none")
        d = {"password": "secret", "name": "bob"}
        self.assertEqual(r.redact_dict(d), d)

    def test_non_sensitive_keys_unchanged(self):
        r = pl.Redactor("partial")
        d = {"name": "bob", "age": 30}
        self.assertEqual(r.redact_dict(d), {"name": "bob", "age": 30})

    def test_sensitive_keys_redacted(self):
        r = pl.Redactor("partial")
        d = {"password": "secret", "name": "bob"}
        result = r.redact_dict(d)
        self.assertEqual(result["name"], "bob")
        self.assertIn("<ud>", result["password"])

    def test_nested_dict_sensitive_keys_redacted(self):
        r = pl.Redactor("partial")
        d = {"auth": {"password": "secret123", "username": "bob"}}
        result = r.redact_dict(d)
        self.assertEqual(result["auth"]["username"], "bob")
        self.assertEqual(result["auth"]["password"], "<ud>s*******3</ud>")

    def test_full_redacts_nested_dict(self):
        r = pl.Redactor("full")
        d = {"auth": {"token": "abc123"}}
        result = r.redact_dict(d)
        self.assertEqual(result["auth"]["token"], "<ud>XXXXX</ud>")


# ===================================================================
# LogKeyLevelFilter
# ===================================================================


class TestLogKeyLevelFilter(unittest.TestCase):
    """Tests for LogKeyLevelFilter."""

    def _make_record(self, level=logging.INFO, log_key=None):
        record = logging.LogRecord(
            name="test",
            level=level,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=None,
        )
        if log_key is not None:
            record.log_key = log_key
        return record

    def test_matching_key_at_threshold_passes(self):
        f = pl.LogKeyLevelFilter(["CHANGES"], logging.INFO, {})
        record = self._make_record(logging.INFO, "CHANGES")
        self.assertTrue(f.filter(record))

    def test_matching_key_above_threshold_passes(self):
        f = pl.LogKeyLevelFilter(["CHANGES"], logging.INFO, {})
        record = self._make_record(logging.WARNING, "CHANGES")
        self.assertTrue(f.filter(record))

    def test_matching_key_below_threshold_rejected(self):
        f = pl.LogKeyLevelFilter(["CHANGES"], logging.WARNING, {})
        record = self._make_record(logging.INFO, "CHANGES")
        self.assertFalse(f.filter(record))

    def test_non_matching_key_rejected(self):
        f = pl.LogKeyLevelFilter(["CHANGES"], logging.DEBUG, {})
        record = self._make_record(logging.INFO, "HTTP")
        self.assertFalse(f.filter(record))

    def test_allow_all_with_star(self):
        f = pl.LogKeyLevelFilter(["*"], logging.INFO, {})
        record = self._make_record(logging.INFO, "HTTP")
        self.assertTrue(f.filter(record))

    def test_allow_all_any_key(self):
        f = pl.LogKeyLevelFilter(["*"], logging.DEBUG, {})
        record = self._make_record(logging.DEBUG, "MAPPING")
        self.assertTrue(f.filter(record))

    def test_per_key_level_override(self):
        f = pl.LogKeyLevelFilter(["*"], logging.INFO, {"HTTP": logging.WARNING})
        # HTTP at INFO should fail because override requires WARNING
        record = self._make_record(logging.INFO, "HTTP")
        self.assertFalse(f.filter(record))

    def test_per_key_level_override_passes_at_threshold(self):
        f = pl.LogKeyLevelFilter(["*"], logging.INFO, {"HTTP": logging.WARNING})
        record = self._make_record(logging.WARNING, "HTTP")
        self.assertTrue(f.filter(record))

    def test_no_log_key_passes_at_base_level(self):
        f = pl.LogKeyLevelFilter(["CHANGES"], logging.INFO, {})
        record = self._make_record(logging.INFO)
        self.assertTrue(f.filter(record))

    def test_no_log_key_rejected_below_base_level(self):
        f = pl.LogKeyLevelFilter(["CHANGES"], logging.WARNING, {})
        record = self._make_record(logging.INFO)
        self.assertFalse(f.filter(record))

    def test_case_insensitive_log_key(self):
        f = pl.LogKeyLevelFilter(["changes"], logging.INFO, {})
        record = self._make_record(logging.INFO, "CHANGES")
        self.assertTrue(f.filter(record))


# ===================================================================
# infer_operation
# ===================================================================


class TestInferOperation(unittest.TestCase):
    """Tests for infer_operation()."""

    def test_method_delete(self):
        self.assertEqual(pl.infer_operation(method="DELETE"), "DELETE")

    def test_change_deleted_true(self):
        self.assertEqual(pl.infer_operation(change={"deleted": True}), "DELETE")

    def test_method_get(self):
        self.assertEqual(pl.infer_operation(method="GET"), "SELECT")

    def test_rev_starting_with_1(self):
        doc = {"_rev": "1-abc123"}
        self.assertEqual(pl.infer_operation(doc=doc), "INSERT")

    def test_rev_starting_with_2(self):
        doc = {"_rev": "2-abc123"}
        self.assertEqual(pl.infer_operation(doc=doc), "UPDATE")

    def test_rev_starting_with_higher(self):
        doc = {"_rev": "15-abc123"}
        self.assertEqual(pl.infer_operation(doc=doc), "UPDATE")

    def test_no_rev_returns_update(self):
        self.assertEqual(pl.infer_operation(), "UPDATE")

    def test_rev_from_change_changes_list(self):
        change = {"changes": [{"rev": "1-abc"}]}
        self.assertEqual(pl.infer_operation(change=change), "INSERT")

    def test_rev_from_change_changes_list_update(self):
        change = {"changes": [{"rev": "3-xyz"}]}
        self.assertEqual(pl.infer_operation(change=change), "UPDATE")

    def test_delete_method_takes_priority_over_rev(self):
        doc = {"_rev": "1-abc"}
        self.assertEqual(pl.infer_operation(doc=doc, method="DELETE"), "DELETE")

    def test_change_deleted_takes_priority_over_get(self):
        # method is not DELETE, but change says deleted
        change = {"deleted": True}
        self.assertEqual(pl.infer_operation(change=change, method="GET"), "DELETE")


# ===================================================================
# log_event
# ===================================================================


class TestLogEvent(unittest.TestCase):
    """Tests for log_event()."""

    def test_calls_logger_log_with_correct_level_and_extras(self):
        logger = MagicMock()
        logger.isEnabledFor.return_value = True
        pl.log_event(logger, "info", "CHANGES", "test message", doc_id="d1")
        logger.log.assert_called_once_with(
            logging.INFO,
            "test message",
            extra={"log_key": "CHANGES", "doc_id": "d1"},
        )

    def test_does_not_log_when_level_too_high(self):
        logger = MagicMock()
        logger.isEnabledFor.return_value = False
        pl.log_event(logger, "debug", "CHANGES", "test message")
        logger.log.assert_not_called()

    def test_uses_trace_level(self):
        logger = MagicMock()
        logger.isEnabledFor.return_value = True
        pl.log_event(logger, "trace", "HTTP", "trace msg")
        logger.log.assert_called_once_with(
            pl.TRACE,
            "trace msg",
            extra={"log_key": "HTTP"},
        )

    def test_unknown_level_defaults_to_info(self):
        logger = MagicMock()
        logger.isEnabledFor.return_value = True
        pl.log_event(logger, "nonexistent", "OUTPUT", "msg")
        logger.log.assert_called_once()
        self.assertEqual(logger.log.call_args[0][0], logging.INFO)


# ===================================================================
# RedactingFormatter
# ===================================================================


class TestRedactingFormatter(unittest.TestCase):
    """Tests for RedactingFormatter."""

    def _make_record(self, msg="test", level=logging.INFO, **extras):
        record = logging.LogRecord(
            name="test",
            level=level,
            pathname="",
            lineno=0,
            msg=msg,
            args=(),
            exc_info=None,
        )
        for k, v in extras.items():
            setattr(record, k, v)
        return record

    def test_formats_with_log_key_prefix(self):
        fmt = pl.RedactingFormatter(
            pl.Redactor("none"), fmt="[%(levelname)s] %(message)s"
        )
        record = self._make_record("hello", log_key="CHANGES")
        result = fmt.format(record)
        # [CHANGES] is inserted right after [LEVEL] in the prefix
        self.assertIn("[CHANGES]", result)
        self.assertIn("hello", result)

    def test_includes_extra_fields(self):
        fmt = pl.RedactingFormatter(pl.Redactor("none"), fmt="%(message)s")
        record = self._make_record("hello", doc_id="doc1", seq="42")
        result = fmt.format(record)
        # Fields are now pipe-delimited: | doc_id doc1 | seq 42
        self.assertIn("doc_id doc1", result)
        self.assertIn("seq 42", result)
        self.assertIn("|", result)

    def test_redacts_url_field(self):
        fmt = pl.RedactingFormatter(pl.Redactor("partial"), fmt="%(message)s")
        record = self._make_record("request", url="http://admin:pass@host/db")
        result = fmt.format(record)
        self.assertIn("<ud>***:***</ud>", result)
        self.assertNotIn("admin:pass", result)

    def test_redacts_message_content(self):
        fmt = pl.RedactingFormatter(pl.Redactor("partial"), fmt="%(message)s")
        record = self._make_record("connecting to http://user:pwd@host")
        result = fmt.format(record)
        self.assertIn("<ud>***:***</ud>", result)
        self.assertNotIn("user:pwd", result)

    def test_no_extras_no_suffix(self):
        fmt = pl.RedactingFormatter(pl.Redactor("none"), fmt="%(message)s")
        record = self._make_record("plain message")
        result = fmt.format(record)
        self.assertEqual(result, "plain message")

    def test_operation_field_included(self):
        fmt = pl.RedactingFormatter(pl.Redactor("none"), fmt="%(message)s")
        record = self._make_record("op test", operation="INSERT")
        result = fmt.format(record)
        # The formatter renders the `operation` extra under its short alias `op`
        # using a space separator: "op INSERT".
        self.assertIn("op INSERT", result)


# ===================================================================
# configure_logging
# ===================================================================


class TestConfigureLogging(unittest.TestCase):
    """Tests for configure_logging()."""

    def setUp(self):
        # Save root handler state
        self._root = logging.getLogger()
        self._original_handlers = self._root.handlers[:]

    def tearDown(self):
        # Restore root handler state
        root = logging.getLogger()
        root.handlers = self._original_handlers

    def test_legacy_mode_configures_console_handler(self):
        pl.configure_logging({"level": "WARNING"})
        listener = pl._queue_listener
        self.assertIsNotNone(listener)
        self.assertEqual(len(listener.handlers), 1)
        handler = listener.handlers[0]
        self.assertIsInstance(handler, logging.StreamHandler)
        self.assertEqual(handler.level, logging.WARNING)

    def test_legacy_mode_sets_redactor(self):
        pl.configure_logging({"level": "DEBUG", "redaction_level": "full"})
        self.assertEqual(pl.get_redactor().level, "full")

    def test_full_mode_console_handler(self):
        cfg = {
            "redaction_level": "partial",
            "console": {
                "enabled": True,
                "log_level": "debug",
                "log_keys": ["*"],
            },
        }
        pl.configure_logging(cfg)
        listener = pl._queue_listener
        self.assertIsNotNone(listener)
        self.assertTrue(len(listener.handlers) >= 1)
        self.assertEqual(pl.get_redactor().level, "partial")

    def test_full_mode_file_handler(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = os.path.join(tmpdir, "test.log")
            cfg = {
                "redaction_level": "none",
                "console": {"enabled": False},
                "file": {
                    "enabled": True,
                    "path": log_path,
                    "log_level": "info",
                    "log_keys": ["*"],
                    "rotation": {
                        "max_size": 1,
                        "max_age": 1,
                        "rotated_logs_size_limit": 10,
                    },
                },
            }
            pl.configure_logging(cfg)
            # Real handlers are behind the QueueListener.
            # log_level=info creates 2 file tiers: _info + _error
            listener = pl._queue_listener
            self.assertIsNotNone(listener)
            file_handlers = [
                h
                for h in listener.handlers
                if isinstance(h, pl.ManagedRotatingFileHandler)
            ]
            self.assertEqual(len(file_handlers), 2)
            for h in listener.handlers:
                h.close()
            listener.stop()

    def test_full_mode_both_handlers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = os.path.join(tmpdir, "test.log")
            cfg = {
                "redaction_level": "partial",
                "console": {
                    "enabled": True,
                    "log_level": "info",
                    "log_keys": ["*"],
                },
                "file": {
                    "enabled": True,
                    "path": log_path,
                    "log_level": "debug",
                    "log_keys": ["*"],
                },
            }
            pl.configure_logging(cfg)
            # Real handlers are behind the QueueListener; root has 1 QueueHandler.
            # log_level=debug creates 3 file tiers: _debug + _info + _error
            # plus 1 console handler = 4 total
            listener = pl._queue_listener
            self.assertIsNotNone(listener)
            self.assertEqual(len(listener.handlers), 4)
            for h in listener.handlers:
                h.close()
            listener.stop()

    def test_legacy_mode_default_redaction_is_none(self):
        pl.configure_logging({"level": "DEBUG"})
        self.assertEqual(pl.get_redactor().level, "none")

    def test_console_with_key_levels(self):
        cfg = {
            "console": {
                "enabled": True,
                "log_level": "info",
                "log_keys": ["*"],
                "key_levels": {"HTTP": "warn"},
            },
        }
        pl.configure_logging(cfg)
        # Real handlers are behind the QueueListener; root has a QueueHandler.
        listener = pl._queue_listener
        self.assertIsNotNone(listener)
        handler = listener.handlers[0]
        filters = handler.filters
        self.assertEqual(len(filters), 1)
        self.assertIsInstance(filters[0], pl.LogKeyLevelFilter)


# ===================================================================
# ManagedRotatingFileHandler
# ===================================================================


class TestManagedRotatingFileHandler(unittest.TestCase):
    """Tests for ManagedRotatingFileHandler."""

    def test_creates_directory_if_not_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nested = os.path.join(tmpdir, "subdir", "deep")
            log_path = os.path.join(nested, "test.log")
            handler = pl.ManagedRotatingFileHandler(log_path, max_size_mb=1)
            self.assertTrue(os.path.isdir(nested))
            handler.close()

    def test_cleanup_removes_old_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = os.path.join(tmpdir, "test.log")
            handler = pl.ManagedRotatingFileHandler(
                log_path,
                max_size_mb=1,
                max_age_days=0,
            )
            # Create fake rotated files with old modification times
            old_file = log_path + ".1"
            with open(old_file, "w") as f:
                f.write("old log data")
            # Set modification time to 2 days ago
            old_time = time.time() - 2 * 86400
            os.utime(old_file, (old_time, old_time))

            handler._cleanup_rotated_files()
            self.assertFalse(os.path.exists(old_file))
            handler.close()

    def test_cleanup_enforces_size_limit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = os.path.join(tmpdir, "test.log")
            # Set very small rotated_logs_size_limit (1 byte effectively)
            handler = pl.ManagedRotatingFileHandler(
                log_path,
                max_size_mb=1,
                max_age_days=30,
                rotated_logs_size_limit_mb=0,
            )
            # Create fake rotated files
            for i in range(3):
                rfile = log_path + f".{i + 1}"
                with open(rfile, "w") as f:
                    f.write("x" * 100)

            handler._cleanup_rotated_files()
            # All files should be removed since limit is 0
            remaining = [f for f in os.listdir(tmpdir) if f.startswith("test.log.")]
            self.assertEqual(len(remaining), 0)
            handler.close()


# ===================================================================
# TRACE level
# ===================================================================


class TestTraceLevel(unittest.TestCase):
    """Tests for custom TRACE level."""

    def test_trace_level_value(self):
        self.assertEqual(pl.TRACE, 5)

    def test_trace_level_name(self):
        self.assertEqual(logging.getLevelName(pl.TRACE), "TRACE")

    def test_logger_has_trace_method(self):
        logger = logging.getLogger("test_trace")
        self.assertTrue(hasattr(logger, "trace"))


if __name__ == "__main__":
    unittest.main()
