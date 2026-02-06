"""Integration tests for the alerting system.

These tests verify the alerting functionality including email and Slack
notifications, alert formatting, and suppression rules.
"""

import json
import os
import unittest
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from unittest.mock import MagicMock, patch

import sys
sys.path.insert(0, "/opt/airflow/pipelines")

from utils.alerting import (
    AlertSeverity,
    format_task_failure_alert,
    format_data_quality_alert,
    format_sla_miss_alert,
    format_platform_health_alert,
    get_alerting_config,
    send_alert,
    send_email_alert,
    send_slack_alert,
    should_send_alert,
)


class MockSlackHandler(BaseHTTPRequestHandler):
    """Mock HTTP handler for Slack webhook testing."""

    received_payloads = []

    def do_POST(self):
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length)
        MockSlackHandler.received_payloads.append(json.loads(post_data))
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format, *args):
        pass  # Suppress logging


class TestAlertSeverity(unittest.TestCase):
    """Tests for AlertSeverity enum."""

    def test_severity_from_string(self):
        """Test converting string to severity."""
        self.assertEqual(AlertSeverity.from_string("critical"), AlertSeverity.CRITICAL)
        self.assertEqual(AlertSeverity.from_string("HIGH"), AlertSeverity.HIGH)
        self.assertEqual(AlertSeverity.from_string("medium"), AlertSeverity.MEDIUM)
        self.assertEqual(AlertSeverity.from_string("low"), AlertSeverity.LOW)
        self.assertEqual(AlertSeverity.from_string("invalid"), AlertSeverity.MEDIUM)

    def test_severity_comparison(self):
        """Test severity comparison operators."""
        self.assertTrue(AlertSeverity.CRITICAL >= AlertSeverity.HIGH)
        self.assertTrue(AlertSeverity.HIGH >= AlertSeverity.MEDIUM)
        self.assertTrue(AlertSeverity.MEDIUM >= AlertSeverity.LOW)
        self.assertFalse(AlertSeverity.LOW >= AlertSeverity.MEDIUM)

    def test_severity_colors(self):
        """Test severity color codes."""
        self.assertEqual(AlertSeverity.CRITICAL.color, "#FF0000")
        self.assertEqual(AlertSeverity.HIGH.color, "#FF6600")
        self.assertEqual(AlertSeverity.MEDIUM.color, "#FFCC00")
        self.assertEqual(AlertSeverity.LOW.color, "#36A64F")


class TestAlertingConfig(unittest.TestCase):
    """Tests for alerting configuration."""

    def test_config_from_environment(self):
        """Test reading config from environment variables."""
        with patch.dict(os.environ, {
            "ALERT_EMAIL_ENABLED": "true",
            "ALERT_SLACK_ENABLED": "false",
            "SMTP_HOST": "test.smtp.com",
            "ALERT_SEVERITY_THRESHOLD": "HIGH",
        }):
            config = get_alerting_config()
            self.assertTrue(config["email_enabled"])
            self.assertFalse(config["slack_enabled"])
            self.assertEqual(config["smtp_host"], "test.smtp.com")
            self.assertEqual(config["severity_threshold"], AlertSeverity.HIGH)

    def test_should_send_alert_by_severity(self):
        """Test alert filtering by severity threshold."""
        with patch.dict(os.environ, {"ALERT_SEVERITY_THRESHOLD": "HIGH"}):
            self.assertTrue(should_send_alert(AlertSeverity.CRITICAL))
            self.assertTrue(should_send_alert(AlertSeverity.HIGH))
            self.assertFalse(should_send_alert(AlertSeverity.MEDIUM))
            self.assertFalse(should_send_alert(AlertSeverity.LOW))


class TestEmailAlerts(unittest.TestCase):
    """Tests for email alerting functionality."""

    @patch("utils.alerting.smtplib.SMTP")
    def test_send_email_alert_success(self, mock_smtp):
        """Test successful email sending."""
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp.return_value.__exit__ = MagicMock(return_value=False)

        with patch.dict(os.environ, {
            "ALERT_EMAIL_ENABLED": "true",
            "SMTP_HOST": "localhost",
            "SMTP_PORT": "25",
            "ALERT_EMAIL_RECIPIENTS": "test@example.com",
            "ALERT_SEVERITY_THRESHOLD": "LOW",
        }):
            result = send_email_alert(
                subject="Test Alert",
                body="This is a test alert.",
            )

        self.assertTrue(result)
        mock_server.sendmail.assert_called_once()

    def test_send_email_alert_disabled(self):
        """Test email alert when disabled."""
        with patch.dict(os.environ, {"ALERT_EMAIL_ENABLED": "false"}):
            result = send_email_alert(
                subject="Test Alert",
                body="This is a test alert.",
            )
            self.assertFalse(result)

    def test_send_email_alert_no_recipients(self):
        """Test email alert with no recipients."""
        with patch.dict(os.environ, {
            "ALERT_EMAIL_ENABLED": "true",
            "ALERT_EMAIL_RECIPIENTS": "",
        }):
            result = send_email_alert(
                subject="Test Alert",
                body="This is a test alert.",
            )
            self.assertFalse(result)


class TestSlackAlerts(unittest.TestCase):
    """Tests for Slack alerting functionality."""

    @classmethod
    def setUpClass(cls):
        """Start mock Slack server."""
        cls.server = HTTPServer(("localhost", 0), MockSlackHandler)
        cls.server_thread = Thread(target=cls.server.serve_forever)
        cls.server_thread.daemon = True
        cls.server_thread.start()
        cls.webhook_url = f"http://localhost:{cls.server.server_address[1]}/webhook"

    @classmethod
    def tearDownClass(cls):
        """Stop mock Slack server."""
        cls.server.shutdown()

    def setUp(self):
        """Clear received payloads before each test."""
        MockSlackHandler.received_payloads.clear()

    def test_send_slack_alert_success(self):
        """Test successful Slack alert sending."""
        with patch.dict(os.environ, {
            "ALERT_SLACK_ENABLED": "true",
            "SLACK_WEBHOOK_URL": self.webhook_url,
            "ALERT_SEVERITY_THRESHOLD": "LOW",
        }):
            result = send_slack_alert(
                message="Test alert message",
                severity=AlertSeverity.MEDIUM,
                title="Test Alert",
            )

        self.assertTrue(result)
        self.assertEqual(len(MockSlackHandler.received_payloads), 1)
        payload = MockSlackHandler.received_payloads[0]
        self.assertIn("attachments", payload)

    def test_send_slack_alert_disabled(self):
        """Test Slack alert when disabled."""
        with patch.dict(os.environ, {"ALERT_SLACK_ENABLED": "false"}):
            result = send_slack_alert(
                message="Test alert message",
            )
            self.assertFalse(result)

    def test_send_slack_alert_no_webhook(self):
        """Test Slack alert with no webhook URL."""
        with patch.dict(os.environ, {
            "ALERT_SLACK_ENABLED": "true",
            "SLACK_WEBHOOK_URL": "",
        }):
            result = send_slack_alert(
                message="Test alert message",
            )
            self.assertFalse(result)


class TestAlertFormatting(unittest.TestCase):
    """Tests for alert message formatting."""

    def test_format_task_failure_alert(self):
        """Test task failure alert formatting."""
        mock_ti = MagicMock()
        mock_ti.task_id = "test_task"
        mock_ti.dag_id = "test_dag"
        mock_ti.try_number = 2
        mock_ti.log_url = "http://airflow/log"
        # Properly set up task priority_weight for severity check
        mock_ti.task.priority_weight = 5  # Below 10, so severity should be HIGH

        # Don't include dag in context so ti.dag_id is used
        context = {
            "task_instance": mock_ti,
            "execution_date": datetime(2024, 1, 15, 10, 30),
            "exception": ValueError("Test error"),
        }

        alert_data = format_task_failure_alert(context)

        self.assertIn("Task Failure", alert_data["title"])
        self.assertIn("test_dag", alert_data["title"])
        self.assertIn("test_task", alert_data["title"])
        self.assertEqual(alert_data["severity"], AlertSeverity.HIGH)
        self.assertTrue(len(alert_data["fields"]) > 0)

    def test_format_data_quality_alert(self):
        """Test data quality alert formatting."""
        # 3/10 = 30% failure rate -> HIGH severity (> 0.2)
        validation_results = {
            "dataset_name": "transactions",
            "layer": "staging",
            "success": False,
            "total_expectations": 10,
            "failed_expectations": [
                {"expectation_type": "expect_column_to_exist", "message": "Column missing"},
                {"expectation_type": "expect_column_values_to_not_be_null", "message": "Null values found"},
                {"expectation_type": "expect_column_values_to_be_unique", "message": "Duplicates found"},
            ],
        }

        alert_data = format_data_quality_alert(validation_results)

        self.assertIn("Data Quality", alert_data["title"])
        self.assertIn("transactions", alert_data["title"])
        self.assertIn(alert_data["severity"], [AlertSeverity.HIGH, AlertSeverity.CRITICAL])

    def test_format_sla_miss_alert(self):
        """Test SLA miss alert formatting."""
        alert_data = format_sla_miss_alert(
            dag_id="test_dag",
            task_id="test_task",
            sla_time="2024-01-15 10:00:00",
            actual_time="2024-01-15 11:30:00",
        )

        self.assertIn("SLA Miss", alert_data["title"])
        self.assertIn("test_dag", alert_data["title"])
        self.assertEqual(alert_data["severity"], AlertSeverity.HIGH)

    def test_format_platform_health_alert(self):
        """Test platform health alert formatting."""
        alert_data = format_platform_health_alert(
            check_name="Scheduler Health",
            status="unhealthy",
            details="Scheduler heartbeat is 600s old",
            severity=AlertSeverity.CRITICAL,
        )

        self.assertIn("Platform Health", alert_data["title"])
        self.assertIn("Scheduler Health", alert_data["title"])
        self.assertEqual(alert_data["severity"], AlertSeverity.CRITICAL)


class TestAlertSuppression(unittest.TestCase):
    """Tests for alert suppression rules."""

    def test_severity_threshold_suppression(self):
        """Test that alerts below threshold are suppressed."""
        with patch.dict(os.environ, {"ALERT_SEVERITY_THRESHOLD": "HIGH"}):
            self.assertFalse(should_send_alert(AlertSeverity.MEDIUM))
            self.assertFalse(should_send_alert(AlertSeverity.LOW))
            self.assertTrue(should_send_alert(AlertSeverity.HIGH))
            self.assertTrue(should_send_alert(AlertSeverity.CRITICAL))


class TestSendAlert(unittest.TestCase):
    """Tests for the unified send_alert function."""

    @patch("utils.alerting.send_email_alert")
    @patch("utils.alerting.send_slack_alert")
    def test_send_alert_both_channels(self, mock_slack, mock_email):
        """Test sending alert to both channels."""
        mock_email.return_value = True
        mock_slack.return_value = True

        with patch.dict(os.environ, {"ALERT_SEVERITY_THRESHOLD": "LOW"}):
            result = send_alert({
                "title": "Test Alert",
                "message": "Test message",
                "fields": [],
                "severity": AlertSeverity.MEDIUM,
            })

        self.assertTrue(result)
        mock_email.assert_called_once()
        mock_slack.assert_called_once()

    @patch("utils.alerting.send_email_alert")
    @patch("utils.alerting.send_slack_alert")
    def test_send_alert_partial_success(self, mock_slack, mock_email):
        """Test alert with one channel failing."""
        mock_email.return_value = False
        mock_slack.return_value = True

        with patch.dict(os.environ, {"ALERT_SEVERITY_THRESHOLD": "LOW"}):
            result = send_alert({
                "title": "Test Alert",
                "message": "Test message",
                "fields": [],
                "severity": AlertSeverity.MEDIUM,
            })

        self.assertTrue(result)  # At least one channel succeeded


if __name__ == "__main__":
    unittest.main()
