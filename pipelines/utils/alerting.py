"""
Alerting utilities for the data platform.

Provides notification functions for email and Slack alerts with support for
different severity levels, retry logic, and formatted messages.
"""

import json
import logging
import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any, Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels with associated colors for Slack."""
    CRITICAL = ("critical", "#FF0000")  # Red
    HIGH = ("high", "#FF6600")  # Orange
    MEDIUM = ("medium", "#FFCC00")  # Yellow
    LOW = ("low", "#36A64F")  # Green

    def __init__(self, level: str, color: str):
        self.level = level
        self.color = color

    @classmethod
    def from_string(cls, level: str) -> "AlertSeverity":
        """Convert string to AlertSeverity enum."""
        level_map = {
            "critical": cls.CRITICAL,
            "high": cls.HIGH,
            "medium": cls.MEDIUM,
            "low": cls.LOW,
        }
        return level_map.get(level.lower(), cls.MEDIUM)

    def __ge__(self, other: "AlertSeverity") -> bool:
        order = [AlertSeverity.LOW, AlertSeverity.MEDIUM, AlertSeverity.HIGH, AlertSeverity.CRITICAL]
        return order.index(self) >= order.index(other)


def get_alerting_config() -> Dict[str, Any]:
    """Get alerting configuration from environment variables."""
    return {
        "email_enabled": os.getenv("ALERT_EMAIL_ENABLED", "false").lower() == "true",
        "slack_enabled": os.getenv("ALERT_SLACK_ENABLED", "false").lower() == "true",
        "smtp_host": os.getenv("SMTP_HOST", "localhost"),
        "smtp_port": int(os.getenv("SMTP_PORT", "587")),
        "smtp_user": os.getenv("SMTP_USER", ""),
        "smtp_password": os.getenv("SMTP_PASSWORD", ""),
        "smtp_from_email": os.getenv("SMTP_FROM_EMAIL", "alerts@dataplatform.local"),
        "email_recipients": [
            r.strip() for r in os.getenv("ALERT_EMAIL_RECIPIENTS", "").split(",") if r.strip()
        ],
        "slack_webhook_url": os.getenv("SLACK_WEBHOOK_URL", ""),
        "slack_channel": os.getenv("SLACK_ALERT_CHANNEL", "#data-platform-alerts"),
        "severity_threshold": AlertSeverity.from_string(
            os.getenv("ALERT_SEVERITY_THRESHOLD", "MEDIUM")
        ),
    }


def should_send_alert(severity: AlertSeverity) -> bool:
    """Check if alert should be sent based on severity threshold."""
    config = get_alerting_config()
    return severity >= config["severity_threshold"]


def send_email_alert(
    subject: str,
    body: str,
    recipients: Optional[List[str]] = None,
    html_body: Optional[str] = None,
    max_retries: int = 3,
    retry_delay: float = 5.0,
) -> bool:
    """
    Send email alert using SMTP.

    Args:
        subject: Email subject line
        body: Plain text email body
        recipients: List of recipient email addresses (uses config default if None)
        html_body: Optional HTML version of the email body
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds

    Returns:
        True if email was sent successfully, False otherwise
    """
    config = get_alerting_config()

    if not config["email_enabled"]:
        logger.debug("Email alerts are disabled")
        return False

    recipients = recipients or config["email_recipients"]
    if not recipients:
        logger.warning("No email recipients configured")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[Data Platform Alert] {subject}"
    msg["From"] = config["smtp_from_email"]
    msg["To"] = ", ".join(recipients)

    msg.attach(MIMEText(body, "plain"))
    if html_body:
        msg.attach(MIMEText(html_body, "html"))

    for attempt in range(max_retries):
        try:
            with smtplib.SMTP(config["smtp_host"], config["smtp_port"]) as server:
                if config["smtp_user"] and config["smtp_password"]:
                    server.starttls()
                    server.login(config["smtp_user"], config["smtp_password"])
                server.sendmail(config["smtp_from_email"], recipients, msg.as_string())
            logger.info(f"Email alert sent successfully: {subject}")
            return True
        except smtplib.SMTPException as e:
            logger.warning(f"Failed to send email (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
        except Exception as e:
            logger.error(f"Unexpected error sending email: {e}")
            break

    logger.error(f"Failed to send email alert after {max_retries} attempts: {subject}")
    return False


def send_slack_alert(
    message: str,
    channel: Optional[str] = None,
    severity: AlertSeverity = AlertSeverity.MEDIUM,
    title: Optional[str] = None,
    fields: Optional[List[Dict[str, str]]] = None,
    max_retries: int = 3,
    retry_delay: float = 5.0,
) -> bool:
    """
    Send alert to Slack webhook with formatted message.

    Args:
        message: Main alert message
        channel: Slack channel (uses config default if None)
        severity: Alert severity level for color coding
        title: Optional title for the alert
        fields: Optional list of field dicts with 'title' and 'value' keys
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds

    Returns:
        True if message was posted successfully, False otherwise
    """
    config = get_alerting_config()

    if not config["slack_enabled"]:
        logger.debug("Slack alerts are disabled")
        return False

    webhook_url = config["slack_webhook_url"]
    if not webhook_url:
        logger.warning("No Slack webhook URL configured")
        return False

    channel = channel or config["slack_channel"]

    # Build Slack attachment with color based on severity
    attachment = {
        "color": severity.color,
        "text": message,
        "footer": "Data Platform Monitoring",
        "ts": int(time.time()),
    }

    if title:
        attachment["title"] = title

    if fields:
        attachment["fields"] = [
            {"title": f["title"], "value": f["value"], "short": f.get("short", True)}
            for f in fields
        ]

    payload = {
        "channel": channel,
        "attachments": [attachment],
        "username": "Data Platform Alerts",
        "icon_emoji": _get_severity_emoji(severity),
    }

    for attempt in range(max_retries):
        try:
            data = json.dumps(payload).encode("utf-8")
            req = Request(
                webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
            )
            with urlopen(req, timeout=30) as response:
                if response.status == 200:
                    logger.info(f"Slack alert sent successfully: {title or message[:50]}")
                    return True
        except HTTPError as e:
            logger.warning(f"Slack webhook HTTP error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
        except URLError as e:
            logger.warning(f"Slack webhook URL error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
        except Exception as e:
            logger.error(f"Unexpected error sending Slack alert: {e}")
            break

    logger.error(f"Failed to send Slack alert after {max_retries} attempts")
    return False


def _get_severity_emoji(severity: AlertSeverity) -> str:
    """Get emoji for severity level."""
    emoji_map = {
        AlertSeverity.CRITICAL: ":rotating_light:",
        AlertSeverity.HIGH: ":warning:",
        AlertSeverity.MEDIUM: ":large_yellow_circle:",
        AlertSeverity.LOW: ":information_source:",
    }
    return emoji_map.get(severity, ":bell:")


def format_task_failure_alert(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format Airflow task failure context into alert message.

    Args:
        context: Airflow task instance context dictionary

    Returns:
        Dictionary with formatted alert components (title, message, fields, severity)
    """
    ti = context.get("task_instance")
    dag_id = context.get("dag", ti.dag_id if ti else "unknown")
    task_id = ti.task_id if ti else context.get("task_id", "unknown")
    execution_date = context.get("execution_date", "unknown")
    exception = context.get("exception", "No exception details available")
    log_url = ti.log_url if ti else "N/A"

    # Determine severity based on task criticality
    severity = AlertSeverity.HIGH
    if hasattr(ti, "task") and hasattr(ti.task, "priority_weight"):
        if ti.task.priority_weight > 10:
            severity = AlertSeverity.CRITICAL

    title = f"Task Failure: {dag_id}.{task_id}"
    message = f"Task `{task_id}` in DAG `{dag_id}` has failed."

    fields = [
        {"title": "DAG", "value": str(dag_id), "short": True},
        {"title": "Task", "value": str(task_id), "short": True},
        {"title": "Execution Date", "value": str(execution_date), "short": True},
        {"title": "Try Number", "value": str(ti.try_number if ti else "N/A"), "short": True},
        {"title": "Error", "value": str(exception)[:500], "short": False},
        {"title": "Log URL", "value": str(log_url), "short": False},
    ]

    return {
        "title": title,
        "message": message,
        "fields": fields,
        "severity": severity,
    }


def format_data_quality_alert(validation_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format data quality validation failures into alert message.

    Args:
        validation_results: Dictionary containing validation results

    Returns:
        Dictionary with formatted alert components
    """
    dataset = validation_results.get("dataset_name", "unknown")
    layer = validation_results.get("layer", "unknown")
    success = validation_results.get("success", False)
    failed_expectations = validation_results.get("failed_expectations", [])
    total_expectations = validation_results.get("total_expectations", 0)
    failed_count = len(failed_expectations)

    # Determine severity based on failure rate and layer
    failure_rate = failed_count / total_expectations if total_expectations > 0 else 0
    if layer == "marts" or failure_rate > 0.5:
        severity = AlertSeverity.CRITICAL
    elif failure_rate > 0.2:
        severity = AlertSeverity.HIGH
    else:
        severity = AlertSeverity.MEDIUM

    title = f"Data Quality Alert: {dataset}"
    message = f"Data quality checks failed for `{dataset}` in `{layer}` layer."

    # Format failed expectations
    failed_details = "\n".join(
        [f"- {exp.get('expectation_type', 'unknown')}: {exp.get('message', 'N/A')}"
         for exp in failed_expectations[:5]]
    )
    if len(failed_expectations) > 5:
        failed_details += f"\n... and {len(failed_expectations) - 5} more"

    fields = [
        {"title": "Dataset", "value": dataset, "short": True},
        {"title": "Layer", "value": layer, "short": True},
        {"title": "Total Checks", "value": str(total_expectations), "short": True},
        {"title": "Failed Checks", "value": str(failed_count), "short": True},
        {"title": "Failure Rate", "value": f"{failure_rate:.1%}", "short": True},
        {"title": "Failed Expectations", "value": failed_details or "N/A", "short": False},
    ]

    return {
        "title": title,
        "message": message,
        "fields": fields,
        "severity": severity,
    }


def format_sla_miss_alert(
    dag_id: str,
    task_id: str,
    sla_time: str,
    actual_time: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Format SLA violation alert.

    Args:
        dag_id: DAG identifier
        task_id: Task identifier
        sla_time: Expected SLA completion time
        actual_time: Actual completion time (if available)

    Returns:
        Dictionary with formatted alert components
    """
    title = f"SLA Miss: {dag_id}.{task_id}"
    message = f"SLA was missed for task `{task_id}` in DAG `{dag_id}`."

    fields = [
        {"title": "DAG", "value": dag_id, "short": True},
        {"title": "Task", "value": task_id, "short": True},
        {"title": "Expected SLA", "value": str(sla_time), "short": True},
    ]

    if actual_time:
        fields.append({"title": "Actual Time", "value": str(actual_time), "short": True})

    return {
        "title": title,
        "message": message,
        "fields": fields,
        "severity": AlertSeverity.HIGH,
    }


def format_platform_health_alert(
    check_name: str,
    status: str,
    details: str,
    severity: AlertSeverity = AlertSeverity.MEDIUM,
) -> Dict[str, Any]:
    """
    Format platform health check alert.

    Args:
        check_name: Name of the health check
        status: Current status (healthy, degraded, unhealthy)
        details: Additional details about the issue
        severity: Alert severity level

    Returns:
        Dictionary with formatted alert components
    """
    title = f"Platform Health: {check_name}"
    message = f"Health check `{check_name}` reported status: {status}"

    fields = [
        {"title": "Check", "value": check_name, "short": True},
        {"title": "Status", "value": status, "short": True},
        {"title": "Details", "value": details, "short": False},
    ]

    return {
        "title": title,
        "message": message,
        "fields": fields,
        "severity": severity,
    }


def _log_alert_to_history(
    alert_data: Dict[str, Any],
    email_sent: bool,
    slack_sent: bool,
    suppressed: bool = False,
    suppression_reason: Optional[str] = None,
) -> None:
    """
    Log alert to alert_history table for audit and dashboard display.

    This function should not raise exceptions to avoid blocking alert delivery.

    Args:
        alert_data: Dictionary with alert details
        email_sent: Whether email was successfully sent
        slack_sent: Whether Slack was successfully sent
        suppressed: Whether the alert was suppressed
        suppression_reason: Reason for suppression if applicable
    """
    try:
        from airflow.hooks.postgres_hook import PostgresHook

        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        severity = alert_data.get("severity", AlertSeverity.MEDIUM)
        severity_str = severity.level if isinstance(severity, AlertSeverity) else str(severity)

        # Extract DAG/task info from fields if available
        dag_id = None
        task_id = None
        execution_date = None

        for field in alert_data.get("fields", []):
            field_title = field.get("title", "").lower()
            field_value = field.get("value")
            if field_title == "dag":
                dag_id = field_value
            elif field_title == "task":
                task_id = field_value
            elif field_title == "execution date" or field_title == "execution_date":
                execution_date = field_value

        # Determine alert type from title
        title = alert_data.get("title", "")
        if "Task Failure" in title:
            alert_type = "task_failure"
        elif "Data Quality" in title:
            alert_type = "data_quality"
        elif "SLA" in title:
            alert_type = "sla_miss"
        elif "Platform Health" in title:
            alert_type = "platform_health"
        elif "Freshness" in title:
            alert_type = "data_freshness"
        elif "Volume" in title:
            alert_type = "volume_anomaly"
        else:
            alert_type = "general"

        # Build channels notified list
        channels_notified = []
        if email_sent:
            channels_notified.append("email")
        if slack_sent:
            channels_notified.append("slack")

        # Build metadata from fields
        metadata = {}
        for field in alert_data.get("fields", []):
            field_title = field.get("title", "").lower().replace(" ", "_")
            if field_title not in ("dag", "task", "execution_date"):
                metadata[field_title] = field.get("value")

        cursor.execute("""
            INSERT INTO data_platform.alert_history (
                alert_type, severity, subject, message,
                dag_id, task_id, execution_date,
                channels_notified, email_sent, slack_sent,
                suppressed, suppression_reason, metadata
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
        """, (
            alert_type,
            severity_str,
            title[:500],  # Truncate to fit column
            alert_data.get("message", ""),
            dag_id,
            task_id,
            execution_date,
            json.dumps(channels_notified),
            email_sent,
            slack_sent,
            suppressed,
            suppression_reason,
            json.dumps(metadata),
        ))

        conn.commit()
        cursor.close()
        conn.close()

        logger.debug(f"Logged alert to history: {title}")

    except Exception as e:
        # Log error but don't raise - alert delivery should not be blocked
        logger.warning(f"Failed to log alert to history (non-blocking): {e}")


def send_alert(
    alert_data: Dict[str, Any],
    include_email: bool = True,
    include_slack: bool = True,
) -> bool:
    """
    Send alert through configured channels.

    Args:
        alert_data: Dictionary with title, message, fields, and severity
        include_email: Whether to send email alert
        include_slack: Whether to send Slack alert

    Returns:
        True if at least one channel succeeded
    """
    severity = alert_data.get("severity", AlertSeverity.MEDIUM)

    if not should_send_alert(severity):
        logger.debug(f"Alert below severity threshold: {alert_data.get('title')}")
        # Log suppressed alert to history
        _log_alert_to_history(
            alert_data,
            email_sent=False,
            slack_sent=False,
            suppressed=True,
            suppression_reason="Below severity threshold",
        )
        return False

    success = False
    slack_result = False
    email_result = False

    if include_slack:
        slack_result = send_slack_alert(
            message=alert_data.get("message", ""),
            severity=severity,
            title=alert_data.get("title"),
            fields=alert_data.get("fields"),
        )
        success = success or slack_result

    if include_email:
        # Format plain text body from fields
        body_parts = [alert_data.get("message", ""), ""]
        for field in alert_data.get("fields", []):
            body_parts.append(f"{field['title']}: {field['value']}")

        email_result = send_email_alert(
            subject=alert_data.get("title", "Data Platform Alert"),
            body="\n".join(body_parts),
        )
        success = success or email_result

    # Log alert to history (non-blocking)
    _log_alert_to_history(
        alert_data,
        email_sent=email_result,
        slack_sent=slack_result,
        suppressed=False,
    )

    return success


def task_failure_callback(context: Dict[str, Any]) -> None:
    """
    Airflow callback for task failures.
    Can be used directly as on_failure_callback in DAG/task definitions.

    Args:
        context: Airflow task instance context
    """
    alert_data = format_task_failure_alert(context)
    send_alert(alert_data)


def sla_miss_callback(
    dag: Any,
    task_list: str,
    blocking_task_list: str,
    slas: List[Any],
    blocking_tis: List[Any],
) -> None:
    """
    Airflow callback for SLA misses.
    Can be used as sla_miss_callback in DAG definitions.

    Args:
        dag: DAG object
        task_list: List of tasks that missed SLA
        blocking_task_list: List of blocking tasks
        slas: List of SLA objects
        blocking_tis: List of blocking task instances
    """
    for sla in slas:
        alert_data = format_sla_miss_alert(
            dag_id=dag.dag_id,
            task_id=sla.task_id,
            sla_time=str(sla.sla),
        )
        send_alert(alert_data)
