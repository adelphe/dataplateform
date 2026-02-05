"""Slack message templates with rich Block Kit formatting.

This module provides pre-built Slack Block Kit templates for various
alert types used in the data platform.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional


def _truncate(text: str, max_length: int = 300) -> str:
    """Truncate text to max length with ellipsis."""
    if len(text) <= max_length:
        return text
    return text[: max_length - 3] + "..."


def task_failure_blocks(
    dag_id: str,
    task_id: str,
    execution_date: str,
    exception: str,
    try_number: int = 1,
    log_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Generate Slack blocks for task failure alert.

    Args:
        dag_id: DAG identifier
        task_id: Task identifier
        execution_date: Execution date string
        exception: Exception message
        try_number: Current try number
        log_url: Optional URL to task logs

    Returns:
        List of Slack Block Kit blocks
    """
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Task Failure Alert", "emoji": True},
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                {"type": "mrkdwn", "text": f"*Task:*\n`{task_id}`"},
                {"type": "mrkdwn", "text": f"*Execution Date:*\n{execution_date}"},
                {"type": "mrkdwn", "text": f"*Try Number:*\n{try_number}"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Error:*\n```{_truncate(str(exception), 500)}```",
            },
        },
    ]

    if log_url:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Logs"},
                        "url": log_url,
                        "action_id": "view_logs",
                    }
                ],
            }
        )

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Data Platform Alert | {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
                }
            ],
        }
    )

    return blocks


def data_quality_blocks(
    dataset_name: str,
    layer: str,
    total_expectations: int,
    passed_expectations: int,
    failed_expectations: int,
    failed_details: Optional[List[Dict[str, str]]] = None,
    data_docs_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Generate Slack blocks for data quality alert.

    Args:
        dataset_name: Name of the dataset
        layer: Data layer (raw/staging/marts)
        total_expectations: Total number of expectations
        passed_expectations: Number of passed expectations
        failed_expectations: Number of failed expectations
        failed_details: List of failed expectation details
        data_docs_url: Optional URL to Great Expectations Data Docs

    Returns:
        List of Slack Block Kit blocks
    """
    success_rate = (
        (passed_expectations / total_expectations * 100) if total_expectations > 0 else 0
    )

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Data Quality Alert",
                "emoji": True,
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Dataset:*\n`{dataset_name}`"},
                {"type": "mrkdwn", "text": f"*Layer:*\n{layer}"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Total Checks:*\n{total_expectations}"},
                {"type": "mrkdwn", "text": f"*Passed:*\n{passed_expectations}"},
                {"type": "mrkdwn", "text": f"*Failed:*\n{failed_expectations}"},
                {"type": "mrkdwn", "text": f"*Success Rate:*\n{success_rate:.1f}%"},
            ],
        },
    ]

    if failed_details:
        failed_text = "\n".join(
            [f"- `{d.get('expectation_type', 'unknown')}`: {d.get('message', 'N/A')}" for d in failed_details[:5]]
        )
        if len(failed_details) > 5:
            failed_text += f"\n_... and {len(failed_details) - 5} more_"

        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Failed Checks:*\n{failed_text}"},
            }
        )

    if data_docs_url:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Data Docs"},
                        "url": data_docs_url,
                        "action_id": "view_data_docs",
                    }
                ],
            }
        )

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Data Platform Alert | {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
                }
            ],
        }
    )

    return blocks


def sla_miss_blocks(
    dag_id: str,
    task_id: str,
    expected_time: str,
    actual_time: Optional[str] = None,
    delay: Optional[str] = None,
    airflow_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Generate Slack blocks for SLA miss alert.

    Args:
        dag_id: DAG identifier
        task_id: Task identifier
        expected_time: Expected SLA time
        actual_time: Actual completion time (if completed)
        delay: Delay duration string
        airflow_url: Optional URL to Airflow UI

    Returns:
        List of Slack Block Kit blocks
    """
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "SLA Violation", "emoji": True},
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                {"type": "mrkdwn", "text": f"*Task:*\n`{task_id}`"},
                {"type": "mrkdwn", "text": f"*Expected SLA:*\n{expected_time}"},
            ],
        },
    ]

    if actual_time:
        blocks[-1]["fields"].append(
            {"type": "mrkdwn", "text": f"*Completed At:*\n{actual_time}"}
        )

    if delay:
        blocks[-1]["fields"].append(
            {"type": "mrkdwn", "text": f"*Delay:*\n{delay}"}
        )

    if airflow_url:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View in Airflow"},
                        "url": airflow_url,
                        "action_id": "view_airflow",
                    }
                ],
            }
        )

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Data Platform Alert | {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
                }
            ],
        }
    )

    return blocks


def platform_health_blocks(
    health_score: int,
    scheduler_healthy: bool,
    failed_tasks_24h: int,
    sla_compliance: float,
    avg_freshness_hours: float,
    issues: Optional[List[str]] = None,
    dashboard_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Generate Slack blocks for platform health alert.

    Args:
        health_score: Overall health score (0-100)
        scheduler_healthy: Whether scheduler is healthy
        failed_tasks_24h: Number of failed tasks in last 24 hours
        sla_compliance: SLA compliance percentage
        avg_freshness_hours: Average data freshness in hours
        issues: List of detected issues
        dashboard_url: Optional URL to monitoring dashboard

    Returns:
        List of Slack Block Kit blocks
    """
    # Determine status emoji
    if health_score >= 90:
        status_emoji = ":large_green_circle:"
    elif health_score >= 70:
        status_emoji = ":large_yellow_circle:"
    else:
        status_emoji = ":red_circle:"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Platform Health Alert {status_emoji}",
                "emoji": True,
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Health Score:* {health_score}%",
            },
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Scheduler:*\n{'Healthy' if scheduler_healthy else 'Unhealthy'}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Failed Tasks (24h):*\n{failed_tasks_24h}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*SLA Compliance:*\n{sla_compliance:.1f}%",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Avg Freshness:*\n{avg_freshness_hours:.1f}h",
                },
            ],
        },
    ]

    if issues:
        issues_text = "\n".join([f"- {issue}" for issue in issues[:5]])
        if len(issues) > 5:
            issues_text += f"\n_... and {len(issues) - 5} more_"

        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Issues:*\n{issues_text}"},
            }
        )

    if dashboard_url:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Dashboard"},
                        "url": dashboard_url,
                        "action_id": "view_dashboard",
                    }
                ],
            }
        )

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Data Platform Monitoring | {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
                }
            ],
        }
    )

    return blocks


def alert_summary_blocks(
    alert_count: int,
    time_window_minutes: int,
    alerts_by_severity: Dict[str, int],
    alerts_by_type: Dict[str, int],
) -> List[Dict[str, Any]]:
    """Generate Slack blocks for aggregated alert summary.

    Args:
        alert_count: Total number of alerts in window
        time_window_minutes: Aggregation window in minutes
        alerts_by_severity: Count of alerts by severity level
        alerts_by_type: Count of alerts by type

    Returns:
        List of Slack Block Kit blocks
    """
    severity_text = "\n".join(
        [f"- {severity}: {count}" for severity, count in alerts_by_severity.items()]
    )
    type_text = "\n".join(
        [f"- {alert_type}: {count}" for alert_type, count in alerts_by_type.items()]
    )

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Alert Summary ({alert_count} alerts)",
                "emoji": True,
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{alert_count} alerts* generated in the last *{time_window_minutes} minutes*.",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*By Severity:*\n{severity_text}"},
                {"type": "mrkdwn", "text": f"*By Type:*\n{type_text}"},
            ],
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Alerts have been aggregated to reduce noise. | {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
                }
            ],
        },
    ]

    return blocks
