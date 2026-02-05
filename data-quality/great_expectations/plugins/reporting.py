"""
Data Quality Reporting utilities.

Provides functions for generating reports from Great Expectations
validation results, including summary reports, trend analysis,
and alert notifications.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


def generate_validation_summary(
    validation_results: List[Dict[str, Any]],
    include_details: bool = True,
) -> str:
    """Generate a text summary of validation results.

    Args:
        validation_results: List of validation result dictionaries.
        include_details: Whether to include per-suite details.

    Returns:
        Formatted summary string.
    """
    if not validation_results:
        return "No validation results to summarize."

    total = len(validation_results)
    passed = sum(1 for r in validation_results if r.get("success", False))
    failed = total - passed

    total_expectations = sum(r.get("total_expectations", 0) for r in validation_results)
    passed_expectations = sum(
        r.get("successful_expectations", 0) for r in validation_results
    )

    lines = [
        "=" * 60,
        " DATA QUALITY VALIDATION SUMMARY",
        "=" * 60,
        "",
        f" Validation Suites:  {total}",
        f"   Passed:           {passed}",
        f"   Failed:           {failed}",
        f"   Success Rate:     {passed / total * 100:.1f}%",
        "",
        f" Total Expectations: {total_expectations}",
        f"   Passed:           {passed_expectations}",
        f"   Failed:           {total_expectations - passed_expectations}",
        f"   Success Rate:     {passed_expectations / total_expectations * 100:.1f}%"
        if total_expectations > 0
        else "   Success Rate:     N/A",
        "",
    ]

    if include_details and failed > 0:
        lines.extend(["", " FAILED VALIDATIONS", "-" * 60])
        for result in validation_results:
            if not result.get("success", True):
                suite_name = result.get("expectation_suite_name", "Unknown")
                exp_passed = result.get("successful_expectations", 0)
                exp_total = result.get("total_expectations", 0)
                lines.append(f"   {suite_name}: {exp_passed}/{exp_total} passed")

                # Show failed expectations if available
                failed_exps = result.get("failed_expectations", [])
                for exp in failed_exps[:3]:  # Limit to 3
                    lines.append(f"     - {exp.get('expectation_type', 'unknown')}")

    lines.extend(["", "=" * 60])
    return "\n".join(lines)


def generate_html_report(
    validation_results: List[Dict[str, Any]],
    title: str = "Data Quality Report",
) -> str:
    """Generate an HTML report of validation results.

    Args:
        validation_results: List of validation result dictionaries.
        title: Report title.

    Returns:
        HTML report string.
    """
    total = len(validation_results)
    passed = sum(1 for r in validation_results if r.get("success", False))
    failed = total - passed

    total_expectations = sum(r.get("total_expectations", 0) for r in validation_results)
    passed_expectations = sum(
        r.get("successful_expectations", 0) for r in validation_results
    )

    # Group by layer
    layers = {"raw": [], "staging": [], "marts": [], "other": []}
    for result in validation_results:
        suite_name = result.get("expectation_suite_name", "").lower()
        if "raw" in suite_name:
            layers["raw"].append(result)
        elif "staging" in suite_name:
            layers["staging"].append(result)
        elif "mart" in suite_name:
            layers["marts"].append(result)
        else:
            layers["other"].append(result)

    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        h1 {{
            color: #333;
            border-bottom: 2px solid #007bff;
            padding-bottom: 10px;
        }}
        .summary {{
            display: flex;
            gap: 20px;
            margin-bottom: 30px;
        }}
        .summary-card {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            flex: 1;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .summary-card h3 {{
            margin-top: 0;
            color: #666;
            font-size: 14px;
            text-transform: uppercase;
        }}
        .summary-card .value {{
            font-size: 36px;
            font-weight: bold;
        }}
        .success {{ color: #28a745; }}
        .failure {{ color: #dc3545; }}
        .warning {{ color: #ffc107; }}
        .layer-section {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .layer-section h2 {{
            margin-top: 0;
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .layer-badge {{
            padding: 4px 12px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: normal;
        }}
        .badge-raw {{ background: #e3f2fd; color: #1976d2; }}
        .badge-staging {{ background: #fff3e0; color: #f57c00; }}
        .badge-marts {{ background: #e8f5e9; color: #388e3c; }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            text-align: left;
            padding: 12px;
            border-bottom: 1px solid #eee;
        }}
        th {{
            background: #f8f9fa;
            font-weight: 600;
        }}
        .status-badge {{
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 600;
        }}
        .status-passed {{ background: #d4edda; color: #155724; }}
        .status-failed {{ background: #f8d7da; color: #721c24; }}
        .progress-bar {{
            background: #e9ecef;
            border-radius: 4px;
            height: 8px;
            overflow: hidden;
        }}
        .progress-fill {{
            height: 100%;
            transition: width 0.3s;
        }}
        .timestamp {{
            color: #666;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>
        <p class="timestamp">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>

        <div class="summary">
            <div class="summary-card">
                <h3>Validation Suites</h3>
                <div class="value {'success' if failed == 0 else 'failure'}">{passed}/{total}</div>
                <p>Passed</p>
            </div>
            <div class="summary-card">
                <h3>Expectations</h3>
                <div class="value">{passed_expectations}/{total_expectations}</div>
                <p>Passed ({passed_expectations / total_expectations * 100:.1f}% success rate)</p>
            </div>
            <div class="summary-card">
                <h3>Status</h3>
                <div class="value {'success' if failed == 0 else 'failure'}">
                    {'HEALTHY' if failed == 0 else 'ISSUES DETECTED'}
                </div>
                <p>{failed} suite(s) with failures</p>
            </div>
        </div>
"""

    # Add layer sections
    layer_config = {
        "raw": ("Raw / Bronze Layer", "badge-raw"),
        "staging": ("Staging / Silver Layer", "badge-staging"),
        "marts": ("Marts / Gold Layer", "badge-marts"),
        "other": ("Other", ""),
    }

    for layer_key, results in layers.items():
        if not results:
            continue

        layer_name, badge_class = layer_config[layer_key]
        layer_passed = sum(1 for r in results if r.get("success", False))

        html += f"""
        <div class="layer-section">
            <h2>
                {layer_name}
                <span class="layer-badge {badge_class}">
                    {layer_passed}/{len(results)} passed
                </span>
            </h2>
            <table>
                <thead>
                    <tr>
                        <th>Expectation Suite</th>
                        <th>Status</th>
                        <th>Expectations</th>
                        <th>Success Rate</th>
                    </tr>
                </thead>
                <tbody>
"""
        for result in results:
            suite_name = result.get("expectation_suite_name", "Unknown")
            success = result.get("success", False)
            exp_total = result.get("total_expectations", 0)
            exp_passed = result.get("successful_expectations", 0)
            success_pct = result.get("success_percent", 0)

            status_class = "status-passed" if success else "status-failed"
            status_text = "PASSED" if success else "FAILED"
            bar_color = "#28a745" if success else "#dc3545"

            html += f"""
                    <tr>
                        <td>{suite_name}</td>
                        <td><span class="status-badge {status_class}">{status_text}</span></td>
                        <td>{exp_passed} / {exp_total}</td>
                        <td>
                            <div class="progress-bar">
                                <div class="progress-fill" style="width: {success_pct}%; background: {bar_color};"></div>
                            </div>
                            <span>{success_pct:.1f}%</span>
                        </td>
                    </tr>
"""

        html += """
                </tbody>
            </table>
        </div>
"""

    html += """
    </div>
</body>
</html>
"""
    return html


def format_slack_notification(
    validation_results: List[Dict[str, Any]],
    run_id: str,
    checkpoint_name: str,
) -> Dict[str, Any]:
    """Format validation results as a Slack message payload.

    Args:
        validation_results: List of validation result dictionaries.
        run_id: Validation run identifier.
        checkpoint_name: Name of the checkpoint.

    Returns:
        Slack message payload dictionary.
    """
    total = len(validation_results)
    passed = sum(1 for r in validation_results if r.get("success", False))
    failed = total - passed

    if failed == 0:
        color = "#28a745"
        title = "Data Quality Check Passed"
        emoji = ":white_check_mark:"
    else:
        color = "#dc3545"
        title = "Data Quality Check Failed"
        emoji = ":x:"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} {title}",
            },
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Checkpoint:*\n{checkpoint_name}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Run ID:*\n{run_id}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Suites Passed:*\n{passed}/{total}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Status:*\n{'All Passed' if failed == 0 else f'{failed} Failed'}",
                },
            ],
        },
    ]

    # Add failed suite details
    if failed > 0:
        failed_suites = [r for r in validation_results if not r.get("success", True)]
        failed_text = "\n".join(
            f"- {r.get('expectation_suite_name', 'Unknown')}: "
            f"{r.get('successful_expectations', 0)}/{r.get('total_expectations', 0)} passed"
            for r in failed_suites[:5]
        )

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Failed Suites:*\n{failed_text}",
            },
        })

    return {
        "attachments": [{"color": color, "blocks": blocks}],
    }


def calculate_quality_trends(
    results: List[Dict[str, Any]],
    group_by: str = "day",
) -> Dict[str, List[Dict[str, Any]]]:
    """Calculate quality trends over time.

    Args:
        results: List of historical validation results with timestamps.
        group_by: Time grouping ('day', 'week', 'month').

    Returns:
        Dictionary with trend data for each suite.
    """
    trends: Dict[str, List[Dict[str, Any]]] = {}

    for result in results:
        suite_name = result.get("expectation_suite_name", "unknown")
        validation_time = result.get("validation_time")

        if suite_name not in trends:
            trends[suite_name] = []

        trends[suite_name].append({
            "timestamp": validation_time,
            "success": result.get("success", False),
            "success_percent": result.get("success_percent", 0),
            "total_expectations": result.get("total_expectations", 0),
            "successful_expectations": result.get("successful_expectations", 0),
        })

    # Sort each suite's data by timestamp
    for suite_name in trends:
        trends[suite_name].sort(
            key=lambda x: x["timestamp"] if x["timestamp"] else datetime.min
        )

    return trends
