#!/usr/bin/env python3
"""
Meta Ads Anomaly Detector
Monitors pixel health and conversion event anomalies across Meta Ad accounts.
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta

import requests
import time

# Configuration
META_API_BASE = "https://graph.facebook.com/v25.0"
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
CONFIG_FILE = "config.json"
OUTPUT_FILE = "docs/index.html"

if not META_ACCESS_TOKEN:
    print("ERROR: META_ACCESS_TOKEN environment variable not set")
    sys.exit(1)

# Global counters for summary
total_accounts = 0
alert_count = 0
all_clear_count = 0
no_spend_count = 0


def get_yesterday_date():
    """Return yesterday's date as YYYY-MM-DD."""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def get_7days_ago_date():
    """Return date 7 days ago as YYYY-MM-DD."""
    seven_days_ago = datetime.now() - timedelta(days=7)
    return seven_days_ago.strftime("%Y-%m-%d")


def api_call(endpoint, params=None):
    """Make API call with error handling and rate limiting."""
    if params is None:
        params = {}
    params["access_token"] = META_ACCESS_TOKEN
    url = f"{META_API_BASE}/{endpoint}"
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        # Verbose logging for errors
        if "error" in data:
            print(f"API Error Response for {endpoint}: {data['error']}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"API Error for {endpoint}: Status {response.status_code if 'response' in locals() else 'N/A'}")
        print(f"  Exception: {e}")
        if 'response' in locals():
            try:
                error_data = response.json()
                if "error" in error_data:
                    print(f"  Error details: {error_data['error']}")
            except:
                pass
        return None
    finally:
        time.sleep(0.1)  # Rate limiting: 100ms between calls


def get_pixel_id(account_id):
    """Fetch the first pixel ID for an account."""
    endpoint = f"{account_id}/adspixels"
    params = {"fields": "id,name,last_fired_time"}
    data = api_call(endpoint, params)
    if data and "data" in data and len(data["data"]) > 0:
        print(f"  Pixels found for {account_id}: {len(data['data'])} pixel(s)")
        return data["data"][0]
    else:
        print(f"  No pixels found for {account_id}")
        return None


def check_pixel_health(pixel_id):
    """Check if pixel has fired in the last 24 hours."""
    endpoint = str(pixel_id)
    params = {"fields": "last_fired_time"}
    data = api_call(endpoint, params)

    if not data or "last_fired_time" not in data:
        return None, "Unknown"

    last_fired = data.get("last_fired_time")
    if not last_fired:
        return None, "Never Fired"

    # Parse last_fired_time
    try:
        # Meta API returns +0000 format; fromisoformat needs +00:00
        cleaned = re.sub(r'([+-]\d{2})(\d{2})$', r'\1:\2', last_fired).replace("Z", "+00:00")
        fired_time = datetime.fromisoformat(cleaned)
        hours_ago = (datetime.now(fired_time.tzinfo) - fired_time).total_seconds() / 3600

        if hours_ago > 24:
            return hours_ago, "STALE"
        else:
            return hours_ago, "HEALTHY"
    except Exception as e:
        print(f"Error parsing pixel time: {e}")
        return None, "Error"


def get_insights(account_id, date_start, date_end, event_types):
    """Fetch insights for a given date range and event types."""
    endpoint = f"{account_id}/insights"
    params = {
        "time_range": json.dumps({"since": date_start, "until": date_end}),
        "fields": "actions,spend"
    }
    data = api_call(endpoint, params)

    if not data or "data" not in data:
        print(f"  Insights {account_id}: spend=0, events=0")
        return 0

    total_actions = 0
    total_spend = 0
    for insight in data["data"]:
        if "spend" in insight:
            try:
                total_spend += float(insight["spend"])
            except:
                pass
        if "actions" in insight:
            for action in insight["actions"]:
                if action.get("action_type") in (event_types if isinstance(event_types, list) else [event_types]):
                    total_actions += int(action.get("value", 0))

    print(f"  Insights {account_id}: spend={total_spend}, events={total_actions}")
    return total_actions


def analyze_account(account):
    """Analyze a single account and return status info."""
    global alert_count, all_clear_count, no_spend_count

    name = account.get("name")
    account_id = account.get("account_id")
    account_type = account.get("type")
    skip_pixel = account.get("skip_pixel_check", False)

    yesterday = get_yesterday_date()
    seven_days_ago = get_7days_ago_date()

    # Get event types to track
    if account_type == "ecommerce":
        event_types = [account.get("event_type")]
    else:
        event_types = account.get("event_types", ["lead"])

    # Check pixel health (skip for JFS Surrogacy)
    pixel_status = "N/A"
    pixel_health = "N/A"
    if not skip_pixel:
        pixel_data = get_pixel_id(account_id)
        if pixel_data:
            pixel_id = pixel_data.get("id")
            hours_ago, health_status = check_pixel_health(pixel_id)
            pixel_status = pixel_id
            pixel_health = health_status
            if health_status == "STALE":
                pixel_health = f"STALE ({int(hours_ago)}h)"
        else:
            pixel_health = "NO PIXEL"
            pixel_status = "NO PIXEL"

    # Get conversion events for yesterday and 7-day average
    yesterday_events = get_insights(account_id, yesterday, yesterday, event_types)

    # Get 7-day data and compute average
    seven_day_events = get_insights(account_id, seven_days_ago, yesterday, event_types)
    avg_daily_events = seven_day_events / 7 if seven_day_events > 0 else 0

    # Determine status and alert
    status = "Healthy"
    alert = ""

    if yesterday_events == 0 and avg_daily_events == 0:
        # No spend and no history = no alerts needed
        status = "No Activity"
        no_spend_count += 1
    elif yesterday_events == 0 and avg_daily_events > 0:
        # Zero events but expecting them = CRITICAL
        status = "Critical"
        alert = "ZERO EVENTS"
        alert_count += 1
    elif yesterday_events < (avg_daily_events * 0.5) and avg_daily_events > 0:
        # Less than 50% of average = WARNING
        status = "Warning"
        alert = f"DOWN {int(100 - (yesterday_events / avg_daily_events * 100))}%"
        alert_count += 1
    else:
        all_clear_count += 1

    # Pixel status alert
    pixel_alert = ""
    if pixel_health.startswith("STALE"):
        status = "Critical"
        pixel_alert = "PIXEL DEAD"
        alert_count += 1
    elif pixel_health == "NO PIXEL":
        pixel_alert = "NO PIXEL"
        if status == "Healthy":
            status = "Warning"
            alert_count += 1

    print(f"Account {name}: status={status}, alert={alert if alert else 'None'}")

    return {
        "name": name,
        "account_type": account_type,
        "pixel_status": pixel_status if pixel_status != "N/A" else "N/A",
        "pixel_health": pixel_health,
        "pixel_alert": pixel_alert,
        "yesterday_events": yesterday_events,
        "seven_day_avg": round(avg_daily_events, 1),
        "status": status,
        "alert": alert,
        "sort_key": 0 if status == "Critical" else (1 if status == "Warning" else (3 if status == "Healthy" else 4))
    }


def generate_html(results):
    """Generate the HTML dashboard."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S ET")

    # Sort by status (alerts first, then warnings, then healthy)
    sorted_results = sorted(results, key=lambda x: (x["sort_key"], x["name"]))

    # Count statuses
    critical_count = sum(1 for r in sorted_results if r["status"] == "Critical")
    warning_count = sum(1 for r in sorted_results if r["status"] == "Warning")
    healthy_count = sum(1 for r in sorted_results if r["status"] == "Healthy")
    no_activity_count = sum(1 for r in sorted_results if r["status"] == "No Activity")
    no_spend_count_val = sum(1 for r in sorted_results if r["status"] == "No Spend")

    html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TWM Marketing - Meta Ads Anomaly Detector</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.3);
            overflow: hidden;
        }
        header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px 30px;
            text-align: center;
        }
        header h1 { font-size: 32px; margin-bottom: 10px; font-weight: 600; }
        header p { font-size: 14px; opacity: 0.9; }
        .summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 20px;
            padding: 30px;
            background: #f8f9fa;
            border-bottom: 1px solid #e0e0e0;
        }
        .summary-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
            border-left: 4px solid #667eea;
        }
        .summary-card.critical { border-left-color: #dc3545; }
        .summary-card.warning { border-left-color: #ffc107; }
        .summary-card.healthy { border-left-color: #28a745; }
        .summary-card .number { font-size: 32px; font-weight: 700; margin: 10px 0; }
        .summary-card .label { font-size: 12px; color: #666; text-transform: uppercase; letter-spacing: 1px; }
        .content { padding: 30px; }
        .table-wrapper { overflow-x: auto; }
        table { width: 100%; border-collapse: collapse; margin-bottom: 20px; }
        thead { background: #f1f3f5; border-bottom: 2px solid #dee2e6; }
        th { padding: 12px; text-align: left; font-weight: 600; color: #333; font-size: 13px; text-transform: uppercase; letter-spacing: 0.5px; }
        td { padding: 12px; border-bottom: 1px solid #e0e0e0; font-size: 14px; }
        tr.critical { background-color: #fff5f5; }
        tr.critical td { color: #721c24; }
        tr.warning { background-color: #fffbf0; }
        tr.warning td { color: #856404; }
        tr.healthy { background-color: #f0fff4; }
        tr.healthy td { color: #155724; }
        tr.no-activity { background-color: #f5f5f5; }
        tr.no-activity td { color: #666; opacity: 0.7; }
        tr.no-spend { background-color: #f5f5f5; }
        tr.no-spend td { color: #666; opacity: 0.7; }
        .status-badge { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
        .status-critical { background: #dc3545; color: white; }
        .status-warning { background: #ffc107; color: #333; }
        .status-healthy { background: #28a745; color: white; }
        .status-no-activity { background: #e9ecef; color: #666; }
        .status-no-spend { background: #e9ecef; color: #666; }
        .alert { display: inline-block; padding: 4px 8px; background: #fff3cd; color: #856404; border-radius: 4px; font-size: 12px; font-weight: 600; margin-left: 8px; }
        .alert.critical { background: #f8d7da; color: #721c24; }
        .type-badge { display: inline-block; padding: 3px 8px; border-radius: 3px; font-size: 11px; background: #e9ecef; color: #495057; }
        .type-badge.ecommerce { background: #cfe2ff; color: #084298; }
        .type-badge.lead-gen { background: #d1e7dd; color: #0f5132; }
        footer { background: #f8f9fa; padding: 20px 30px; text-align: center; font-size: 12px; color: #666; border-top: 1px solid #e0e0e0; }
        .pixel-health { font-size: 12px; padding: 2px 6px; border-radius: 3px; display: inline-block; }
        .pixel-health.healthy { background: #d4edda; color: #155724; }
        .pixel-health.stale { background: #f8d7da; color: #721c24; font-weight: 600; }
        .pixel-health.unknown { background: #e2e3e5; color: #383d41; }
        .pixel-health.no-pixel { background: #fff3cd; color: #856404; }
        .no-data { color: #999; font-style: italic; }
        @media (max-width: 768px) {
            header h1 { font-size: 24px; }
            .summary { grid-template-columns: repeat(2, 1fr); gap: 15px; padding: 20px; }
            table { font-size: 12px; }
            th, td { padding: 8px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>TWM Marketing &mdash; Meta Ads Anomaly Detector</h1>
            <p>Last updated: """

    html += timestamp
    html += """</p>
        </header>
        <div class="summary">
            <div class="summary-card critical">
                <div class="label">Critical Alerts</div>
                <div class="number">"""

    html += str(critical_count)
    html += """</div>
            </div>
            <div class="summary-card warning">
                <div class="label">Warnings</div>
                <div class="number">"""

    html += str(warning_count)
    html += """</div>
            </div>
            <div class="summary-card healthy">
                <div class="label">Healthy</div>
                <div class="number">"""

    html += str(healthy_count)
    html += """</div>
            </div>
            <div class="summary-card">
                <div class="label">No Activity</div>
                <div class="number">"""

    html += str(no_activity_count)
    html += """</div>
            </div>
            <div class="summary-card">
                <div class="label">Total Accounts</div>
                <div class="number">"""

    html += str(len(sorted_results))
    html += """</div>
            </div>
        </div>
        <div class="content">
            <div class="table-wrapper">
                <table>
                    <thead>
                        <tr>
                            <th>Account Name</th>
                            <th>Type</th>
                            <th>Pixel Status</th>
                            <th>Pixel Health</th>
                            <th>Yesterday Events</th>
                            <th>7-Day Avg</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
"""

    for result in sorted_results:
        row_class = result["status"].lower().replace(" ", "-")
        html += f'                        <tr class="{row_class}">\n'
        html += f'                            <td>{result["name"]}</td>\n'
        html += f'                            <td><span class="type-badge {"ecommerce" if result["account_type"] == "ecommerce" else "lead-gen"}">{result["account_type"]}</span></td>\n'

        # Pixel Status column
        if result["pixel_status"] == "N/A" or result["pixel_status"] == "NO PIXEL":
            html += f'                            <td><span class="no-data">&mdash;</span></td>\n'
        else:
            html += f'                            <td><code style="font-size: 11px;">{result["pixel_status"]}</code></td>\n'

        # Pixel Health column
        if result["pixel_health"] == "N/A":
            html += f'                            <td><span class="no-data">&mdash;</span></td>\n'
        elif result["pixel_health"] == "NO PIXEL":
            html += f'                            <td><span class="pixel-health no-pixel">NO PIXEL</span></td>\n'
        elif result["pixel_health"] == "HEALTHY":
            html += f'                            <td><span class="pixel-health healthy">HEALTHY</span></td>\n'
        elif result["pixel_health"].startswith("STALE"):
            html += f'                            <td><span class="pixel-health stale">{result["pixel_health"]}</span></td>\n'
        else:
            html += f'                            <td><span class="pixel-health unknown">{result["pixel_health"]}</span></td>\n'

        # Yesterday Events
        html += f'                            <td>{result["yesterday_events"]}</td>\n'

        # 7-Day Avg
        html += f'                            <td>{result["seven_day_avg"]}</td>\n'

        # Status
        status_class = f'status-{result["status"].lower().replace(" ", "-")}'
        html += f'                            <td><span class="status-badge {status_class}">{result["status"]}</span>'
        if result["alert"]:
            alert_class = "critical" if result["status"] == "Critical" else ""
            html += f' <span class="alert {alert_class}">{result["alert"]}</span>'
        if result["pixel_alert"]:
            html += f' <span class="alert critical">{result["pixel_alert"]}</span>'
        html += f'</td>\n'
        html += f'                        </tr>\n'

    html += """                    </tbody>
                </table>
            </div>
        </div>
        <footer>
            <p>Powered by TWM Marketing API &bull; Auto-refreshes daily at 9am ET</p>
        </footer>
    </div>
</body>
</html>
"""

    return html


def main():
    """Main execution."""
    global total_accounts

    # Load config
    if not os.path.exists(CONFIG_FILE):
        print(f"ERROR: {CONFIG_FILE} not found")
        sys.exit(1)

    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)

    accounts_data = config.get("accounts", [])
    # Flatten accounts: config may have categories (ecommerce, lead_gen, special) as dict
    if isinstance(accounts_data, dict):
        accounts = []
        for category in accounts_data.values():
            if isinstance(category, list):
                accounts.extend(category)
    else:
        accounts = accounts_data
    total_accounts = len(accounts)

    print(f"Analyzing {total_accounts} accounts...")
    print()

    # Analyze each account
    results = []
    for account in accounts:
        result = analyze_account(account)
        results.append(result)

    print()
    print(f"Summary:")
    print(f"  Critical Alerts: {sum(1 for r in results if r['status'] == 'Critical')}")
    print(f"  Warnings: {sum(1 for r in results if r['status'] == 'Warning')}")
    print(f"  Healthy: {sum(1 for r in results if r['status'] == 'Healthy')}")
    print(f"  No Activity: {sum(1 for r in results if r['status'] == 'No Activity')}")

    # Generate HTML
    html = generate_html(results)

    # Write output file with UTF-8 encoding
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Report written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
