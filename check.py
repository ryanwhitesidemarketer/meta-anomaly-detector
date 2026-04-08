#!/usr/bin/env python3
"""
Meta Ads Anomaly Detector v2
Monitors pixel health and conversion event anomalies across Meta Ad accounts.
Features 30-day trend charts, event type labels, and TWM branding.
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


def get_date_n_days_ago(n):
    """Return date N days ago as YYYY-MM-DD."""
    return (datetime.now() - timedelta(days=n)).strftime("%Y-%m-%d")


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
        time.sleep(0.05)


def get_pixel_id(account_id):
    """Fetch the best (most active) pixel for an account by checking data volume."""
    endpoint = f"{account_id}/adspixels"
    params = {"fields": "id,name,last_fired_time", "limit": "25"}
    data = api_call(endpoint, params)
    if data and "data" in data and len(data["data"]) > 0:
        pixels = data["data"]
        print(f"  Pixels found for {account_id}: {len(pixels)} pixel(s)")
        if len(pixels) == 1:
            print(f"  Using pixel {pixels[0].get('id')} ({pixels[0].get('name', 'unnamed')})")
            return pixels[0]
        # Multiple pixels â check PageView volume for each to find the most active
        best_pixel = None
        best_count = -1
        for p in pixels:
            pid = p.get("id")
            stats_endpoint = f"{pid}/stats"
            # Use only aggregation=event, no other filter params
            stats_params = {"aggregation": "event"}
            stats_data = api_call(stats_endpoint, stats_params)
            total = 0
            if stats_data and "data" in stats_data:
                for entry in stats_data["data"]:
                    # Entry format: {start_time, aggregation, data: [{value, count}, ...]}
                    if "data" in entry:
                        for event_item in entry["data"]:
                            if event_item.get("value") == "PageView":
                                total += int(event_item.get("count", 0))
            print(f"    Pixel {pid} ({p.get('name', 'unnamed')}): {total} PageViews (24h), last_fired: {p.get('last_fired_time', 'N/A')}")
            if total > best_count:
                best_count = total
                best_pixel = p
        if best_pixel:
            print(f"  Selected pixel {best_pixel.get('id')} ({best_pixel.get('name', 'unnamed')}) with {best_count} PageViews")
            return best_pixel
        # Fallback: pick the one with most recent last_fired_time
        pixels_sorted = sorted(pixels, key=lambda x: x.get("last_fired_time", ""), reverse=True)
        return pixels_sorted[0]
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

    try:
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


def get_pixel_event_names(account):
    """Return the pixel event name(s) to look for in the pixel stats endpoint."""
    account_type = account.get("type")
    if account_type == "ecommerce":
        return ["Purchase"]
    custom = account.get("custom_event_name")
    if custom:
        return [custom]
    return ["Lead"]


def get_pixel_daily_stats(pixel_id, date_start, date_end, pixel_event_names):
    """Fetch daily pixel event stats from the pixel stats endpoint (ALL events, not just ad-attributed)."""
    start_dt = datetime.strptime(date_start, "%Y-%m-%d")
    end_dt = datetime.strptime(date_end, "%Y-%m-%d")

    daily_data = {}

    # Fetch data using aggregation=event with cursor-based pagination
    cursor = None
    pages_fetched = 0
    max_pages = 35

    while pages_fetched < max_pages:
        endpoint = f"{pixel_id}/stats"
        params = {"aggregation": "event"}
        if cursor:
            params["before"] = cursor

        data = api_call(endpoint, params)
        if not data or "data" not in data or len(data["data"]) == 0:
            break

        # Parse each hourly entry
        for entry in data["data"]:
            start_time_str = entry.get("start_time", "")
            if not start_time_str:
                continue

            # Parse start_time to extract date (YYYY-MM-DD)
            try:
                # Format: "2026-04-07T17:00:00+0000"
                entry_dt = datetime.fromisoformat(
                    re.sub(r'([+-]\d{2})(\d{2})$', r'\1:\2', start_time_str).replace("Z", "+00:00")
                )
                entry_date = entry_dt.strftime("%Y-%m-%d")
            except:
                continue

            # Skip if outside date range
            if entry_dt.date() < start_dt.date() or entry_dt.date() > end_dt.date():
                continue

            # Initialize date entry if needed
            if entry_date not in daily_data:
                daily_data[entry_date] = {"events": 0, "pageviews": 0}

            # Parse nested data array: {value, count}
            if "data" in entry:
                for event_item in entry["data"]:
                    value = event_item.get("value", "")
                    count = int(event_item.get("count", 0))

                    if value in pixel_event_names:
                        daily_data[entry_date]["events"] += count
                    elif value == "PageView":
                        daily_data[entry_date]["pageviews"] += count

        pages_fetched += 1

        # Check if earliest entry on this page is before our date range â stop paginating
        earliest_on_page = data["data"][-1].get("start_time", "") if data["data"] else ""
        if earliest_on_page:
            try:
                earliest_dt = datetime.fromisoformat(
                    re.sub(r'([+-]\d{2})(\d{2})$', r'\1:\2', earliest_on_page).replace("Z", "+00:00")
                )
                if earliest_dt.date() < start_dt.date():
                    break
            except:
                pass

        # Check for more pages
        if "paging" in data and "cursors" in data["paging"]:
            cursor = data["paging"]["cursors"].get("before")
            if not cursor:
                break
        else:
            break

    return daily_data


def get_daily_insights(account_id, date_start, date_end, event_types):
    """Fallback: Fetch daily ad-attributed insights for accounts without pixels."""
    endpoint = f"{account_id}/insights"
    params = {
        "time_range": json.dumps({"since": date_start, "until": date_end}),
        "time_increment": "1",
        "fields": "actions,spend",
        "limit": "100"
    }
    data = api_call(endpoint, params)

    daily_data = {}
    if data and "data" in data:
        for day in data["data"]:
            date_str = day.get("date_start", "")
            events = 0
            pageviews = 0
            if "actions" in day:
                for action in day["actions"]:
                    atype = action.get("action_type", "")
                    val = int(action.get("value", 0))
                    if atype in (event_types if isinstance(event_types, list) else [event_types]):
                        events += val
                    if atype in ["offsite_conversion.fb_pixel_view_content", "landing_page_view",
                                  "page_view", "offsite_conversion.fb_pixel_page_view",
                                  "view_content", "omni_view_content"]:
                        pageviews += val
            daily_data[date_str] = {"events": events, "pageviews": pageviews}

    return daily_data


def get_event_label(account):
    """Return a human-readable label for the event type being tracked."""
    account_type = account.get("type")
    if account_type == "ecommerce":
        return "Purchases"
    custom = account.get("custom_event_name")
    if custom:
        return custom.replace("_", " ").title()
    event_types = account.get("event_types", [])
    if isinstance(event_types, list):
        for et in event_types:
            if "lead" in et.lower():
                return "Leads"
    return "Leads"


def analyze_account(account):
    """Analyze a single account and return status info with 30-day daily data."""
    global alert_count, all_clear_count, no_spend_count

    name = account.get("name")
    account_id = account.get("account_id")
    account_type = account.get("type")
    skip_pixel = account.get("skip_pixel_check", False)
    event_label = get_event_label(account)

    yesterday = get_date_n_days_ago(1)
    thirty_days_ago = get_date_n_days_ago(31)
    seven_days_ago = get_date_n_days_ago(7)

    # Get event types to track (include all Meta API naming variants)
    if account_type == "ecommerce":
        base = account.get("event_type", "")
        event_types = [base, "purchase", "omni_purchase", "offsite_conversion.fb_pixel_purchase"]
        if base and base not in event_types:
            event_types.append(base)
    else:
        base_types = account.get("event_types", ["lead"])
        event_types = list(base_types)
        # Add variants for lead tracking
        if any("lead" in et.lower() for et in base_types):
            for variant in ["lead", "omni_lead", "offsite_conversion.fb_pixel_lead"]:
                if variant not in event_types:
                    event_types.append(variant)
    # Deduplicate
    event_types = list(dict.fromkeys(event_types))

    # Check pixel health and get pixel ID
    pixel_status = "N/A"
    pixel_health = "N/A"
    pixel_id = None
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

    # Get 30-day daily data
    # Use pixel stats (all events) when pixel is available; fall back to insights API
    pixel_event_names = get_pixel_event_names(account)
    if pixel_id:
        print(f"  Using pixel stats for {name} (pixel {pixel_id}, events: {pixel_event_names})")
        daily = get_pixel_daily_stats(pixel_id, thirty_days_ago, yesterday, pixel_event_names)
        if not daily:
            print(f"  Pixel stats empty, falling back to insights API for {name}")
            daily = get_daily_insights(account_id, thirty_days_ago, yesterday, event_types)
    else:
        print(f"  Using insights API for {name} (no pixel)")
        daily = get_daily_insights(account_id, thirty_days_ago, yesterday, event_types)

    # Build sorted daily arrays for charting
    all_dates = sorted(daily.keys())
    daily_events = [daily[d]["events"] for d in all_dates]
    daily_pageviews = [daily[d]["pageviews"] for d in all_dates]
    chart_labels = [d[5:] for d in all_dates]  # MM-DD format

    # Calculate yesterday and 7-day averages
    yesterday_events = daily.get(yesterday, {}).get("events", 0)
    yesterday_pageviews = daily.get(yesterday, {}).get("pageviews", 0)

    recent_7_dates = [d for d in all_dates if d >= seven_days_ago]
    seven_day_events = sum(daily.get(d, {}).get("events", 0) for d in recent_7_dates)
    seven_day_pageviews = sum(daily.get(d, {}).get("pageviews", 0) for d in recent_7_dates)
    avg_daily_events = seven_day_events / 7 if seven_day_events > 0 else 0
    avg_daily_pageviews = seven_day_pageviews / 7 if seven_day_pageviews > 0 else 0

    # Determine status
    status = "Healthy"
    alert = ""

    if yesterday_events == 0 and avg_daily_events == 0:
        status = "No Activity"
        no_spend_count += 1
    elif yesterday_events == 0 and avg_daily_events > 0:
        status = "Critical"
        alert = "ZERO EVENTS"
        alert_count += 1
    elif yesterday_events < (avg_daily_events * 0.5) and avg_daily_events > 0:
        status = "Warning"
        alert = f"DOWN {int(100 - (yesterday_events / avg_daily_events * 100))}%"
        alert_count += 1
    else:
        all_clear_count += 1

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

    print(f"Account {name}: status={status}, alert={alert if alert else 'None'}, days={len(all_dates)}")

    return {
        "name": name,
        "account_type": account_type,
        "event_label": event_label,
        "pixel_status": pixel_status if pixel_status != "N/A" else "N/A",
        "pixel_health": pixel_health,
        "pixel_alert": pixel_alert,
        "yesterday_events": yesterday_events,
        "seven_day_avg": round(avg_daily_events, 1),
        "yesterday_pageviews": yesterday_pageviews,
        "seven_day_avg_pageviews": round(avg_daily_pageviews, 1),
        "chart_labels": chart_labels,
        "chart_events": daily_events,
        "chart_pageviews": daily_pageviews,
        "status": status,
        "alert": alert,
        "sort_key": 0 if status == "Critical" else (1 if status == "Warning" else (3 if status == "Healthy" else 4))
    }


def generate_html(results):
    """Generate the HTML dashboard with TWM branding and Chart.js graphs."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S ET")

    sorted_results = sorted(results, key=lambda x: (x["sort_key"], x["name"]))

    critical_count = sum(1 for r in sorted_results if r["status"] == "Critical")
    warning_count = sum(1 for r in sorted_results if r["status"] == "Warning")
    healthy_count = sum(1 for r in sorted_results if r["status"] == "Healthy")
    no_activity_count = sum(1 for r in sorted_results if r["status"] == "No Activity")

    # Serialize chart data for JS
    chart_data_json = json.dumps([{
        "name": r["name"],
        "labels": r["chart_labels"],
        "events": r["chart_events"],
        "pageviews": r["chart_pageviews"],
        "eventLabel": r["event_label"]
    } for r in sorted_results])

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TWM Meta Ads Anomaly Detector</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: #f0f2f5;
            min-height: 100vh;
            color: #333;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            box-shadow: 0 4px 24px rgba(0, 0, 0, 0.08);
        }}
        header {{
            background: linear-gradient(135deg, #050062 0%, #003D8E 100%);
            color: white;
            padding: 32px 40px;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }}
        .header-left {{
            display: flex;
            align-items: center;
            gap: 20px;
        }}
        .header-logo {{
            height: 48px;
            filter: brightness(0) invert(1);
        }}
        header h1 {{
            font-size: 24px;
            font-weight: 700;
            letter-spacing: -0.3px;
        }}
        header .subtitle {{
            font-size: 13px;
            opacity: 0.7;
            margin-top: 4px;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 0;
            border-bottom: 2px solid #e8e8e8;
        }}
        .summary-card {{
            padding: 24px 20px;
            text-align: center;
            border-right: 1px solid #e8e8e8;
            transition: background 0.2s;
        }}
        .summary-card:last-child {{ border-right: none; }}
        .summary-card .number {{
            font-size: 36px;
            font-weight: 700;
            margin: 6px 0;
        }}
        .summary-card .label {{
            font-size: 11px;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 1.2px;
            font-weight: 600;
        }}
        .summary-card.critical .number {{ color: #dc3545; }}
        .summary-card.warning .number {{ color: #FF6100; }}
        .summary-card.healthy .number {{ color: #00B6DB; }}
        .content {{ padding: 0; }}
        .table-wrapper {{ overflow-x: auto; }}
        table {{ width: 100%; border-collapse: collapse; }}
        thead {{ background: #fafafa; }}
        th {{
            padding: 14px 16px;
            text-align: left;
            font-weight: 600;
            color: #666;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.8px;
            border-bottom: 2px solid #e8e8e8;
        }}
        td {{
            padding: 14px 16px;
            border-bottom: 1px solid #f0f0f0;
            font-size: 14px;
        }}
        tr.data-row {{ cursor: pointer; transition: background 0.15s; }}
        tr.data-row:hover {{ background: #f8f9ff; }}
        tr.critical {{ background-color: #fff8f8; }}
        tr.critical:hover {{ background-color: #fff0f0; }}
        tr.warning {{ background-color: #fffbf5; }}
        tr.warning:hover {{ background-color: #fff5e8; }}
        tr.healthy {{ background-color: #f8fdff; }}
        tr.healthy:hover {{ background-color: #f0faff; }}
        tr.no-activity td {{ color: #999; }}
        .account-name {{
            font-weight: 600;
            font-size: 14px;
        }}
        .account-name.ecommerce {{ color: #003D8E; }}
        .account-name.lead-gen {{ color: #00856A; }}
        .status-badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .status-critical {{ background: #dc3545; color: white; }}
        .status-warning {{ background: #FF6100; color: white; }}
        .status-healthy {{ background: #00B6DB; color: white; }}
        .status-no-activity {{ background: #e9ecef; color: #888; }}
        .alert {{
            display: inline-block;
            padding: 3px 8px;
            background: #fff0f0;
            color: #dc3545;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 600;
            margin-left: 6px;
        }}
        .pixel-health {{
            font-size: 11px;
            padding: 3px 8px;
            border-radius: 4px;
            display: inline-block;
            font-weight: 500;
        }}
        .pixel-health.healthy {{ background: #e8f8f0; color: #00856A; }}
        .pixel-health.stale {{ background: #fff0f0; color: #dc3545; font-weight: 600; }}
        .pixel-health.unknown {{ background: #f0f0f0; color: #888; }}
        .pixel-health.no-pixel {{ background: #fff5e8; color: #FF6100; }}
        .no-data {{ color: #ccc; }}
        .expand-icon {{
            display: inline-block;
            width: 18px;
            height: 18px;
            line-height: 18px;
            text-align: center;
            font-size: 12px;
            color: #999;
            transition: transform 0.2s;
            margin-right: 8px;
        }}
        tr.expanded .expand-icon {{ transform: rotate(90deg); }}
        .chart-row {{ display: none; }}
        .chart-row.visible {{ display: table-row; }}
        .chart-row td {{
            padding: 16px 24px 24px;
            background: #fafbfc;
            border-bottom: 2px solid #e8e8e8;
        }}
        .chart-container {{
            max-width: 900px;
            height: 250px;
        }}
        .event-label {{
            font-size: 11px;
            color: #888;
            font-weight: 500;
        }}
        .legend-hint {{
            display: flex;
            gap: 20px;
            margin-bottom: 12px;
            padding-left: 4px;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 12px;
            color: #666;
        }}
        .legend-dot {{
            width: 10px;
            height: 10px;
            border-radius: 50%;
        }}
        footer {{
            background: #050062;
            padding: 24px 40px;
            text-align: center;
            font-size: 12px;
            color: rgba(255,255,255,0.6);
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 12px;
        }}
        footer img {{
            height: 28px;
            opacity: 0.8;
        }}
        @media (max-width: 900px) {{
            header {{ flex-direction: column; text-align: center; gap: 12px; padding: 24px 20px; }}
            .summary {{ grid-template-columns: repeat(3, 1fr); }}
            th, td {{ padding: 10px 8px; font-size: 12px; }}
            .chart-container {{ height: 200px; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="header-left">
                <img src="https://twowheelsmarketing.com/wp-content/uploads/2023/08/footerlogo-outer-1.svg" alt="TWM" class="header-logo">
                <div>
                    <h1>Meta Ads Anomaly Detector</h1>
                    <div class="subtitle">Last updated: {timestamp}</div>
                </div>
            </div>
        </header>
        <div class="summary">
            <div class="summary-card critical">
                <div class="label">Critical</div>
                <div class="number">{critical_count}</div>
            </div>
            <div class="summary-card warning">
                <div class="label">Warnings</div>
                <div class="number">{warning_count}</div>
            </div>
            <div class="summary-card healthy">
                <div class="label">Healthy</div>
                <div class="number">{healthy_count}</div>
            </div>
            <div class="summary-card">
                <div class="label">No Activity</div>
                <div class="number" style="color:#999">{no_activity_count}</div>
            </div>
            <div class="summary-card">
                <div class="label">Total</div>
                <div class="number" style="color:#050062">{len(sorted_results)}</div>
            </div>
        </div>
        <div class="content">
            <div class="table-wrapper">
                <table>
                    <thead>
                        <tr>
                            <th style="width:22%">Account</th>
                            <th>Pixel</th>
                            <th>Event Type</th>
                            <th>Yesterday</th>
                            <th>7-Day Avg</th>
                            <th>PageViews (Yday)</th>
                            <th>PV 7-Day Avg</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
"""

    for idx, result in enumerate(sorted_results):
        row_class = result["status"].lower().replace(" ", "-")
        name_class = "ecommerce" if result["account_type"] == "ecommerce" else "lead-gen"

        html += f'                        <tr class="{row_class} data-row" onclick="toggleChart({idx})" id="row-{idx}">\n'
        html += f'                            <td><span class="expand-icon">&#9654;</span><span class="account-name {name_class}">{result["name"]}</span></td>\n'

        # Pixel Health
        if result["pixel_health"] == "N/A":
            html += f'                            <td><span class="no-data">&mdash;</span></td>\n'
        elif result["pixel_health"] == "NO PIXEL":
            html += f'                            <td><span class="pixel-health no-pixel">No Pixel</span></td>\n'
        elif result["pixel_health"] == "HEALTHY":
            html += f'                            <td><span class="pixel-health healthy">Healthy</span></td>\n'
        elif result["pixel_health"].startswith("STALE"):
            html += f'                            <td><span class="pixel-health stale">{result["pixel_health"]}</span></td>\n'
        else:
            html += f'                            <td><span class="pixel-health unknown">{result["pixel_health"]}</span></td>\n'

        # Event Type label
        html += f'                            <td><span class="event-label">{result["event_label"]}</span></td>\n'

        # Yesterday Events
        html += f'                            <td><strong>{result["yesterday_events"]}</strong></td>\n'

        # 7-Day Avg
        html += f'                            <td>{result["seven_day_avg"]}</td>\n'

        # PageViews Yesterday
        html += f'                            <td>{result["yesterday_pageviews"]}</td>\n'

        # PageViews 7-Day Avg
        html += f'                            <td>{result["seven_day_avg_pageviews"]}</td>\n'

        # Status
        status_class = f'status-{result["status"].lower().replace(" ", "-")}'
        html += f'                            <td><span class="status-badge {status_class}">{result["status"]}</span>'
        if result["alert"]:
            html += f' <span class="alert">{result["alert"]}</span>'
        if result["pixel_alert"]:
            html += f' <span class="alert">{result["pixel_alert"]}</span>'
        html += f'</td>\n'
        html += f'                        </tr>\n'

        # Chart row (hidden by default)
        html += f'                        <tr class="chart-row" id="chart-row-{idx}">\n'
        html += f'                            <td colspan="8">\n'
        html += f'                                <div class="legend-hint">\n'
        html += f'                                    <div class="legend-item"><div class="legend-dot" style="background:#FF6100"></div>{result["event_label"]}</div>\n'
        html += f'                                    <div class="legend-item"><div class="legend-dot" style="background:#00B6DB"></div>PageViews</div>\n'
        html += f'                                </div>\n'
        html += f'                                <div class="chart-container"><canvas id="chart-{idx}"></canvas></div>\n'
        html += f'                            </td>\n'
        html += f'                        </tr>\n'

    html += """                    </tbody>
                </table>
            </div>
        </div>
        <footer>
            <img src="https://twowheelsmarketing.com/wp-content/uploads/2023/08/footerlogo-outer-1.svg" alt="Two Wheels Marketing">
            <span>Powered by Two Wheels Marketing &bull; Auto-refreshes daily at 9am ET</span>
        </footer>
    </div>
    <script>
"""
    html += f"    const chartData = {chart_data_json};\n"
    html += """    const charts = {};

    function toggleChart(idx) {
        const row = document.getElementById('row-' + idx);
        const chartRow = document.getElementById('chart-row-' + idx);
        const isVisible = chartRow.classList.contains('visible');

        if (isVisible) {
            chartRow.classList.remove('visible');
            row.classList.remove('expanded');
            if (charts[idx]) {
                charts[idx].destroy();
                delete charts[idx];
            }
        } else {
            chartRow.classList.add('visible');
            row.classList.add('expanded');
            createChart(idx);
        }
    }

    function createChart(idx) {
        const d = chartData[idx];
        const ctx = document.getElementById('chart-' + idx).getContext('2d');
        charts[idx] = new Chart(ctx, {
            type: 'line',
            data: {
                labels: d.labels,
                datasets: [
                    {
                        label: d.eventLabel,
                        data: d.events,
                        borderColor: '#FF6100',
                        backgroundColor: 'rgba(255, 97, 0, 0.08)',
                        borderWidth: 2.5,
                        pointRadius: 2,
                        pointHoverRadius: 5,
                        tension: 0.3,
                        fill: true
                    },
                    {
                        label: 'PageViews',
                        data: d.pageviews,
                        borderColor: '#00B6DB',
                        backgroundColor: 'rgba(0, 182, 219, 0.06)',
                        borderWidth: 2,
                        pointRadius: 1.5,
                        pointHoverRadius: 4,
                        tension: 0.3,
                        fill: true
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: '#050062',
                        titleFont: { family: 'DM Sans', size: 12 },
                        bodyFont: { family: 'DM Sans', size: 12 },
                        padding: 10,
                        cornerRadius: 6
                    }
                },
                scales: {
                    x: {
                        grid: { display: false },
                        ticks: { font: { family: 'DM Sans', size: 10 }, color: '#999', maxTicksLimit: 15 }
                    },
                    y: {
                        beginAtZero: true,
                        grid: { color: '#f0f0f0' },
                        ticks: { font: { family: 'DM Sans', size: 11 }, color: '#999' }
                    }
                }
            }
        });
    }
    </script>
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

    html = generate_html(results)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Report written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
