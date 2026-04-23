import html
from pathlib import Path
from urllib.parse import urlencode

import pyodbc
from dotenv import dotenv_values, load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

APP_TITLE = "WPS Reports DEV"
ENV_PATH = Path("/workspaces/WPS_REPORTS_DEV/.env")
OPEN_WORK_STATUSES = ("UNASSIGNED", "IN_PROGRESS", "PAUSED", "HOLD")
REPORT_TYPE_OPTIONS = [
    ("open_work", "Open Work"),
    ("completed_work", "Completed Work"),
]
DATE_SCOPE_OPTIONS = [
    ("today", "Today"),
    ("tomorrow", "Tomorrow"),
    ("future", "Future"),
    ("all", "All"),
]
OPEN_WORK_COLUMNS = [
    "WorkDate",
    "Division",
    "Store",
    "Task",
    "Machine",
    "Operator",
    "Quantity",
    "QtyCompleted",
    "JobStatus",
    "ExpectedFinishTime",
    "StartTime",
    "FinishTime",
    "Key",
]
COMPLETED_WORK_COLUMNS = [
    "Operator",
    "Task",
    "Machine",
    "Store",
    "Quantity",
    "StartTime",
    "FinishTime",
]

# Force this standalone app to prefer its local DEV .env values over inherited shell values.
load_dotenv(ENV_PATH, override=True)
LOCAL_SQL_CONFIG = dotenv_values(ENV_PATH)

app = FastAPI(title=APP_TITLE)


def get_sql_setting(name: str) -> str:
    value = (LOCAL_SQL_CONFIG.get(name) or "").strip()
    if not value:
        raise RuntimeError(f"Missing required setting: {name}")
    return value


def get_connection():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={get_sql_setting('AZURE_SQL_SERVER')};"
        f"DATABASE={get_sql_setting('AZURE_SQL_DATABASE')};"
        f"UID={get_sql_setting('AZURE_SQL_USERNAME')};"
        f"PWD={get_sql_setting('AZURE_SQL_PASSWORD')};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def fetch_open_work_rows(date_scope: str | None = None):
    date_filter = ""
    params = list(OPEN_WORK_STATUSES)

    if date_scope == "today":
        date_filter = " AND CAST(WorkDate AS date) <= CAST(GETDATE() AS date)"
    elif date_scope == "tomorrow":
        date_filter = " AND CAST(WorkDate AS date) = DATEADD(day, 1, CAST(GETDATE() AS date))"
    elif date_scope == "future":
        date_filter = " AND CAST(WorkDate AS date) > CAST(GETDATE() AS date)"

    query = """
        SELECT
            WorkDate,
            Division,
            Store,
            Task,
            Machine,
            Operator,
            Quantity,
            QtyCompleted,
            JobStatus,
            ExpectedFinishTime,
            StartTime,
            FinishTime,
            [Key]
        FROM dbo.WPS
        WHERE JobStatus IN (?, ?, ?, ?)
    """
    query += date_filter
    query += """
        ORDER BY
            CASE WHEN WorkDate IS NULL THEN 1 ELSE 0 END,
            WorkDate DESC,
            Division,
            Store,
            Task,
            [Key]
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def fetch_completed_work_rows(date_scope: str | None = None):
    date_filter = ""

    if date_scope == "today":
        date_filter = " AND CAST(FinishDateTime AS date) = CAST(GETDATE() AS date)"
    elif date_scope == "tomorrow":
        date_filter = " AND CAST(FinishDateTime AS date) = DATEADD(day, 1, CAST(GETDATE() AS date))"
    elif date_scope == "future":
        date_filter = " AND CAST(FinishDateTime AS date) > CAST(GETDATE() AS date)"

    query = """
        SELECT
            Operator,
            Task,
            Machine,
            Store,
            Quantity,
            StartTime,
            FinishTime
        FROM dbo.WPS
        WHERE JobStatus = ?
    """
    query += date_filter
    query += """
        ORDER BY
            FinishTime DESC,
            Operator,
            Task,
            Machine
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, "COMPLETED")
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def display_value(value):
    if value is None:
        return ""
    return html.escape(str(value))


def build_url(path: str, **params) -> str:
    clean_params = {key: value for key, value in params.items() if value not in (None, "", "select")}
    if not clean_params:
        return path
    return f"{path}?{urlencode(clean_params)}"


def render_open_work_table(rows):
    header_html = "".join(f"<th>{html.escape(column)}</th>" for column in OPEN_WORK_COLUMNS)
    body_parts = []
    for row in rows:
        cells = "".join(
            f"<td>{display_value(row.get(column))}</td>" for column in OPEN_WORK_COLUMNS
        )
        body_parts.append(f"<tr>{cells}</tr>")

    if not body_parts:
        colspan = len(OPEN_WORK_COLUMNS)
        body_parts.append(
            f"<tr><td colspan=\"{colspan}\">No open work rows matched the filter.</td></tr>"
        )

    return (
        "<table>"
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{''.join(body_parts)}</tbody>"
        "</table>"
    )


def render_completed_work_table(rows):
    header_html = "".join(f"<th>{html.escape(column)}</th>" for column in COMPLETED_WORK_COLUMNS)
    body_parts = []
    for row in rows:
        cells = "".join(
            f"<td>{display_value(row.get(column))}</td>" for column in COMPLETED_WORK_COLUMNS
        )
        body_parts.append(f"<tr>{cells}</tr>")

    if not body_parts:
        colspan = len(COMPLETED_WORK_COLUMNS)
        body_parts.append(
            f"<tr><td colspan=\"{colspan}\">No completed work rows matched the filter.</td></tr>"
        )

    return (
        "<table>"
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{''.join(body_parts)}</tbody>"
        "</table>"
    )


def render_page(
    title: str,
    body_html: str,
    print_mode: bool = False,
    nav_href: str | None = "/reports",
    print_href: str | None = "/reports/open-work/print",
) -> HTMLResponse:
    print_controls = ""
    if not print_mode and print_href:
        print_controls = f"<p><a href=\"{html.escape(print_href)}\">Print View</a></p>"
    nav = ""
    if not print_mode and nav_href:
        nav = f"<p><a href=\"{html.escape(nav_href)}\">Back to Reports</a></p>"
    page = f"""
    <html>
        <head>
            <title>{html.escape(title)}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 24px; color: #111; }}
                h1 {{ margin-bottom: 8px; }}
                p {{ margin: 8px 0; }}
                table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
                th, td {{ border: 1px solid #ccc; padding: 6px 8px; text-align: left; vertical-align: top; }}
                th {{ background: #f4f4f4; position: sticky; top: 0; }}
                .meta {{ color: #555; }}
                @media print {{
                    a {{ color: inherit; text-decoration: none; }}
                    .no-print {{ display: none; }}
                    body {{ margin: 0; }}
                }}
            </style>
        </head>
        <body>
            <h1>{html.escape(title)}</h1>
            <div class="no-print">
                {nav}
                {print_controls}
            </div>
            {body_html}
        </body>
    </html>
    """
    return HTMLResponse(page)


@app.get("/", response_class=HTMLResponse)
def reports_home():
    return RedirectResponse(url="/reports", status_code=302)


@app.get("/health", response_class=JSONResponse)
def health_check():
    configured_database = LOCAL_SQL_CONFIG.get("AZURE_SQL_DATABASE")
    configured_server = LOCAL_SQL_CONFIG.get("AZURE_SQL_SERVER")
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    DB_NAME() AS actual_database,
                    CAST(SERVERPROPERTY('ServerName') AS nvarchar(255)) AS actual_server,
                    1 AS test_value
                """
            )
            row = cursor.fetchone()

        return {
            "ok": True,
            "app": APP_TITLE,
            "configured_server": configured_server,
            "configured_database": configured_database,
            "actual_server": row.actual_server,
            "actual_database": row.actual_database,
            "test_value": row.test_value,
            "env_path": str(ENV_PATH),
        }
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "app": APP_TITLE,
                "configured_server": configured_server,
                "configured_database": configured_database,
                "env_path": str(ENV_PATH),
                "error": str(exc),
            },
        )


@app.get("/reports", response_class=HTMLResponse)
def reports_launcher(report_type: str | None = None, date_scope: str | None = None):
    if report_type == "open_work" and date_scope in {value for value, _ in DATE_SCOPE_OPTIONS}:
        return RedirectResponse(
            url=build_url("/reports/open-work", report_type=report_type, date_scope=date_scope),
            status_code=302,
        )
    if report_type == "completed_work" and date_scope in {value for value, _ in DATE_SCOPE_OPTIONS}:
        return RedirectResponse(
            url=build_url("/reports/completed-work", report_type=report_type, date_scope=date_scope),
            status_code=302,
        )

    report_type_options = ['<option value="" selected>Select</option>']
    for value, label in REPORT_TYPE_OPTIONS:
        selected = " selected" if report_type == value else ""
        report_type_options.append(
            f'<option value="{html.escape(value)}"{selected}>{html.escape(label)}</option>'
        )

    date_scope_options = ['<option value="" selected>Select</option>']
    for value, label in DATE_SCOPE_OPTIONS:
        selected = " selected" if date_scope == value else ""
        date_scope_options.append(
            f'<option value="{html.escape(value)}"{selected}>{html.escape(label)}</option>'
        )

    body_html = f"""
        <p class="meta">this is the standalone read-only reports app for DEV</p>
        <form method="get" action="/reports" class="launcher">
            <label for="report_type">Report Type</label>
            <select id="report_type" name="report_type" required>
                {''.join(report_type_options)}
            </select>
            <label for="date_scope">Date Scope</label>
            <select id="date_scope" name="date_scope" required>
                {''.join(date_scope_options)}
            </select>
            <button type="submit">Open Report</button>
        </form>
    """
    return render_page(APP_TITLE, body_html, nav_href=None, print_href=None)


@app.get("/reports/open-work", response_class=HTMLResponse)
def open_work_report(date_scope: str | None = None, report_type: str | None = None):
    rows = fetch_open_work_rows(date_scope=date_scope)
    active_scope = next(
        (label for value, label in DATE_SCOPE_OPTIONS if value == date_scope),
        "Not specified",
    )
    body_html = (
        "<p class=\"meta\">Showing open work from dbo.WPS for statuses: "
        + ", ".join(html.escape(status) for status in OPEN_WORK_STATUSES)
        + f". Date Scope: {html.escape(active_scope)}.</p>"
        + render_open_work_table(rows)
    )
    return render_page(
        "Open Work",
        body_html,
        nav_href="/reports",
        print_href=build_url(
            "/reports/open-work/print",
            report_type=report_type or "open_work",
            date_scope=date_scope,
        ),
    )


@app.get("/reports/open-work/print", response_class=HTMLResponse)
def open_work_report_print(date_scope: str | None = None, report_type: str | None = None):
    rows = fetch_open_work_rows(date_scope=date_scope)
    active_scope = next(
        (label for value, label in DATE_SCOPE_OPTIONS if value == date_scope),
        "Not specified",
    )
    body_html = (
        "<p class=\"meta\">Print-friendly open work report from dbo.WPS. "
        + f"Date Scope: {html.escape(active_scope)}.</p>"
        + render_open_work_table(rows)
    )
    return render_page("Open Work Print", body_html, print_mode=True)


@app.get("/reports/completed-work", response_class=HTMLResponse)
def completed_work_report(date_scope: str | None = None, report_type: str | None = None):
    rows = fetch_completed_work_rows(date_scope=date_scope)
    active_scope = next(
        (label for value, label in DATE_SCOPE_OPTIONS if value == date_scope),
        "Not specified",
    )
    body_html = (
        "<p class=\"meta\">Showing completed work from dbo.WPS where JobStatus is COMPLETED. "
        + f"Date Scope: {html.escape(active_scope)}.</p>"
        + render_completed_work_table(rows)
    )
    return render_page(
        "Completed Work",
        body_html,
        nav_href="/reports",
        print_href=build_url(
            "/reports/completed-work/print",
            report_type=report_type or "completed_work",
            date_scope=date_scope,
        ),
    )


@app.get("/reports/completed-work/print", response_class=HTMLResponse)
def completed_work_report_print(date_scope: str | None = None, report_type: str | None = None):
    rows = fetch_completed_work_rows(date_scope=date_scope)
    active_scope = next(
        (label for value, label in DATE_SCOPE_OPTIONS if value == date_scope),
        "Not specified",
    )
    body_html = (
        "<p class=\"meta\">Print-friendly completed work report from dbo.WPS. "
        + f"Date Scope: {html.escape(active_scope)}.</p>"
        + render_completed_work_table(rows)
    )
    return render_page("Completed Work Print", body_html, print_mode=True)
