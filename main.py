import html
from pathlib import Path
from urllib.parse import urlencode

import pyodbc
from dotenv import dotenv_values, load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

APP_TITLE = "WPS Reports PRODUCTION"
WPS_PRODUCTION_BASE_URL = "https://wps-production-gkd3adfkgehqascz.westus2-01.azurewebsites.net"
REPORTS_HOME_PATH = "/reports"
ENV_PATH = Path("/workspaces/WPS_REPORTS/.env")
OPEN_WORK_STATUSES = ("UNASSIGNED", "IN_PROGRESS", "PAUSED")
OPEN_WORK_GROUPS = [
    ("UNASSIGNED", "Quick Unassigned"),
    ("IN_PROGRESS", "Work In Progress"),
    ("PAUSED", "Paused Work"),
]
REPORT_TYPE_OPTIONS = [
    ("open_work", "Open Work"),
    ("completed_work", "Completed Work"),
    ("paused_work", "Paused Work"),
    ("hold_orders", "Hold Orders"),
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
    "FinishDateTime",
]
INCOMPLETE_WORK_COLUMNS = [
    "WorkDate",
    "Division",
    "Store",
    "Task",
    "Machine",
    "Operator",
    "Quantity",
    "QtyCompleted",
    "RemainingQty",
    "JobStatus",
    "ExpectedFinishTime",
    "StartTime",
    "FinishTime",
    "[Key]",
]

# Force this standalone app to prefer its local .env values over inherited shell values.
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


def _wps_production_url(path: str = "/") -> str:
    clean_path = path if path.startswith("/") else f"/{path}"
    return f"{WPS_PRODUCTION_BASE_URL}{clean_path}"


def _table_exists(cursor, table_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'dbo'
          AND TABLE_NAME = ?;
        """,
        table_name,
    )
    return cursor.fetchone() is not None


def _columns_exist(cursor, table_name: str, required_columns: tuple[str, ...]) -> bool:
    if not required_columns:
        return True
    placeholders = ", ".join("?" for _ in required_columns)
    cursor.execute(
        f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo'
          AND TABLE_NAME = ?
          AND COLUMN_NAME IN ({placeholders});
        """,
        table_name,
        *required_columns,
    )
    found_columns = {row[0] for row in cursor.fetchall()}
    return all(column in found_columns for column in required_columns)


def fetch_open_work_rows(date_scope: str | None = None):
    date_filter = ""
    params = list(OPEN_WORK_STATUSES)
    status_placeholders = ", ".join("?" for _ in OPEN_WORK_STATUSES)

    if date_scope == "today":
        date_filter = (
            " AND ("
            "CAST(WorkDate AS date) <= CAST(GETDATE() AS date) "
            "OR ISNULL(ReleaseToToday, 0) = 1"
            ")"
        )
    elif date_scope == "tomorrow":
        date_filter = " AND CAST(WorkDate AS date) = DATEADD(day, 1, CAST(GETDATE() AS date))"
    elif date_scope == "future":
        date_filter = " AND CAST(WorkDate AS date) > CAST(GETDATE() AS date)"

    query = f"""
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
        WHERE JobStatus IN ({status_placeholders})
          AND ISNULL(IsDeleted, 0) = 0
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
        cursor.execute(query, *params)
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
    elif date_scope == "tomorrow_completed_early":
        date_filter = (
            " AND FinishDateTime IS NOT NULL"
            " AND CAST(FinishDateTime AS date) = CAST(GETDATE() AS date)"
            " AND CAST(WorkDate AS date) = DATEADD(day, 1, CAST(GETDATE() AS date))"
        )
    elif date_scope == "all":
        date_filter = ""

    query = """
        SELECT
            Operator,
            Task,
            Machine,
            Store,
            Quantity,
            StartTime,
            FinishDateTime,
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


def fetch_incomplete_work_rows(date_scope: str | None = None):
    date_filter = ""

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
            (Quantity - ISNULL(QtyCompleted, 0)) AS RemainingQty,
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
            WorkDate DESC,
            Division,
            Store,
            Task,
            [Key]
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, OPEN_WORK_STATUSES)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def fetch_paused_work_rows():
    wps_query = """
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
        WHERE JobStatus = ?
        ORDER BY
            WorkDate ASC,
            Division,
            Store,
            Task
    """
    bundle_query = """
        SELECT
            BundleID,
            Task,
            Machine,
            Operator,
            RemainingQuantity,
            TotalQuantity,
            ExpectedFinishTime,
            StartTime,
            FinishTime,
            Status
        FROM dbo.BundleHeader
        WHERE Status = ?
    """
    bundle_required_columns = (
        "BundleID",
        "Task",
        "Machine",
        "Operator",
        "RemainingQuantity",
        "TotalQuantity",
        "ExpectedFinishTime",
        "StartTime",
        "FinishTime",
        "Status",
    )
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(wps_query, "PAUSED")
        wps_rows = cursor.fetchall()
        wps_columns = [col[0] for col in cursor.description]
        paused_rows = [dict(zip(wps_columns, row)) for row in wps_rows]
        bundle_rows = []
        bundle_columns = []
        bundle_warning = None
        if _table_exists(cursor, "BundleHeader") and _columns_exist(
            cursor, "BundleHeader", bundle_required_columns
        ):
            try:
                cursor.execute(bundle_query, "PAUSED")
                bundle_rows = cursor.fetchall()
                bundle_columns = [col[0] for col in cursor.description]
            except pyodbc.Error:
                bundle_warning = "BundleHeader paused rows are unavailable; showing only dbo.WPS paused rows."
        else:
            bundle_warning = "BundleHeader paused rows are unavailable; showing only dbo.WPS paused rows."

    for bundle_row in bundle_rows:
        bundle = dict(zip(bundle_columns, bundle_row))
        paused_rows.append(
            {
                "WorkDate": None,
                "Division": "",
                "Store": "",
                "Task": bundle.get("Task"),
                "Machine": bundle.get("Machine"),
                "Operator": bundle.get("Operator"),
                "Quantity": bundle.get("RemainingQuantity"),
                "QtyCompleted": (bundle.get("TotalQuantity") or 0)
                - (bundle.get("RemainingQuantity") or 0),
                "JobStatus": bundle.get("Status"),
                "ExpectedFinishTime": bundle.get("ExpectedFinishTime"),
                "StartTime": bundle.get("StartTime"),
                "FinishTime": bundle.get("FinishTime"),
                "Key": f"BUNDLE_{bundle.get('BundleID')}",
            }
        )

    return paused_rows, bundle_warning


def fetch_hold_order_rows():
    wps_query = """
        SELECT
            WorkDate,
            Division,
            Store,
            Task,
            Machine,
            Operator,
            Quantity,
            StartTime,
            ExpectedFinishTime,
            [Key]
        FROM dbo.WPS
        WHERE JobStatus = ?
        ORDER BY
            WorkDate ASC,
            Division,
            Store,
            Task
    """
    bundle_query = """
        SELECT
            NULL AS WorkDate,
            '' AS Division,
            '' AS Store,
            Task,
            Machine,
            Operator,
            RemainingQuantity AS Quantity,
            StartTime,
            ExpectedFinishTime,
            CAST(BundleID AS varchar(50)) AS [Key]
        FROM dbo.BundleHeader
        WHERE Status = ? AND IsActive = 1
    """
    bundle_required_columns = (
        "Task",
        "Machine",
        "Operator",
        "RemainingQuantity",
        "StartTime",
        "ExpectedFinishTime",
        "BundleID",
        "Status",
        "IsActive",
    )
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(wps_query, "HOLD")
        wps_rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in wps_rows]
        bundle_warning = None
        if _table_exists(cursor, "BundleHeader") and _columns_exist(
            cursor, "BundleHeader", bundle_required_columns
        ):
            try:
                cursor.execute(bundle_query, "HOLD")
                bundle_rows = cursor.fetchall()
                rows.extend(dict(zip(columns, row)) for row in bundle_rows)
            except pyodbc.Error:
                bundle_warning = "BundleHeader hold rows are unavailable; showing only dbo.WPS hold rows."
        else:
            bundle_warning = "BundleHeader hold rows are unavailable; showing only dbo.WPS hold rows."

    rows.sort(
        key=lambda row: (
            row.get("WorkDate") is None,
            row.get("WorkDate"),
            row.get("Division") or "",
            row.get("Store") or "",
            row.get("Task") or "",
        )
    )
    return rows, bundle_warning


def group_hold_rows_by_work_date(rows):
    grouped_rows = []
    current_label = None
    current_rows = []

    for row in rows:
        work_date = row.get("WorkDate")
        if work_date in (None, ""):
            work_date_label = "Unknown"
        else:
            work_date_label = str(work_date)

        if work_date_label != current_label:
            if current_rows:
                grouped_rows.append((current_label, current_rows))
            current_label = work_date_label
            current_rows = []

        current_rows.append(row)

    if current_rows:
        grouped_rows.append((current_label, current_rows))

    return grouped_rows


def group_paused_rows_by_work_date(rows):
    grouped_rows = []
    current_label = None
    current_rows = []

    for row in rows:
        work_date = row.get("WorkDate")
        if work_date in (None, ""):
            work_date_label = "Unknown"
        else:
            work_date_label = str(work_date)

        if work_date_label != current_label:
            if current_rows:
                grouped_rows.append((current_label, current_rows))
            current_label = work_date_label
            current_rows = []

        current_rows.append(row)

    if current_rows:
        grouped_rows.append((current_label, current_rows))

    return grouped_rows


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


def render_open_work_sections(rows):
    section_parts = []
    for status, title in OPEN_WORK_GROUPS:
        section_rows = [row for row in rows if str(row.get("JobStatus") or "").upper() == status]
        if not section_rows:
            continue
        section_parts.append(f"<h2>{html.escape(title)}</h2>")
        section_parts.append(render_open_work_table(section_rows))
    return "".join(section_parts)


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


def render_incomplete_work_table(rows):
    header_html = "".join(f"<th>{html.escape(column)}</th>" for column in INCOMPLETE_WORK_COLUMNS)
    body_parts = []
    for row in rows:
        cells = "".join(
            f"<td>{display_value(row.get(column))}</td>" for column in INCOMPLETE_WORK_COLUMNS
        )
        body_parts.append(f"<tr>{cells}</tr>")

    if not body_parts:
        colspan = len(INCOMPLETE_WORK_COLUMNS)
        body_parts.append(
            f"<tr><td colspan=\"{colspan}\">No incomplete work rows matched the filter.</td></tr>"
        )

    return (
        "<table>"
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{''.join(body_parts)}</tbody>"
        "</table>"
    )


def _render_reports_header(current_page: str = "Reports", active_section: str = "reports") -> str:
    nav_items = [
        (_wps_production_url("/"), "Operations"),
        (_wps_production_url("/quick-unassigned"), "Quick Unassigned"),
        (_wps_production_url("/quick-in-progress"), "Quick In Progress"),
        (_wps_production_url("/completed-jobs"), "Completed Jobs"),
        (_wps_production_url("/paused-jobs"), "Paused Jobs"),
        (_wps_production_url("/held-jobs"), "Held Orders"),
        (_wps_production_url("/division-progress"), "Division Progress"),
        (_wps_production_url("/performance"), "Performance"),
        (REPORTS_HOME_PATH, "Reports"),
        (_wps_production_url("/admin"), "Admin"),
        (_wps_production_url("/logout"), "Logout"),
    ]
    nav_links = []
    for href, label in nav_items:
        is_active = active_section == "reports" and href == REPORTS_HOME_PATH
        class_name = "reports-nav__link reports-nav__link--active" if is_active else "reports-nav__link"
        nav_links.append(
            f'<a class="{class_name}" href="{html.escape(href)}">{html.escape(label)}</a>'
        )

    return f"""
    <header class="reports-header">
        <div class="reports-header__title-block">
            <div class="reports-header__eyebrow">Workflow Production Scheduler</div>
            <div class="reports-header__title">{html.escape(APP_TITLE)}</div>
        </div>
        <nav class="reports-nav" aria-label="Primary">
            {''.join(nav_links)}
        </nav>
    </header>
    """


def render_page(
    title: str,
    body_html: str,
    print_mode: bool = False,
    show_app_chrome: bool = True,
    nav_href: str | None = REPORTS_HOME_PATH,
    print_href: str | None = "/reports/open-work/print",
) -> HTMLResponse:
    nav_links = [f'<a href="{html.escape(_wps_production_url("/"))}">Back to WPS</a>']
    if nav_href:
        nav_links.insert(0, f'<a href="{html.escape(nav_href)}">Back to Reports</a>')

    print_controls = ""
    if print_mode:
        print_controls = (
            "<div class=\"print-controls no-print\">"
            f"<a class=\"button-link\" href=\"{html.escape(nav_href or REPORTS_HOME_PATH)}\">Back to Report</a>"
            "<button type=\"button\" class=\"button-link\" onclick=\"window.print()\">Print</button>"
            "</div>"
        )
    elif print_href:
        print_controls = f"<p><a href=\"{html.escape(print_href)}\">Print View</a></p>"
    nav = f'<div class="page-nav">{"".join(nav_links)}</div>' if show_app_chrome and nav_links else ""
    header_html = _render_reports_header(title, active_section="reports") if show_app_chrome else ""
    page = f"""
    <!DOCTYPE html>
    <html lang="en">
        <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title>{html.escape(title)}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; color: #111; background: #eef3f8; }}
                .page-shell {{ padding: 24px; }}
                .reports-header {{ padding: 14px 24px 16px; background: linear-gradient(180deg, #0f3558 0%, #1b4f7d 100%); color: #fff; border-bottom: 4px solid #d0a94a; box-shadow: 0 2px 6px rgba(8, 26, 44, 0.18); }}
                .reports-header__title-block {{ margin-bottom: 12px; }}
                .warning {{ padding: 10px 12px; border-radius: 8px; background: #fff3cd; color: #6b4f00; border: 1px solid #f1d58a; }}
                .reports-header__eyebrow {{ font-size: 11px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; color: #c8d8e8; }}
                .reports-header__title {{ font-size: 28px; font-weight: 700; letter-spacing: 0.02em; }}
                .reports-nav {{ display: flex; flex-wrap: wrap; gap: 8px; }}
                .reports-nav__link {{ display: inline-flex; align-items: center; padding: 7px 12px; border: 1px solid rgba(255, 255, 255, 0.26); border-radius: 999px; background: rgba(255, 255, 255, 0.08); color: #f8fbff; text-decoration: none; font-size: 13px; font-weight: 600; line-height: 1; }}
                .reports-nav__link:hover {{ background: rgba(255, 255, 255, 0.18); }}
                .reports-nav__link--active {{ background: #d0a94a; border-color: #d0a94a; color: #102f4d; }}
                h1 {{ margin: 0 0 8px; }}
                p {{ margin: 8px 0; }}
                .page-nav {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 0 0 12px; }}
                .page-nav a {{ color: #1b4f7d; font-weight: 600; text-decoration: none; }}
                .page-nav a:hover {{ text-decoration: underline; }}
                .print-controls {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 0 0 12px; }}
                .button-link {{ display: inline-flex; align-items: center; justify-content: center; padding: 8px 12px; border: 1px solid #1b4f7d; border-radius: 6px; background: #1b4f7d; color: #fff; font: inherit; font-weight: 600; text-decoration: none; cursor: pointer; }}
                .button-link:hover {{ background: #163f63; border-color: #163f63; }}
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
            {header_html}
            <div class="page-shell">
                <h1>{html.escape(title)}</h1>
                {nav}
                {print_controls}
                {body_html}
            </div>
        </body>
    </html>
    """
    return HTMLResponse(page)


@app.get("/", response_class=HTMLResponse)
def reports_home():
    return reports_launcher()


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


@app.get("/reports/", response_class=HTMLResponse)
@app.get("/reports", response_class=HTMLResponse)
def reports_launcher(report_type: str | None = None, date_scope: str | None = None):
    if report_type == "open_work" and date_scope in {value for value, _ in DATE_SCOPE_OPTIONS}:
        return RedirectResponse(
            url=build_url("/reports/open-work", report_type=report_type, date_scope=date_scope),
            status_code=302,
        )
    if report_type == "completed_work" and (
        date_scope in {value for value, _ in DATE_SCOPE_OPTIONS}
        or date_scope == "tomorrow_completed_early"
    ):
        return RedirectResponse(
            url=build_url("/reports/completed-work", report_type=report_type, date_scope=date_scope),
            status_code=302,
        )
    if report_type == "incomplete_work":
        return RedirectResponse(
            url=build_url("/reports/incomplete-work", report_type=report_type, date_scope=date_scope),
            status_code=302,
        )
    if report_type == "paused_work":
        return RedirectResponse(url="/reports/paused-work", status_code=302)
    if report_type == "hold_orders":
        return RedirectResponse(url="/reports/hold-orders", status_code=302)

    report_type_options = ['<option value="" selected>Select</option>']
    for value, label in REPORT_TYPE_OPTIONS:
        selected = " selected" if report_type == value else ""
        report_type_options.append(
            f'<option value="{html.escape(value)}"{selected}>{html.escape(label)}</option>'
        )

    date_scope_options = ['<option value="" selected>Select</option>']
    for value, label in DATE_SCOPE_OPTIONS:
        if report_type == "completed_work" and value in {"tomorrow", "future"}:
            continue
        selected = " selected" if date_scope == value else ""
        date_scope_options.append(
            f'<option value="{html.escape(value)}"{selected}>{html.escape(label)}</option>'
        )

    body_html = f"""
        <p class="meta">Reports Module</p>
        <form method="get" action="/reports" class="launcher">
            <label for="report_type">Report Type</label>
            <select id="report_type" name="report_type" required>
                {''.join(report_type_options)}
            </select>
            <label for="date_scope">Date Scope</label>
            <select id="date_scope" name="date_scope" required data-selected-value="{html.escape(date_scope or '')}">
                {''.join(date_scope_options)}
            </select>
            <button type="submit">Open Report</button>
        </form>
        <script>
            (function () {{
                const reportTypeSelect = document.getElementById("report_type");
                const dateScopeSelect = document.getElementById("date_scope");
                const dateScopeOptionsByReport = {{
                    open_work: [
                        {{ value: "today", label: "Today" }},
                        {{ value: "tomorrow", label: "Tomorrow" }},
                        {{ value: "future", label: "Future" }},
                        {{ value: "all", label: "All" }}
                    ],
                    completed_work: [
                        {{ value: "today", label: "Today" }},
                        {{ value: "tomorrow_completed_early", label: "Tomorrow Completed Early" }},
                        {{ value: "all", label: "All" }}
                    ],
                    paused_work: [
                        {{ value: "", label: "Select" }}
                    ],
                    hold_orders: [
                        {{ value: "", label: "Select" }}
                    ]
                }};

                function updateDateScopeOptions() {{
                    const selectedReportType = reportTypeSelect.value;
                    const previousValue = dateScopeSelect.value || dateScopeSelect.dataset.selectedValue || "";
                    const allowedOptions = dateScopeOptionsByReport[selectedReportType] || [];
                    const requiresDateScope = selectedReportType === "open_work" || selectedReportType === "completed_work";

                    dateScopeSelect.required = requiresDateScope;

                    dateScopeSelect.innerHTML = "";
                    dateScopeSelect.appendChild(new Option("Select", ""));

                    for (const option of allowedOptions) {{
                        dateScopeSelect.appendChild(new Option(option.label, option.value));
                    }}

                    const stillValid = allowedOptions.some((option) => option.value === previousValue);
                    dateScopeSelect.value = stillValid ? previousValue : "";
                    dateScopeSelect.dataset.selectedValue = dateScopeSelect.value;
                }}

                reportTypeSelect.addEventListener("change", updateDateScopeOptions);
                dateScopeSelect.addEventListener("change", function () {{
                    dateScopeSelect.dataset.selectedValue = dateScopeSelect.value;
                }});
                updateDateScopeOptions();
            }})();
        </script>
    """
    return render_page(APP_TITLE, body_html, nav_href=None, print_href=None)


@app.get("/reports/open-work", response_class=HTMLResponse)
def open_work_report(date_scope: str | None = None, report_type: str | None = None):
    rows = fetch_open_work_rows(date_scope=date_scope)
    active_scope = next(
        (label for value, label in DATE_SCOPE_OPTIONS if value == date_scope),
        "Not specified",
    )
    scope_label = active_scope
    if date_scope == "today":
        scope_label = "Today + Carryover"
    body_html = (
        "<p class=\"meta\">Showing open work from dbo.WPS for statuses: "
        + ", ".join(html.escape(status) for status in OPEN_WORK_STATUSES)
        + f". Date Scope: {html.escape(scope_label)}.</p>"
        + render_open_work_sections(rows)
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
    scope_label = active_scope
    if date_scope == "today":
        scope_label = "Today + Carryover"
    body_html = (
        "<p class=\"meta\">Print-friendly open work report from dbo.WPS. "
        + f"Date Scope: {html.escape(scope_label)}.</p>"
        + render_open_work_sections(rows)
    )
    return render_page(
        "Open Work Print",
        body_html,
        print_mode=True,
        show_app_chrome=False,
        nav_href=build_url(
            "/reports/open-work",
            report_type=report_type or "open_work",
            date_scope=date_scope,
        ),
    )


@app.get("/reports/paused-work", response_class=HTMLResponse)
def paused_work_report():
    rows, bundle_warning = fetch_paused_work_rows()
    grouped = group_paused_rows_by_work_date(rows)

    group_parts = []
    for work_date_label, group_rows in grouped:
        display_label = work_date_label
        if work_date_label != "Unknown":
            display_label = group_rows[0].get("WorkDate").strftime("%m/%d/%Y")
        group_parts.append(f"<h2>Work Date: {html.escape(display_label)}</h2>")
        group_parts.append(render_open_work_table(group_rows))

    body_html = (
        (f'<p class="warning">{html.escape(bundle_warning)}</p>' if bundle_warning else "")
        +
        "<p class=\"meta\">Showing all currently paused jobs grouped by original Work Date.</p>"
        + "".join(group_parts)
    )
    return render_page(
        "Paused Work",
        body_html,
        nav_href="/reports",
        print_href="/reports/paused-work/print",
    )


@app.get("/reports/paused-work/print", response_class=HTMLResponse)
def paused_work_report_print():
    rows, bundle_warning = fetch_paused_work_rows()
    grouped = group_paused_rows_by_work_date(rows)

    group_parts = []
    for work_date_label, group_rows in grouped:
        display_label = work_date_label
        if work_date_label != "Unknown":
            display_label = group_rows[0].get("WorkDate").strftime("%m/%d/%Y")
        group_parts.append(f"<h2>Work Date: {html.escape(display_label)}</h2>")
        group_parts.append(render_open_work_table(group_rows))

    body_html = (
        (f'<p class="warning">{html.escape(bundle_warning)}</p>' if bundle_warning else "")
        +
        "<p class=\"meta\">Showing all currently paused jobs grouped by original Work Date.</p>"
        + "".join(group_parts)
    )
    return render_page(
        "Paused Work Print",
        body_html,
        print_mode=True,
        show_app_chrome=False,
        nav_href="/reports/paused-work",
    )


@app.get("/reports/hold-orders", response_class=HTMLResponse)
def hold_orders_report():
    rows, bundle_warning = fetch_hold_order_rows()
    grouped = group_hold_rows_by_work_date(rows)

    group_parts = []
    for work_date_label, group_rows in grouped:
        display_label = work_date_label
        if work_date_label != "Unknown":
            display_label = group_rows[0].get("WorkDate").strftime("%m/%d/%Y")
        group_parts.append(f"<h2>Work Date: {html.escape(display_label)}</h2>")
        group_parts.append(render_open_work_table(group_rows))

    body_html = (
        (f'<p class="warning">{html.escape(bundle_warning)}</p>' if bundle_warning else "")
        +
        "<p class=\"meta\">Showing all held orders grouped by original Work Date.</p>"
        + "".join(group_parts)
    )
    return render_page(
        "Hold Orders",
        body_html,
        nav_href="/reports",
        print_href="/reports/hold-orders/print",
    )


@app.get("/reports/hold-orders/print", response_class=HTMLResponse)
def hold_orders_report_print():
    rows, bundle_warning = fetch_hold_order_rows()
    grouped = group_hold_rows_by_work_date(rows)

    group_parts = []
    for work_date_label, group_rows in grouped:
        display_label = work_date_label
        if work_date_label != "Unknown":
            display_label = group_rows[0].get("WorkDate").strftime("%m/%d/%Y")
        group_parts.append(f"<h2>Work Date: {html.escape(display_label)}</h2>")
        group_parts.append(render_open_work_table(group_rows))

    body_html = (
        (f'<p class="warning">{html.escape(bundle_warning)}</p>' if bundle_warning else "")
        +
        "<p class=\"meta\">Showing all held orders grouped by original Work Date.</p>"
        + "".join(group_parts)
    )
    return render_page(
        "Hold Orders Print",
        body_html,
        print_mode=True,
        show_app_chrome=False,
        nav_href="/reports/hold-orders",
    )


@app.get("/reports/completed-work", response_class=HTMLResponse)
def completed_work_report(date_scope: str | None = None, report_type: str | None = None):
    rows = fetch_completed_work_rows(date_scope=date_scope)
    active_scope = next(
        (label for value, label in DATE_SCOPE_OPTIONS if value == date_scope),
        "Not specified",
    )
    if date_scope == "tomorrow_completed_early":
        active_scope = "Tomorrow Completed Early"
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
    if date_scope == "tomorrow_completed_early":
        active_scope = "Tomorrow Completed Early"
    generated_at = __import__("datetime").datetime.now().strftime("%m/%d/%Y %I:%M %p")
    body_html = (
        """
        <style>
            table { font-size: 12px; }
            th, td { padding: 4px 6px; }
            th { background: #f2f2f2; }
            .print-meta { margin: 0 0 12px 0; font-size: 13px; color: #444; }
            .print-meta p { margin: 2px 0; }
        </style>
        <div class="print-meta">
        """
        + f"<p>Date Scope: {html.escape(active_scope)}</p>"
        + f"<p>Generated: {html.escape(generated_at)}</p>"
        + "</div>"
        + render_completed_work_table(rows)
    )
    return render_page(
        "Completed Work Report",
        body_html,
        print_mode=True,
        show_app_chrome=False,
        nav_href=build_url(
            "/reports/completed-work",
            report_type=report_type or "completed_work",
            date_scope=date_scope,
        ),
    )


@app.get("/reports/incomplete-work", response_class=HTMLResponse)
def incomplete_work_report(date_scope: str | None = None, report_type: str | None = None):
    rows = fetch_incomplete_work_rows(date_scope=date_scope)
    active_scope = next(
        (label for value, label in DATE_SCOPE_OPTIONS if value == date_scope),
        "All",
    )
    body_html = (
        "<p class=\"meta\">Showing incomplete work from dbo.WPS where JobStatus is not completed. "
        + f"Date Scope: {html.escape(active_scope)}.</p>"
        + render_incomplete_work_table(rows)
    )
    return render_page(
        "Incomplete Work",
        body_html,
        nav_href="/reports",
        print_href=build_url(
            "/reports/incomplete-work/print",
            report_type=report_type or "incomplete_work",
            date_scope=date_scope,
        ),
    )


@app.get("/reports/incomplete-work/print", response_class=HTMLResponse)
def incomplete_work_report_print(date_scope: str | None = None, report_type: str | None = None):
    rows = fetch_incomplete_work_rows(date_scope=date_scope)
    active_scope = next(
        (label for value, label in DATE_SCOPE_OPTIONS if value == date_scope),
        "All",
    )
    body_html = (
        "<p class=\"meta\">Showing incomplete work from dbo.WPS where JobStatus is not completed. "
        + f"Date Scope: {html.escape(active_scope)}.</p>"
        + render_incomplete_work_table(rows)
    )
    return render_page(
        "Incomplete Work Print",
        body_html,
        print_mode=True,
        show_app_chrome=False,
        nav_href=build_url(
            "/reports/incomplete-work",
            report_type=report_type or "incomplete_work",
            date_scope=date_scope,
        ),
    )
