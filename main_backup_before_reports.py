import os
import pyodbc
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv

load_dotenv("/workspaces/WPS_REPORTS_DEV/.env", override=False)

app = FastAPI(title="WPS Reports DEV")


def get_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('AZURE_SQL_SERVER')};"
        f"DATABASE={os.getenv('AZURE_SQL_DATABASE')};"
        f"UID={os.getenv('AZURE_SQL_USERNAME')};"
        f"PWD={os.getenv('AZURE_SQL_PASSWORD')};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


@app.get("/", response_class=HTMLResponse)
def reports_home():
    return """
    <html>
        <body style="font-family: Arial; padding: 40px;">
            <h1>WPS Reports DEV</h1>
            <p>Separate read-only reports app is running.</p>
            <p><a href="/health">Run health check</a></p>
            <p><a href="/open-work">Open Work (next step)</a></p>
        </body>
    </html>
    """


@app.get("/health", response_class=JSONResponse)
def health_check():
    env_file_db = None
    try:
        with open("/workspaces/WPS_REPORTS_DEV/.env", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("AZURE_SQL_DATABASE="):
                    env_file_db = line.strip().split("=", 1)[1]
                    break
    except Exception:
        env_file_db = None

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS test_value")
        row = cursor.fetchone()
        conn.close()

        return {
            "ok": True,
            "app": "WPS Reports DEV",
            "env_file_database": env_file_db,
            "runtime_database": os.getenv("AZURE_SQL_DATABASE"),
            "test_value": row.test_value,
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "app": "WPS Reports DEV",
                "env_file_database": env_file_db,
                "runtime_database": os.getenv("AZURE_SQL_DATABASE"),
                "error": str(e),
            },
        )
