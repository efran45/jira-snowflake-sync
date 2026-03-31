"""
Jira → Snowflake Sync
=====================
Incrementally syncs all Jira tickets (all fields) into a Snowflake table.

Run manually:      python sync.py
Run for one project: python sync.py --project ACS
Full re-sync:      python sync.py --full

Schedule via cron (every 15 minutes):
    */15 * * * * /usr/bin/python3 /path/to/sync.py >> /path/to/sync.log 2>&1

Schedule via Windows Task Scheduler:
    Action: python.exe C:\\path\\to\\sync.py
    Trigger: On a schedule, every 15 minutes
"""

import argparse
import json
import logging
import sys
from base64 import b64encode
from datetime import datetime, timezone
from pathlib import Path

import requests
import snowflake.connector

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).parent
CONFIG     = ROOT / "config.json"
SYNC_STATE = ROOT / "sync_state.json"   # tracks last successful sync per project


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

def load_config() -> dict:
    if not CONFIG.exists():
        log.error("config.json not found. Copy config.example.json → config.json and fill in your values.")
        sys.exit(1)
    return json.loads(CONFIG.read_text())


# ══════════════════════════════════════════════════════════════════════════════
# SYNC STATE  (tracks last run timestamp per project so we only pull changes)
# ══════════════════════════════════════════════════════════════════════════════

def load_state() -> dict:
    if SYNC_STATE.exists():
        return json.loads(SYNC_STATE.read_text())
    return {}


def save_state(state: dict):
    SYNC_STATE.write_text(json.dumps(state, indent=2))


def get_last_sync(state: dict, project_key: str) -> str | None:
    """Return ISO timestamp of last successful sync for this project, or None."""
    return state.get(project_key)


def set_last_sync(state: dict, project_key: str, ts: str):
    state[project_key] = ts
    save_state(state)


# ══════════════════════════════════════════════════════════════════════════════
# JIRA  (extraction layer — do not modify when switching to Snowflake)
# ══════════════════════════════════════════════════════════════════════════════

def jira_headers(email: str, token: str) -> dict:
    creds = b64encode(f"{email}:{token}".encode()).decode()
    return {"Authorization": f"Basic {creds}", "Accept": "application/json"}


def fetch_projects(cfg: dict) -> list[str]:
    """Return list of project keys. Uses config projects list if set, else all projects."""
    configured = cfg["sync"].get("projects", [])
    if configured:
        return configured

    log.info("No projects configured — fetching all projects from Jira...")
    r = requests.get(
        f"{cfg['jira']['base_url'].rstrip('/')}/rest/api/3/project",
        headers=jira_headers(cfg["jira"]["email"], cfg["jira"]["token"]),
        timeout=30,
    )
    r.raise_for_status()
    keys = [p["key"] for p in r.json()]
    log.info(f"Found {len(keys)} projects: {', '.join(keys)}")
    return keys


def fetch_issues(cfg: dict, project_key: str, updated_since: str | None) -> list[dict]:
    """
    Fetch all issues for a project updated after `updated_since`.
    Uses cursor pagination (/rest/api/3/search/jql).
    Returns raw issue dicts with all fields.
    """
    base    = cfg["jira"]["base_url"].rstrip("/")
    headers = jira_headers(cfg["jira"]["email"], cfg["jira"]["token"])
    batch   = cfg["sync"].get("batch_size", 100)
    url     = f"{base}/rest/api/3/search/jql"

    # Build JQL — incremental if we have a last sync time, else full project
    if updated_since:
        jql = f'project = {project_key} AND updated >= "{updated_since}" ORDER BY updated ASC'
        log.info(f"[{project_key}] Incremental sync from {updated_since}")
    else:
        jql = f"project = {project_key} ORDER BY updated ASC"
        log.info(f"[{project_key}] Full sync (first run)")

    issues     = []
    next_token = None

    while True:
        params = {"jql": jql, "maxResults": batch, "fields": "*all"}
        if next_token:
            params["nextPageToken"] = next_token

        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data  = r.json()
        batch_issues = data.get("issues", [])
        issues.extend(batch_issues)

        log.info(f"[{project_key}] Fetched {len(issues)} tickets so far...")

        if data.get("isLast", True) or not batch_issues:
            break
        next_token = data.get("nextPageToken")

    log.info(f"[{project_key}] Total fetched: {len(issues)}")
    return issues


# ══════════════════════════════════════════════════════════════════════════════
# TRANSFORM  (flatten a raw Jira issue into a Snowflake-ready row)
# ══════════════════════════════════════════════════════════════════════════════

def extract_text(value) -> str | None:
    """Pull a display string out of common Jira field structures."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("displayName", "name", "value", "key"):
            if key in value and value[key] is not None:
                return str(value[key])
    if isinstance(value, list):
        return ", ".join(extract_text(v) for v in value if v is not None)
    return str(value)


def transform(issue: dict) -> dict:
    """
    Returns a flat dict ready for Snowflake.

    Dedicated columns hold the most-queried fields.
    RAW_FIELDS (VARIANT) holds the complete fields payload so nothing is lost.
    """
    fields = issue.get("fields", {})

    return {
        # ── Identity ──────────────────────────────────────────────────────────
        "ISSUE_KEY":    issue.get("key"),
        "ISSUE_ID":     issue.get("id"),
        "PROJECT_KEY":  issue.get("key", "").split("-")[0],

        # ── Common fields ─────────────────────────────────────────────────────
        "SUMMARY":      fields.get("summary"),
        "ISSUE_TYPE":   extract_text(fields.get("issuetype")),
        "STATUS":       extract_text(fields.get("status")),
        "PRIORITY":     extract_text(fields.get("priority")),
        "ASSIGNEE":     extract_text(fields.get("assignee")),
        "REPORTER":     extract_text(fields.get("reporter")),
        "LABELS":       json.dumps(fields.get("labels", [])),
        "COMPONENTS":   extract_text(fields.get("components")),
        "FIX_VERSIONS": extract_text(fields.get("fixVersions")),
        "RESOLUTION":   extract_text(fields.get("resolution")),

        # ── Dates ─────────────────────────────────────────────────────────────
        "CREATED":      fields.get("created"),
        "UPDATED":      fields.get("updated"),
        "RESOLVED":     fields.get("resolutiondate"),
        "DUE_DATE":     fields.get("duedate"),

        # ── Full payload (maps to Snowflake VARIANT column) ───────────────────
        # Query any field via: RAW_FIELDS:customfield_10151::string
        "RAW_FIELDS":   json.dumps(fields),

        # ── Sync metadata ─────────────────────────────────────────────────────
        "SYNCED_AT":    datetime.now(timezone.utc).isoformat(),
    }


# ══════════════════════════════════════════════════════════════════════════════
# SNOWFLAKE  (write layer — this is the only section to update when connecting)
# ══════════════════════════════════════════════════════════════════════════════

# ┌─────────────────────────────────────────────────────────────────────────┐
# │  SNOWFLAKE CONNECTION                                                   │
# │                                                                         │
# │  Fill in config.json → snowflake section:                               │
# │                                                                         │
# │  account   — Your Snowflake account identifier.                         │
# │              Format: <orgname>-<account_name>  (e.g. myorg-myaccount)  │
# │              Found in: Snowflake UI → bottom-left account menu          │
# │                                                                         │
# │  user      — Your Snowflake login username                              │
# │                                                                         │
# │  password  — Your Snowflake password                                    │
# │              (consider using key-pair auth for production)              │
# │                                                                         │
# │  warehouse — Compute warehouse to use (e.g. COMPUTE_WH)                │
# │              Found in: Snowflake UI → Admin → Warehouses                │
# │                                                                         │
# │  database  — Database to write to (e.g. JIRA_DB)                       │
# │                                                                         │
# │  schema    — Schema inside that database (e.g. PUBLIC or RAW)          │
# │                                                                         │
# │  role      — Role that has INSERT/MERGE rights on the table             │
# │              (e.g. SYSADMIN or a custom ETL role)                       │
# └─────────────────────────────────────────────────────────────────────────┘

def snowflake_connect(cfg: dict):
    sf = cfg["snowflake"]
    return snowflake.connector.connect(
        account   = sf["account"],
        user      = sf["user"],
        password  = sf["password"],
        warehouse = sf["warehouse"],
        database  = sf["database"],
        schema    = sf["schema"],
        role      = sf["role"],
    )


# ┌─────────────────────────────────────────────────────────────────────────┐
# │  TABLE SETUP                                                            │
# │                                                                         │
# │  Run this once to create the table in Snowflake.                        │
# │  RAW_FIELDS is VARIANT — Snowflake's native JSON type.                  │
# │  Query any field from it: RAW_FIELDS:customfield_10151::string          │
# └─────────────────────────────────────────────────────────────────────────┘

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS JIRA_ISSUES (
    ISSUE_KEY    VARCHAR(50)   NOT NULL PRIMARY KEY,
    ISSUE_ID     VARCHAR(50),
    PROJECT_KEY  VARCHAR(20),
    SUMMARY      VARCHAR(2000),
    ISSUE_TYPE   VARCHAR(100),
    STATUS       VARCHAR(100),
    PRIORITY     VARCHAR(50),
    ASSIGNEE     VARCHAR(200),
    REPORTER     VARCHAR(200),
    LABELS       VARCHAR(2000),
    COMPONENTS   VARCHAR(2000),
    FIX_VERSIONS VARCHAR(2000),
    RESOLUTION   VARCHAR(200),
    CREATED      TIMESTAMP_TZ,
    UPDATED      TIMESTAMP_TZ,
    RESOLVED     TIMESTAMP_TZ,
    DUE_DATE     DATE,
    RAW_FIELDS   VARIANT,
    SYNCED_AT    TIMESTAMP_TZ
);
"""

# ┌─────────────────────────────────────────────────────────────────────────┐
# │  UPSERT                                                                 │
# │                                                                         │
# │  Uses Snowflake MERGE to insert new tickets and update changed ones.    │
# │  ISSUE_KEY is the unique identifier — safe to re-run at any time.       │
# └─────────────────────────────────────────────────────────────────────────┘

MERGE_SQL = """
MERGE INTO JIRA_ISSUES AS target
USING (
    SELECT
        %(ISSUE_KEY)s    AS ISSUE_KEY,
        %(ISSUE_ID)s     AS ISSUE_ID,
        %(PROJECT_KEY)s  AS PROJECT_KEY,
        %(SUMMARY)s      AS SUMMARY,
        %(ISSUE_TYPE)s   AS ISSUE_TYPE,
        %(STATUS)s       AS STATUS,
        %(PRIORITY)s     AS PRIORITY,
        %(ASSIGNEE)s     AS ASSIGNEE,
        %(REPORTER)s     AS REPORTER,
        %(LABELS)s       AS LABELS,
        %(COMPONENTS)s   AS COMPONENTS,
        %(FIX_VERSIONS)s AS FIX_VERSIONS,
        %(RESOLUTION)s   AS RESOLUTION,
        %(CREATED)s      AS CREATED,
        %(UPDATED)s      AS UPDATED,
        %(RESOLVED)s     AS RESOLVED,
        %(DUE_DATE)s     AS DUE_DATE,
        PARSE_JSON(%(RAW_FIELDS)s) AS RAW_FIELDS,
        %(SYNCED_AT)s    AS SYNCED_AT
) AS source ON target.ISSUE_KEY = source.ISSUE_KEY
WHEN MATCHED THEN UPDATE SET
    ISSUE_ID     = source.ISSUE_ID,
    PROJECT_KEY  = source.PROJECT_KEY,
    SUMMARY      = source.SUMMARY,
    ISSUE_TYPE   = source.ISSUE_TYPE,
    STATUS       = source.STATUS,
    PRIORITY     = source.PRIORITY,
    ASSIGNEE     = source.ASSIGNEE,
    REPORTER     = source.REPORTER,
    LABELS       = source.LABELS,
    COMPONENTS   = source.COMPONENTS,
    FIX_VERSIONS = source.FIX_VERSIONS,
    RESOLUTION   = source.RESOLUTION,
    CREATED      = source.CREATED,
    UPDATED      = source.UPDATED,
    RESOLVED     = source.RESOLVED,
    DUE_DATE     = source.DUE_DATE,
    RAW_FIELDS   = source.RAW_FIELDS,
    SYNCED_AT    = source.SYNCED_AT
WHEN NOT MATCHED THEN INSERT (
    ISSUE_KEY, ISSUE_ID, PROJECT_KEY, SUMMARY, ISSUE_TYPE, STATUS,
    PRIORITY, ASSIGNEE, REPORTER, LABELS, COMPONENTS, FIX_VERSIONS,
    RESOLUTION, CREATED, UPDATED, RESOLVED, DUE_DATE, RAW_FIELDS, SYNCED_AT
) VALUES (
    source.ISSUE_KEY, source.ISSUE_ID, source.PROJECT_KEY, source.SUMMARY,
    source.ISSUE_TYPE, source.STATUS, source.PRIORITY, source.ASSIGNEE,
    source.REPORTER, source.LABELS, source.COMPONENTS, source.FIX_VERSIONS,
    source.RESOLUTION, source.CREATED, source.UPDATED, source.RESOLVED,
    source.DUE_DATE, source.RAW_FIELDS, source.SYNCED_AT
);
"""


def ensure_table(conn):
    conn.cursor().execute(CREATE_TABLE_SQL)
    log.info("Table JIRA_ISSUES ready.")


def upsert_batch(conn, rows: list[dict]):
    """Write a batch of transformed rows to Snowflake."""
    cur = conn.cursor()
    cur.executemany(MERGE_SQL, rows)
    log.info(f"Upserted {len(rows)} rows.")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN SYNC LOOP
# ══════════════════════════════════════════════════════════════════════════════

def sync_project(conn, cfg: dict, state: dict, project_key: str, full: bool):
    last_sync = None if full else get_last_sync(state, project_key)
    sync_start = datetime.now(timezone.utc).isoformat()

    issues = fetch_issues(cfg, project_key, updated_since=last_sync)
    if not issues:
        log.info(f"[{project_key}] Nothing new to sync.")
        return

    # Transform and write in batches of 500
    batch_size = 500
    rows = [transform(i) for i in issues]
    for i in range(0, len(rows), batch_size):
        upsert_batch(conn, rows[i:i + batch_size])

    set_last_sync(state, project_key, sync_start)
    log.info(f"[{project_key}] Sync complete — {len(rows)} tickets processed.")


def main():
    parser = argparse.ArgumentParser(description="Jira → Snowflake sync")
    parser.add_argument("--project", help="Sync a single project key")
    parser.add_argument("--full", action="store_true",
                        help="Ignore last sync time and re-pull everything")
    args = parser.parse_args()

    cfg   = load_config()
    state = load_state()

    log.info("Connecting to Snowflake...")
    conn = snowflake_connect(cfg)
    ensure_table(conn)

    projects = [args.project] if args.project else fetch_projects(cfg)

    for project_key in projects:
        try:
            sync_project(conn, cfg, state, project_key, full=args.full)
        except Exception as e:
            log.error(f"[{project_key}] Sync failed: {e}")

    conn.close()
    log.info("All done.")


if __name__ == "__main__":
    main()
