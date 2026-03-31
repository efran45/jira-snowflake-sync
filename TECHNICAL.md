# Technical Documentation — Jira → Snowflake Sync

## Overview

`sync.py` is a single-file Python ETL script. It extracts ticket data from the Jira REST API, transforms it into a flat structure, and loads it into Snowflake using a MERGE (upsert) statement. It is designed to run repeatedly on a schedule and only process data that has changed since the last run.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          sync.py                                │
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌──────────────────┐   │
│  │   EXTRACT   │ →  │  TRANSFORM  │ →  │      LOAD        │   │
│  │             │    │             │    │                  │   │
│  │  Jira API   │    │  flatten +  │    │  Snowflake MERGE │   │
│  │  (REST v3)  │    │  normalize  │    │  into JIRA_ISSUES│   │
│  └─────────────┘    └─────────────┘    └──────────────────┘   │
│                                                                 │
│  ┌──────────────────────────────────────┐                      │
│  │  sync_state.json                     │                      │
│  │  { "ACS": "2026-03-31T14:00:00Z" }  │  ← last run per      │
│  │  { "LPM": "2026-03-31T14:00:00Z" }  │    project           │
│  └──────────────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
```

The three layers are clearly separated in `sync.py` and are independently modifiable.

---

## How Incremental Sync Works

On every run the script checks `sync_state.json` for the last successful sync timestamp for each project. It then queries Jira with:

```
project = ACS AND updated >= "2026-03-31T14:00:00Z" ORDER BY updated ASC
```

Only tickets modified after that timestamp are returned. Once all tickets are written to Snowflake, the timestamp is updated. If the script crashes mid-run, the timestamp is **not** updated, so the next run will re-fetch and re-upsert the same tickets safely.

On first run (or with `--full`), there is no timestamp filter and every ticket in the project is pulled.

---

## Extract Layer — Jira API

### Authentication

Jira uses HTTP Basic Auth with a Base64-encoded `email:token` string passed as the `Authorization` header. API tokens are generated at [id.atlassian.com](https://id.atlassian.com/manage-profile/security/api-tokens).

### Endpoint

```
GET /rest/api/3/search/jql
```

This is Jira Cloud's cursor-based search endpoint. It does not return a `total` count — instead it returns `isLast: true/false` and a `nextPageToken` for the next page.

### Pagination

The script loops until `isLast` is `true`:

```
Request 1:  ?jql=...&maxResults=100
            → { issues: [...100 tickets], isLast: false, nextPageToken: "abc" }

Request 2:  ?jql=...&maxResults=100&nextPageToken=abc
            → { issues: [...100 tickets], isLast: false, nextPageToken: "def" }

Request N:  ?jql=...&maxResults=100&nextPageToken=xyz
            → { issues: [...43 tickets], isLast: true }
```

### Fields

Every request uses `fields=*all` to return every field on the ticket — system fields and all custom fields — without needing to specify them individually.

---

## Transform Layer

### `extract_text(value)`

Jira fields come back in many different shapes depending on type:

| Field type | Jira response | Extracted value |
|---|---|---|
| Text field | `"In Progress"` | `"In Progress"` |
| User field | `{"displayName": "Jane Smith", "email": "..."}` | `"Jane Smith"` |
| Select field | `{"value": "Bug", "id": "1"}` | `"Bug"` |
| Multi-select | `[{"value": "A"}, {"value": "B"}]` | `"A, B"` |
| Empty | `null` | `None` |

`extract_text()` normalises all of these into a single string (or `None`).

### `transform(issue)`

Produces a flat dict with:

- **Dedicated columns** for the most commonly queried fields (`STATUS`, `ASSIGNEE`, `ISSUE_TYPE`, etc.) — stored as typed Snowflake columns for fast filtering and joining.
- **`RAW_FIELDS`** — the complete `fields` payload from Jira serialized as JSON. Stored as Snowflake `VARIANT`. Nothing is lost; every custom field is accessible here.
- **`SYNCED_AT`** — UTC timestamp of when this row was written, useful for auditing.

---

## Load Layer — Snowflake

### Snowflake Connection

Values required in `config.json → snowflake`:

| Key | What it is | Where to find it |
|---|---|---|
| `account` | Account identifier | Snowflake UI → bottom-left menu → copy account identifier. Format: `orgname-accountname` |
| `user` | Login username | Your Snowflake username |
| `password` | Login password | Your Snowflake password |
| `warehouse` | Compute warehouse | Snowflake UI → Admin → Warehouses |
| `database` | Target database | Snowflake UI → Data → Databases |
| `schema` | Schema inside database | e.g. `PUBLIC` or `RAW` |
| `role` | Role with write access | Snowflake UI → Admin → Users and Roles. Must have `INSERT` and `UPDATE` on the table. `SYSADMIN` works for setup. |

### Table: `JIRA_ISSUES`

Created automatically on first run via `CREATE TABLE IF NOT EXISTS`.

| Column | Type | Description |
|---|---|---|
| `ISSUE_KEY` | `VARCHAR(50)` | Primary key. e.g. `ACS-1234` |
| `ISSUE_ID` | `VARCHAR(50)` | Internal Jira numeric ID |
| `PROJECT_KEY` | `VARCHAR(20)` | e.g. `ACS` |
| `SUMMARY` | `VARCHAR(2000)` | Ticket title |
| `ISSUE_TYPE` | `VARCHAR(100)` | e.g. `Bug`, `Story`, `Task` |
| `STATUS` | `VARCHAR(100)` | e.g. `In Progress`, `Done` |
| `PRIORITY` | `VARCHAR(50)` | e.g. `High`, `Medium` |
| `ASSIGNEE` | `VARCHAR(200)` | Display name of assignee |
| `REPORTER` | `VARCHAR(200)` | Display name of reporter |
| `LABELS` | `VARCHAR(2000)` | JSON array of label strings |
| `COMPONENTS` | `VARCHAR(2000)` | Comma-separated component names |
| `FIX_VERSIONS` | `VARCHAR(2000)` | Comma-separated fix version names |
| `RESOLUTION` | `VARCHAR(200)` | e.g. `Fixed`, `Won't Fix` |
| `CREATED` | `TIMESTAMP_TZ` | Ticket creation time |
| `UPDATED` | `TIMESTAMP_TZ` | Last modification time |
| `RESOLVED` | `TIMESTAMP_TZ` | Resolution time (null if open) |
| `DUE_DATE` | `DATE` | Due date if set |
| `RAW_FIELDS` | `VARIANT` | Full Jira fields payload as JSON |
| `SYNCED_AT` | `TIMESTAMP_TZ` | When this row was last written |

### Querying Custom Fields

Any field not in the dedicated columns can be queried from `RAW_FIELDS`:

```sql
-- Single custom field
SELECT
    ISSUE_KEY,
    RAW_FIELDS:customfield_10151::string AS health_plan
FROM JIRA_ISSUES;

-- Nested value (e.g. a select field)
SELECT
    ISSUE_KEY,
    RAW_FIELDS:customfield_10356:value::string AS category
FROM JIRA_ISSUES;

-- Check if a field has any value
SELECT ISSUE_KEY
FROM JIRA_ISSUES
WHERE RAW_FIELDS:customfield_10151 IS NOT NULL;
```

### Upsert (MERGE)

The script uses Snowflake's `MERGE` statement so it is safe to re-run at any time:

- If `ISSUE_KEY` already exists → **UPDATE** all columns
- If `ISSUE_KEY` does not exist → **INSERT** new row
- No duplicates are ever created

---

## Sync State

`sync_state.json` is auto-generated and tracks the last successful sync timestamp per project:

```json
{
  "ACS": "2026-03-31T14:00:00.000000+00:00",
  "LPM": "2026-03-31T14:00:00.000000+00:00"
}
```

- **Delete this file** to trigger a full re-sync of all projects on next run
- **Delete a single key** to re-sync just that project
- Use `python sync.py --full` to force a full re-sync without editing the file

---

## Error Handling

- If a project sync fails (network error, Jira API error, Snowflake error), the error is logged and the script moves on to the next project. The failed project's `sync_state.json` timestamp is **not** updated, so it will be retried in full on the next run.
- If `config.json` is missing the script exits immediately with a clear error message.
- Snowflake connection errors will stop the entire run since no writes are possible.

---

## Dependency Reference

| Package | Purpose |
|---|---|
| `requests` | HTTP client for Jira API calls |
| `snowflake-connector-python` | Official Snowflake Python driver |
