# Jira → Snowflake Sync

Incrementally syncs all Jira tickets — all fields — into a Snowflake table. Runs on a schedule and only pulls tickets that have changed since the last run.

---

## Requirements

- **Python 3.8 or higher** — check with `python --version`
- **Windows users**: if `pip install` fails, first install [Microsoft Visual C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) — required by the Snowflake connector

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure credentials

Copy the example config and fill in your values:

```bash
cp config.example.json config.json
```

Open `config.json` and fill in:

**Jira section**
| Field | Description |
|---|---|
| `base_url` | Your Jira URL, e.g. `https://yourcompany.atlassian.net` |
| `email` | Your Jira login email |
| `token` | Your Jira API token — get one at [id.atlassian.com](https://id.atlassian.com/manage-profile/security/api-tokens) |

**Snowflake section** — see [TECHNICAL.md](TECHNICAL.md#snowflake-connection) for detail on each value.

| Field | Description |
|---|---|
| `account` | Your Snowflake account identifier |
| `user` | Your Snowflake username |
| `password` | Your Snowflake password |
| `warehouse` | Compute warehouse to use |
| `database` | Database to write into |
| `schema` | Schema inside that database |
| `role` | Role with write access to the table |

**Sync section**
| Field | Description |
|---|---|
| `projects` | List of project keys to sync, e.g. `["ACS", "LPM"]`. Leave as `[]` to sync all projects. |
| `batch_size` | Tickets fetched per API call. Default `100`, max `100`. |

### 3. Run

```bash
# Sync all projects (incremental — only changed tickets)
python sync.py

# Sync one specific project
python sync.py --project ACS

# Re-pull everything from scratch (ignores last sync time)
python sync.py --full
```

---

## Scheduling

### Mac / Linux (cron)

Run every 15 minutes:

```bash
crontab -e
```

Add this line (update the path):

```
*/15 * * * * /usr/bin/python3 /path/to/jira-snowflake-sync/sync.py >> /path/to/sync.log 2>&1
```

### Windows (Task Scheduler)

1. Open **Task Scheduler** → **Create Basic Task**
2. Trigger: **Daily**, repeat every **15 minutes**
3. Action: **Start a program**
   - Program: `python.exe`
   - Arguments: `C:\path\to\jira-snowflake-sync\sync.py`

---

## Querying data in Snowflake

Every ticket lands in the `JIRA_ISSUES` table with dedicated columns for common fields plus a `RAW_FIELDS` column (Snowflake `VARIANT` type) containing the full Jira payload.

```sql
-- Basic query
SELECT ISSUE_KEY, SUMMARY, STATUS, ASSIGNEE, UPDATED
FROM JIRA_ISSUES
WHERE PROJECT_KEY = 'ACS'
ORDER BY UPDATED DESC;

-- Query a custom field from RAW_FIELDS
SELECT
    ISSUE_KEY,
    RAW_FIELDS:customfield_10151::string AS health_plan,
    RAW_FIELDS:customfield_10356::string AS category
FROM JIRA_ISSUES;

-- Find all open tickets by project
SELECT PROJECT_KEY, COUNT(*) AS open_tickets
FROM JIRA_ISSUES
WHERE STATUS != 'Done'
GROUP BY PROJECT_KEY
ORDER BY open_tickets DESC;
```

---

## Files

| File | Purpose |
|---|---|
| `sync.py` | Main sync script |
| `config.json` | Your credentials (gitignored — never committed) |
| `config.example.json` | Template for config.json |
| `sync_state.json` | Auto-generated — tracks last sync time per project |
| `requirements.txt` | Python dependencies |
| `TECHNICAL.md` | Full technical documentation |
