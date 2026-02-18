# =========================
# dynamic_jira_etl.py
# =========================

import requests
import json
from loguru import logger
from datetime import datetime
from collections import defaultdict
from sqlalchemy import create_engine, text
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# CONFIG DYNAMIC TABLES
# =========================
DYNAMIC_TABLES = {
    "jira_subtasks": "fields.subtasks",
    "jira_changelog": "changelog.histories"
}


# =========================
# FETCH JIRA ISSUES
# =========================
class FetchJiraIssues:

    def __init__(self, config):
        self.jira_username = config.jira_username
        self.jira_api_key = config.jira_api_key
        self.jira_endpoint = config.jira_endpoint
        self.jira_issues_jql = config.jira_issues_jql
        self.jira_epics_jql = config.jira_epics_jql
        self.results_per_page = 100

        # Session con retry automático (MEJORA CRÍTICA)
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def get_issues(self):
        return self.__fetch_all_results(self.jira_issues_jql)

    def get_epics(self):
        return self.__fetch_all_results(self.jira_epics_jql)

    def __fetch_all_results(self, jql):
        # 1) Fetch keys only
        keys = []
        start_at = 0

        while True:
            params = {
                "jql": jql,
                "fields": "key",
                "maxResults": self.results_per_page,
                "startAt": start_at
            }

            r = self.session.get(self.jira_endpoint,
                                 params=params,
                                 auth=(self.jira_username, self.jira_api_key))

            data = r.json()
            issues = data.get("issues", [])
            keys.extend([i["key"] for i in issues])

            # FIX: Jira Cloud no siempre envía isLast
            if len(issues) < self.results_per_page:
                break

            start_at += len(issues)

        logger.info(f"Found {len(keys)} issues")

        # 2) Fetch full issues
        issue_arr = []
        base = self.jira_endpoint.rsplit("/search", 1)[0]

        for key in keys:
            url = f"{base}/issue/{key}"
            params = {"expand": "names,changelog,renderedFields"}
            r = self.session.get(url, params=params,
                                 auth=(self.jira_username, self.jira_api_key))

            if r.status_code == 200:
                issue_arr.append(r.json())
            else:
                logger.warning(f"Failed {key}: {r.status_code}")

        return issue_arr


# =========================
# TRANSFORM STATIC CORE TABLE
# =========================
class TransformData:

    def construct_dataframe(self, issues_json):
        issue_list = [self.make_issue_body(issue) for issue in issues_json]
        return pd.json_normalize(issue_list)

    def make_issue_body(self, issue):
        f = issue.get("fields", {})
        body = defaultdict(None)

        body["key"] = issue.get("key")
        body["summary"] = f.get("summary")
        body["status"] = f.get("status", {}).get("name")
        body["project"] = f.get("project", {}).get("name")
        body["issuetype"] = f.get("issuetype", {}).get("name")
        body["priority"] = (f.get("priority") or {}).get("name")
        body["assignee"] = (f.get("assignee") or {}).get("displayName")

        body["created"] = f.get("created")
        body["updated"] = f.get("updated")

        return body


# =========================
# DYNAMIC EXTRACTION
# =========================
def extract_path(obj, path):
    """Safe deep path extraction"""
    for p in path.split("."):
        if not isinstance(obj, dict):
            return []
        obj = obj.get(p, [])
        if obj is None:
            return []
    return obj


def infer_columns(rows):
    cols = set()
    for r in rows:
        if isinstance(r, dict):
            cols.update(r.keys())
    return list(cols)


def drop_and_create_table(engine, table, columns):
    # Escapar columnas problemáticas
    cols_sql = ", ".join([f"\"{c}\" TEXT" for c in columns])
    sql = f'DROP TABLE IF EXISTS "{table}"; CREATE TABLE "{table}" ({cols_sql});'

    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

    logger.info(f"Created table {table}")


def insert_rows(engine, table, columns, rows):
    with engine.begin() as conn:
        for r in rows:
            clean = {}
            for c in columns:
                v = r.get(c)

                # FIX CRÍTICO
                if isinstance(v, (dict, list)):
                    v = json.dumps(v, ensure_ascii=False)

                clean[c] = v

            placeholders = ",".join([f":{c}" for c in columns])
            col_sql = ",".join([f"\"{c}\"" for c in columns])
            sql = f'INSERT INTO "{table}" ({col_sql}) VALUES ({placeholders})'

            conn.execute(text(sql), clean)


# =========================
# DATABASE
# =========================
class Database:

    def __init__(self, Config):
        self.engine = create_engine(Config.db_uri, pool_pre_ping=True)

    def upload_core(self, df, table):
        df.to_sql(table, self.engine, if_exists="replace", index=False)
        logger.info(f"Uploaded {len(df)} rows to {table}")

    def upload_dynamic(self, issues_json):
            for table, path in DYNAMIC_TABLES.items():
                logger.info(f"Extracting dynamic table {table} from {path}")
                all_rows = []
                for issue in issues_json:
                    rows = extract_path(issue, path)
                    if isinstance(rows, list):
                        for r in rows:
                            if isinstance(r, dict):
                                r["issue_key"] = issue["key"]
                                all_rows.append(r)

                if not all_rows:
                    logger.warning(f"No rows for {table}")
                    continue

                # Tabla dinámica original
                cols = infer_columns(all_rows)
                drop_and_create_table(self.engine, table, cols)
                insert_rows(self.engine, table, cols, all_rows)

                # ✨ Tabla plana para changelog
                if table == "jira_changelog":
                    flat_rows = []
                    for r in all_rows:
                        flat = {
                            "issue_key": r.get("issue_key"),
                            "author_name": r.get("author", {}).get("displayName") if isinstance(r.get("author"), dict) else None,
                            "author_account_id": r.get("author", {}).get("accountId") if isinstance(r.get("author"), dict) else None,
                            "created": r.get("created"),
                            "field": r.get("items", [{}])[0].get("field") if isinstance(r.get("items"), list) else None,
                            "from": r.get("items", [{}])[0].get("fromString") if isinstance(r.get("items"), list) else None,
                            "to": r.get("items", [{}])[0].get("toString") if isinstance(r.get("items"), list) else None,
                        }
                        flat_rows.append(flat)

                    flat_cols = list(flat_rows[0].keys()) if flat_rows else []
                    if flat_rows:
                        drop_and_create_table(self.engine, "jira_changelog_flat", flat_cols)
                        insert_rows(self.engine, "jira_changelog_flat", flat_cols, flat_rows)
                        logger.info(f"Uploaded flattened {len(flat_rows)} rows to jira_changelog_flat")


# =========================
# MAIN PIPELINE
# =========================
from config import Config


def init_script():
    logger.info("Fetching JIRA data")
    jira = FetchJiraIssues(Config)
    issues = jira.get_issues()
    epics = jira.get_epics()

    logger.info("Transforming core table")
    t = TransformData()
    issues_df = t.construct_dataframe(issues)

    db = Database(Config)
    db.upload_core(issues_df, "jira_issues_core")
    db.upload_dynamic(issues)

    logger.info("DONE")


if __name__ == "__main__":
    init_script()
