import math
import requests
from loguru import logger


class FetchJiraIssues:
    """Fetch issues from JIRA instance matching JQL query."""

    def __init__(self, config):
        self.jira_username = config.jira_username
        self.jira_api_key = config.jira_api_key
        self.jira_endpoint = config.jira_endpoint
        self.jira_issues_jql = config.jira_issues_jql
        self.jira_issues_fields = config.jira_issues_fields
        self.jira_epics_jql = config.jira_epics_jql
        self.jira_epics_fields = config.jira_epics_fields
        self.results_per_page = 100

    def get_issues(self):
        """Fetch JIRA issues which are not Epics."""
        logger.info('Fetching issues from JIRA...')
        issues = self.__fetch_all_results(
            self.jira_issues_jql,
            self.jira_issues_fields
        )
        return issues

    def get_epics(self):
        """Fetch JIRA issues which are Epics."""
        logger.info('Fetching epics from JIRA...')
        issues = self.__fetch_all_results(
            self.jira_epics_jql,
            self.jira_epics_fields
        )
        return issues

    def __get_total_number_of_issues(self, jql):
        """Gets the total number of results to retrieve."""
        params = {
            "jql": jql,
            "maxResults": 1000,
            "startAt": 0}
        req = requests.get(
            self.jira_endpoint,
            headers={"Accept": "application/json"},
            params=params,
            auth=(self.jira_username, self.jira_api_key)
        )
        logger.debug(f"reque: {req.json()}")
        total_results = req.json().get('total', None)
        if total_results:
            return total_results
        logger.info('Could not find any issues!')

    def __fetch_all_results(self, jql, fields):
        """
        Obtener todos los issues de un JQL y traer su información completa
        usando la llamada específica /issue/{key}?expand=names,changelog,renderedFields
        """
        # 1️⃣ Traer solo los keys de los issues con la búsqueda JQL
        issues_keys = []
        start_at = 0

        while True:
            params = {
                "jql": jql,
                "fields": "key",  # solo necesitamos la clave
                "maxResults": self.results_per_page,
                "startAt": start_at,
                "validateQuery": "warn"
            }

            req = requests.get(
                self.jira_endpoint,
                headers={"Accept": "application/json"},
                params=params,
                auth=(self.jira_username, self.jira_api_key)
            )

            if req.status_code != 200:
                raise Exception(f"JIRA API request failed: {req.text}")

            data = req.json()
            issues_page = data.get("issues", [])
            issues_keys.extend([i["key"] for i in issues_page])

            if data.get("isLast", True) or len(issues_page) == 0:
                break

            start_at += len(issues_page)

        logger.info(f"Found {len(issues_keys)} issues. Fetching full details now...")

        # 2️⃣ Por cada key, llamar al endpoint específico y guardar resultado completo
        issue_arr = []
        for key in issues_keys:
            url = f"{self.jira_endpoint.rsplit('/search',1)[0]}/issue/{key}"
            params = {"expand": "names,changelog,renderedFields"}
            req = requests.get(url,
                            headers={"Accept": "application/json"},
                            params=params,
                            auth=(self.jira_username, self.jira_api_key))
            if req.status_code == 200:
                issue_arr.append(req.json())
            else:
                logger.warning(f"Failed to fetch {key}: {req.status_code}")
            logger.info(f"Item: {req.json()}")

        logger.info(f"Total issues fetched with full details: {len(issue_arr)}")
        return issue_arr
