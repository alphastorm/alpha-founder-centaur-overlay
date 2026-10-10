"""Bounded Linear reads using Centaur's native credential mediation (no token input)."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import re
import urllib.error
import urllib.request
from urllib.parse import urlsplit

WORKFLOW_NAME = "carrythrough_linear_snapshot"
WORKFLOW_PRINCIPAL = "carrythrough-linear"
REQUEST_SCHEMA = "carrythrough.linear-snapshot-request.v1"
RESULT_SCHEMA = "carrythrough.linear-snapshot-result.v1"
ENDPOINT = "https://api.linear.app/graphql"
OMISSIONS = ["attachments", "comments", "relations"]
FIELDS = """id identifier title description url updatedAt
team { id key } project { id } state { id name type }
labels(first: 100) { nodes { name } pageInfo { hasNextPage endCursor } }"""
ISSUE_QUERY = "query CarrythroughIssue($id: String!) { organization { id urlKey } issue(id: $id) { " + FIELDS + " } }"
POLL_QUERY = "query CarrythroughIssues($filter: IssueFilter!, $after: String) { organization { id urlKey } issues(first: 50, after: $after, filter: $filter) { nodes { " + FIELDS + " } pageInfo { hasNextPage endCursor } } }"
LABEL_QUERY = "query CarrythroughLabels($id: String!, $after: String) { issue(id: $id) { id updatedAt labels(first: 100, after: $after) { nodes { name } pageInfo { hasNextPage endCursor } } } }"


class SourceError(Exception):
    def __init__(self, code, resource_id=None, retry_after_seconds=None):
        self.code = code
        self.resource_id = resource_id
        self.retry_after_seconds = retry_after_seconds
        super().__init__(code)

    def result(self):
        return {"code": self.code, "resource_id": self.resource_id,
                "retry_after_seconds": self.retry_after_seconds}


def string(value, maximum=256, pattern=None):
    if not isinstance(value, str) or not 1 <= len(value) <= maximum or (pattern and not re.fullmatch(pattern, value)):
        raise SourceError("invalid_result")
    return value


def ids(value, *, required=False):
    if not isinstance(value, list) or len(value) > 100 or (required and not value):
        raise ValueError("scope requires a bounded id list")
    if len(set(value)) != len(value):
        raise ValueError("scope ids must be unique")
    return [string(item, 64, r"[A-Za-z0-9_-]+") for item in value]


class Input(dict):
    @classmethod
    def parse(cls, raw):
        required = {"schema_version", "request_id", "workspace_id", "workspace_slug", "allowed_team_ids", "allowed_project_ids", "issue_identifier", "label_names", "state_types", "max_pages", "max_bytes"}
        if not isinstance(raw, dict) or set(raw) != required or raw["schema_version"] != REQUEST_SCHEMA:
            raise ValueError("unsupported Linear snapshot request")
        data = dict(raw)
        string(data["request_id"], 128)
        if len(data["request_id"]) < 8:
            raise ValueError("invalid request id")
        validate_scope_input(data)
        if data["issue_identifier"] is not None:
            string(data["issue_identifier"], 32, r"[A-Z][A-Z0-9]{0,15}-[1-9][0-9]{0,9}")
        for name, maximum in (("label_names", 50), ("state_types", 20)):
            if not isinstance(data[name], list) or len(data[name]) > maximum:
                raise ValueError("invalid filter")
            for value in data[name]:
                string(value)
        for name, low, high in (("max_pages", 1, 20), ("max_bytes", 1024, 1048576)):
            if type(data[name]) is not int or not low <= data[name] <= high:
                raise ValueError("invalid source limit")
        return cls(data)


def validate_scope_input(data):
    string(data["workspace_id"], 64, r"[A-Za-z0-9_-]+")
    string(data["workspace_slug"], 64, r"[a-z0-9][a-z0-9-]*")
    ids(data["allowed_team_ids"], required=True)
    ids(data["allowed_project_ids"])


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GraphQL:
    """Same centaur_sdk secret resolution as the stock Linear client, bounded HTTP."""
    def __init__(self, max_bytes):
        from centaur_sdk import secret
        self._token = secret("LINEAR_API_KEY", "")
        if self._token != "LINEAR_API_KEY":
            raise SourceError("access_denied")
        self.remaining = max_bytes
        self.opener = urllib.request.build_opener(NoRedirect())

    def query(self, query, variables):
        request = urllib.request.Request(ENDPOINT, method="POST",
            headers={"Authorization": self._token, "Content-Type": "application/json"},
            data=json.dumps({"query": query, "variables": variables}, separators=(",", ":")).encode())
        try:
            with self.opener.open(request, timeout=15) as response:
                raw = response.read(self.remaining + 1)
        except urllib.error.HTTPError as error:
            if error.code == 429:
                retry = error.headers.get("Retry-After", "")
                raise SourceError("rate_limited", retry_after_seconds=min(int(retry), 86400) if retry.isdigit() else None) from None
            raise SourceError("access_denied" if error.code in (401, 403) else "unavailable") from None
        except (OSError, urllib.error.URLError):
            raise SourceError("unavailable") from None
        self.remaining -= len(raw)
        if self.remaining < 0:
            raise SourceError("bounds_exceeded")
        try:
            payload = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            raise SourceError("invalid_result") from None
        if not isinstance(payload, dict):
            raise SourceError("invalid_result")
        if payload.get("errors"):
            # Never expose GraphQL error text, including provider credentials echoed in it.
            codes = {str(e.get("extensions", {}).get("code", "")) for e in payload["errors"] if isinstance(e, dict)}
            if "RATELIMITED" in codes:
                raise SourceError("rate_limited")
            raise SourceError("partial_result")
        if not isinstance(payload.get("data"), dict):
            raise SourceError("invalid_result")
        return payload["data"]


def connection(value):
    if not isinstance(value, dict) or not isinstance(value.get("nodes"), list):
        raise SourceError("invalid_result")
    info = value.get("pageInfo")
    if not isinstance(info, dict) or type(info.get("hasNextPage")) is not bool:
        raise SourceError("invalid_result")
    cursor = info.get("endCursor")
    if info["hasNextPage"] and (not value["nodes"] or not isinstance(cursor, str) or not cursor):
        raise SourceError("partial_result")
    return value["nodes"], cursor if info["hasNextPage"] else None


def check_workspace(data, inp):
    org = data.get("organization")
    if not isinstance(org, dict) or org.get("id") != inp["workspace_id"] or org.get("urlKey") != inp["workspace_slug"]:
        raise SourceError("out_of_scope")


def normalize_issue(raw, inp, query):
    if not isinstance(raw, dict):
        raise SourceError("not_found")
    issue_id = string(raw.get("id"), 64)
    team, project, state = raw.get("team"), raw.get("project"), raw.get("state")
    if not isinstance(team, dict) or not isinstance(state, dict) or (project is not None and not isinstance(project, dict)):
        raise SourceError("invalid_result", issue_id)
    project_id = string(project.get("id"), 64) if project else None
    if team.get("id") not in inp["allowed_team_ids"] or (inp["allowed_project_ids"] and project_id not in inp["allowed_project_ids"]):
        raise SourceError("out_of_scope", issue_id)
    identifier = string(raw.get("identifier"), 32, r"[A-Z][A-Z0-9]{0,15}-[1-9][0-9]{0,9}")
    url = string(raw.get("url"), 512)
    parts = urlsplit(url)
    path = parts.path.split("/")
    if parts.scheme != "https" or parts.netloc != "linear.app" or parts.query or parts.fragment or len(path) != 5 or path[1:4] != [inp["workspace_slug"], "issue", identifier] or not path[4] or any(c.isspace() for c in url):
        raise SourceError("out_of_scope", issue_id)
    updated = string(raw.get("updatedAt"), 40)
    try:
        if dt.datetime.fromisoformat(updated.replace("Z", "+00:00")).tzinfo is None:
            raise ValueError()
    except ValueError:
        raise SourceError("invalid_result", issue_id) from None
    label_nodes, cursor = connection(raw.get("labels"))
    seen = set()
    while cursor is not None:
        if cursor in seen:
            raise SourceError("partial_result", issue_id)
        seen.add(cursor)
        page = query(LABEL_QUERY, {"id": issue_id, "after": cursor}).get("issue")
        if not isinstance(page, dict) or page.get("id") != issue_id or page.get("updatedAt") != updated:
            raise SourceError("source_changed", issue_id)
        nodes, cursor = connection(page.get("labels"))
        label_nodes.extend(nodes)
    labels = sorted({string(label.get("name")) for label in label_nodes if isinstance(label, dict)})
    if len(labels) > 100 or len(label_nodes) != len([x for x in label_nodes if isinstance(x, dict)]):
        raise SourceError("bounds_exceeded", issue_id)
    description = raw.get("description")
    if description is None:
        description = ""
    if not isinstance(description, str) or len(description) > 100000:
        raise SourceError("bounds_exceeded", issue_id)
    result = {"id": issue_id, "identifier": identifier, "title": string(raw.get("title"), 8000),
        "description": description, "url": url, "updated_at": updated,
        "team_id": string(team.get("id"), 64), "team_key": string(team.get("key"), 16),
        "project_id": project_id, "state_id": string(state.get("id"), 64),
        "state_name": string(state.get("name")), "state_type": string(state.get("type")), "labels": labels}
    if (inp["label_names"] and not set(inp["label_names"]).issubset(labels)) or (inp["state_types"] and result["state_type"] not in inp["state_types"]):
        raise SourceError("out_of_scope", issue_id)
    return result


def snapshot(inp, client):
    result = {"schema_version": RESULT_SCHEMA, "request_id": inp["request_id"],
              "workspace_id": inp["workspace_id"], "complete": False, "issues": [],
              "omissions": OMISSIONS, "error": None}
    pages = 0
    def query(document, variables):
        nonlocal pages
        pages += 1
        if pages > inp["max_pages"]:
            raise SourceError("bounds_exceeded")
        return client.query(document, variables)
    try:
        if inp["issue_identifier"]:
            data = query(ISSUE_QUERY, {"id": inp["issue_identifier"]})
            check_workspace(data, inp)
            issue = normalize_issue(data.get("issue"), inp, query)
            if issue["identifier"] != inp["issue_identifier"]:
                raise SourceError("out_of_scope")
            result["issues"] = [issue]
        else:
            filters = {"team": {"id": {"in": inp["allowed_team_ids"]}}}
            if inp["allowed_project_ids"]:
                filters["project"] = {"id": {"in": inp["allowed_project_ids"]}}
            if inp["state_types"]:
                filters["state"] = {"type": {"in": inp["state_types"]}}
            if inp["label_names"]:
                filters["and"] = [{"labels": {"some": {"name": {"eq": name}}}} for name in inp["label_names"]]
            cursor, seen = None, set()
            while True:
                data = query(POLL_QUERY, {"filter": filters, "after": cursor})
                check_workspace(data, inp)
                nodes, cursor = connection(data.get("issues"))
                result["issues"].extend(normalize_issue(raw, inp, query) for raw in nodes)
                if len(result["issues"]) > 100:
                    raise SourceError("bounds_exceeded")
                if cursor is None:
                    break
                if cursor in seen:
                    raise SourceError("partial_result")
                seen.add(cursor)
        if len({issue["id"] for issue in result["issues"]}) != len(result["issues"]):
            raise SourceError("source_changed")
        result["complete"] = True
        if len(json.dumps(result, ensure_ascii=False).encode()) > inp["max_bytes"]:
            raise SourceError("bounds_exceeded")
    except SourceError as error:
        result.update(complete=False, issues=[], error=error.result())
    return result


async def handler(inp, ctx):
    inp = Input.parse(inp)
    async def acquire():
        try:
            client = GraphQL(inp["max_bytes"])
        except SourceError as error:
            return {"schema_version": RESULT_SCHEMA, "request_id": inp["request_id"],
                    "workspace_id": inp["workspace_id"], "complete": False, "issues": [],
                    "omissions": OMISSIONS, "error": error.result()}
        return await asyncio.to_thread(snapshot, inp, client)
    return await ctx.step("snapshot", acquire)
