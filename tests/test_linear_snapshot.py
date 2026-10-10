from __future__ import annotations

import copy
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import urllib.error

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "workflows"))
import linear_snapshot as linear


def request(**changes):
    value = {"schema_version": linear.REQUEST_SCHEMA, "request_id": "synthetic-read-1",
             "workspace_id": "internal", "workspace_slug": "carrythrough",
             "allowed_team_ids": ["team-1"], "allowed_project_ids": ["project-1"],
             "issue_identifier": "CT-1", "label_names": [], "state_types": [],
             "max_pages": 5, "max_bytes": 262144}
    value.update(changes)
    return linear.Input.parse(value)


def issue(**changes):
    value = json.loads((ROOT / "tests/fixtures/linear_issue.json").read_text())
    value.update(changes)
    return value


def page(nodes, cursor=None):
    return {"nodes": nodes, "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor}}


class Client:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def query(self, query, variables):
        self.calls.append((query, variables))
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return copy.deepcopy(value)


def data(**changes):
    return {"organization": {"id": "internal", "urlKey": "carrythrough"}, **changes}


def test_selected_snapshot_normalization_omissions_and_scope():
    client = Client([data(issue=issue())])
    result = linear.snapshot(request(), client)
    assert result["complete"] is True
    assert result["issues"][0]["updated_at"] == "2026-10-10T01:00:00.000Z"
    assert result["issues"][0]["labels"] == ["internal"]
    assert result["omissions"] == ["attachments", "comments", "relations"]
    assert client.calls[0][1] == {"id": "CT-1"}


def test_poll_and_label_pagination_are_bounded_and_complete():
    first = issue(labels=page([{"name": "z"}], "label-next"))
    second = issue(id="issue-2", identifier="CT-2", url="https://linear.app/carrythrough/issue/CT-2/example")
    client = Client([data(issues=page([first], "issue-next")),
                     {"issue": {"id": "issue-1", "updatedAt": first["updatedAt"], "labels": page([{"name": "a"}])}},
                     data(issues=page([second]))])
    result = linear.snapshot(request(issue_identifier=None), client)
    assert result["complete"] is True
    assert result["issues"][0]["labels"] == ["a", "z"]
    assert client.calls[-1][1]["after"] == "issue-next"
    assert client.calls[0][1]["filter"]["team"] == {"id": {"in": ["team-1"]}}


@pytest.mark.parametrize("responses,changes,code", [
    ([data(issue=issue(team={"id": "foreign", "key": "NO"}))], {}, "out_of_scope"),
    ([data(issue=issue(project=None))], {}, "out_of_scope"),
    ([{"organization": {"id": "foreign", "urlKey": "carrythrough"}, "issue": issue()}], {}, "out_of_scope"),
    ([data(issue=None)], {}, "not_found"),
    ([linear.SourceError("partial_result")], {}, "partial_result"),
    ([linear.SourceError("access_denied")], {}, "access_denied"),
    ([data(issues=page([issue()], "next"))], {"issue_identifier": None, "max_pages": 1}, "bounds_exceeded"),
    ([data(issues={"nodes": [], "pageInfo": {"hasNextPage": True, "endCursor": None}})], {"issue_identifier": None}, "partial_result"),
])
def test_refused_snapshots_never_publish_partial_issues(responses, changes, code):
    result = linear.snapshot(request(**changes), Client(responses))
    assert result["complete"] is False
    assert result["issues"] == []
    assert result["error"]["code"] == code


def test_revision_change_during_label_pagination_refused():
    client = Client([data(issue=issue(labels=page([{"name": "a"}], "next"))),
                     {"issue": {"id": "issue-1", "updatedAt": "2026-10-11T01:00:00.000Z", "labels": page([])}}])
    assert linear.snapshot(request(), client)["error"]["code"] == "source_changed"


class Response(io.BytesIO):
    def __enter__(self):
        return self
    def __exit__(self, *_):
        self.close()


class Opener:
    def __init__(self, payload):
        self.payload = payload
    def open(self, request, timeout):
        assert request.full_url == "https://api.linear.app/graphql"
        assert timeout == 15
        if isinstance(self.payload, Exception):
            raise self.payload
        return Response(self.payload)


def graph(payload, max_bytes=1024):
    client = object.__new__(linear.GraphQL)
    client._token, client.remaining, client.opener = "placeholder", max_bytes, Opener(payload)
    return client


def test_partial_graphql_errors_and_response_bytes_refused():
    payload = json.dumps({"data": {"issue": issue()}, "errors": [{"message": "not logged"}]}).encode()
    with pytest.raises(linear.SourceError, match="partial_result"):
        graph(payload).query("query", {})
    with pytest.raises(linear.SourceError, match="bounds_exceeded"):
        graph(b" " * 1025).query("query", {})


def test_rate_limit_honors_retry_after_without_retry():
    error = urllib.error.HTTPError(linear.ENDPOINT, 429, "rate limited", {"Retry-After": "90"}, None)
    with pytest.raises(linear.SourceError) as raised:
        graph(error).query("query", {})
    assert raised.value.code == "rate_limited"
    assert raised.value.retry_after_seconds == 90


def test_native_custody_accepts_only_proxy_placeholder(monkeypatch):
    requested = []
    def secret(name, default):
        requested.append(name)
        return name
    monkeypatch.setitem(sys.modules, "centaur_sdk", SimpleNamespace(secret=secret))
    linear.GraphQL(1024)
    assert requested == ["LINEAR_API_KEY"]
    monkeypatch.setitem(sys.modules, "centaur_sdk", SimpleNamespace(secret=lambda *_: "copied-token"))
    with pytest.raises(linear.SourceError, match="access_denied"):
        linear.GraphQL(1024)
