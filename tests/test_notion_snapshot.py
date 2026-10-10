from __future__ import annotations

import asyncio
import copy
import importlib.util
import io
import json
import os
import sys
import urllib.error
from email.message import Message
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "workflows" / "notion_snapshot.py"
SPEC = importlib.util.spec_from_file_location("overlay_notion_snapshot", SOURCE)
assert SPEC is not None and SPEC.loader is not None
notion = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = notion
SPEC.loader.exec_module(notion)
SCHEMAS = ROOT / "tests" / "schemas"
REQUEST = Draft202012Validator(json.loads((SCHEMAS / "notion_snapshot_request.json").read_text()))
RESULT = Draft202012Validator(json.loads((SCHEMAS / "notion_snapshot_result.json").read_text()))


def fixture(name="page_nested") -> dict:
    return json.loads((ROOT / "tests" / "fixtures" / "notion" / f"{name}.json").read_text())


def request(data: dict, **limits) -> dict:
    value = {
        key: data[key] for key in ("request_id", "workspace_id", "resource_id", "resource_kind")
    }
    value["schema_version"] = notion.REQUEST_SCHEMA
    value.update(limits)
    REQUEST.validate(value)
    return value


class Client:
    def __init__(self, data: dict):
        self.data = copy.deepcopy(data)
        self.calls = []
        self.failure: Exception | dict[str, object] | None = None
        self.changed = False
        self.root_reads = 0
        self.workspace = data["workspace_id"]

    def read(self, path: str, body=None) -> dict:
        self.calls.append((path, body))
        if path == "/v1/users/me":
            return {"object": "user", "type": "bot", "bot": {"workspace_id": self.workspace}}
        resource_id = self.data["resource_id"]
        if path in (f"/v1/pages/{resource_id}", f"/v1/databases/{resource_id}"):
            self.root_reads += 1
            root = copy.deepcopy(self.data["resource"])
            if self.changed and self.root_reads > 1:
                root["last_edited_time"] = "2026-07-15T08:42:00.000Z"
            return root
        if self.failure is not None:
            if isinstance(self.failure, Exception):
                raise self.failure
            return self.failure
        database = self.data["resource_kind"] == "database"
        pages = self.data["database_pages" if database else "block_pages"]
        for page in pages:
            query = {"page_size": 100}
            if page["start_cursor"] is not None:
                query["start_cursor"] = page["start_cursor"]
            expected = (
                f"/v1/data_sources/{page['parent_id']}/query"
                if database
                else (
                    f"/v1/blocks/{page['parent_id']}/children?{notion.urllib.parse.urlencode(query)}"
                )
            )
            if path == expected and body == (query if database else None):
                return {
                    "object": "list",
                    **{key: page[key] for key in ("has_more", "next_cursor", "results")},
                }
        raise AssertionError(f"unexpected read path {path}")


class Context:
    def __init__(self):
        self.steps = []

    async def step(self, name, operation):
        self.steps.append(name)
        return await operation()


def run(data: dict, monkeypatch, client=None, **limits) -> tuple[dict, Client]:
    client = client or Client(data)
    monkeypatch.setattr(notion, "_client", lambda _max_bytes: client)
    context = Context()
    result = asyncio.run(notion._handler(request(data, **limits), context))
    RESULT.validate(result)
    assert context.steps == ["notion-snapshot-read"]
    return result, client


@pytest.mark.parametrize("name", ["page_nested", "database_paginated"])
def test_workflow_returns_exact_api_fixture_and_only_scoped_read_paths(name, monkeypatch):
    data = fixture(name)
    result, client = run(data, monkeypatch)
    assert result == data
    assert client.calls[0][0] == "/v1/users/me"
    assert not any(
        "search" in path or "comments" in path or "files.invalid" in path
        for path, _ in client.calls
    )
    if name == "database_paginated":
        queries = [(path, body) for path, body in client.calls if path.endswith("/query")]
        assert len(queries) == 2 and queries[1][1]["start_cursor"] == "cursor-two"


@pytest.mark.parametrize(
    "failure", ["access_denied", "not_found", "unavailable", "partial_result", "rate_limited"]
)
def test_partial_or_unavailable_source_discards_all_captured_content(failure, monkeypatch):
    data = fixture()
    client = Client(data)
    client.failure = notion.ReadError(failure, 60 if failure == "rate_limited" else None)
    result, _ = run(data, monkeypatch, client)
    assert result["state"] == "unavailable" and result["resource"] is None
    assert result["block_pages"] == [] and result["database_pages"] == []
    assert result["error"]["code"] == failure
    assert "snapshot_artifact_ref" not in result
    assert "NOTION_API_KEY" not in json.dumps(result)


def test_partial_provider_error_is_rejected_instead_of_returning_earlier_content(monkeypatch):
    data = fixture()
    client = Client(data)
    client.failure = {
        "object": "list",
        "results": [],
        "has_more": False,
        "next_cursor": None,
        "errors": [{"message": "private diagnostic"}],
    }
    result, _ = run(data, monkeypatch, client)
    assert result["state"] == "unavailable" and result["error"]["code"] == "partial_result"
    assert "private diagnostic" not in json.dumps(result)


def test_wrong_workspace_stops_before_any_source_read(monkeypatch):
    data = fixture()
    client = Client(data)
    client.workspace = "b8ddf423-595c-8092-935f-e1b062932d2a"
    result, _ = run(data, monkeypatch, client)
    assert result["error"]["code"] == "out_of_scope"
    assert client.calls == [("/v1/users/me", None)]


@pytest.mark.parametrize("limits", [{"max_depth": 0}, {"max_pages": 1}, {"max_blocks": 2}])
def test_read_bounds_disclose_truncation_instead_of_false_completeness(limits, monkeypatch):
    result, _ = run(fixture(), monkeypatch, **limits)
    assert result["state"] == "truncated" and result["omissions"]
    assert any("children_truncated:" in omission for omission in result["omissions"])


def test_database_pagination_bound_remains_explicit(monkeypatch):
    result, _ = run(fixture("database_paginated"), monkeypatch, max_pages=1)
    assert result["state"] == "truncated" and len(result["database_pages"]) == 1
    assert result["database_pages"][0]["has_more"] is True


def test_changed_source_is_unavailable_and_oversized_return_discards_payload(monkeypatch):
    data = fixture()
    client = Client(data)
    client.changed = True
    result, _ = run(data, monkeypatch, client)
    assert result["error"]["code"] == "source_changed"
    result, _ = run(data, monkeypatch, max_bytes=1024)
    assert result["state"] == "unavailable" and result["error"]["code"] == "bounds_exceeded"
    assert result["resource"] is None


def test_input_rejects_credentials_and_unsupported_schema_or_bounds():
    valid = request(fixture())
    for changes in (
        {"api_key": "never accepted"},
        {"max_pages": 51},
        {"max_bytes": True},
        {"schema_version": "alpha-founder.notion-snapshot-request.v1"},
        {"resource_id": "https://example.com/page"},
    ):
        with pytest.raises(ValueError):
            notion.Input.parse(valid | changes)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class Opener:
    def __init__(self, payload=b'{"object":"user"}', failure=None):
        self.payload = payload
        self.failure = failure
        self.calls = []

    def open(self, req, timeout):
        self.calls.append((req, timeout))
        if self.failure is not None:
            raise self.failure
        return Response(self.payload)


def test_client_uses_only_custodied_proxy_placeholder_and_bounds_response_bytes():
    client = notion.NotionClient(1024)
    client.opener = opener = Opener()
    assert client.read("/v1/users/me") == {"object": "user"}
    req, timeout = opener.calls[0]
    assert req.full_url == "https://api.notion.com/v1/users/me" and req.get_method() == "GET"
    assert req.get_header("Authorization") == "Bearer NOTION_API_KEY"
    assert req.get_header("Notion-version") == "2025-09-03" and timeout == 30
    client.opener = Opener(b"x" * 1025)
    with pytest.raises(notion.ReadError, match="bounds_exceeded"):
        client.read("/v1/users/me")


@pytest.mark.parametrize(
    "status,code",
    [
        (401, "access_denied"),
        (403, "access_denied"),
        (404, "not_found"),
        (429, "rate_limited"),
        (503, "unavailable"),
        (302, "unavailable"),
    ],
)
def test_client_redacts_errors_and_honors_rate_limit_without_retry(status, code):
    client = notion.NotionClient(1024)
    headers = Message()
    headers["Retry-After"] = "60"
    error = urllib.error.HTTPError(
        "https://api.notion.com/v1/users/me",
        status,
        "secret-like diagnostic",
        headers,
        None,
    )
    client.opener = opener = Opener(failure=error)
    with pytest.raises(notion.ReadError) as caught:
        client.read("/v1/users/me")
    assert caught.value.code == code and "secret-like" not in str(caught.value)
    assert caught.value.retry_after_seconds == 60 and len(opener.calls) == 1


def test_native_discovery_is_inert_by_default_and_explicit_opt_in_is_required(monkeypatch):
    centaur = Path(os.environ.get("CENTAUR_144_SOURCE", "~/.cache/centaur-144-src")).expanduser()
    host_source = centaur / "services" / "workflow-python" / "workflow_host.py"
    host_spec = importlib.util.spec_from_file_location("native_workflow_host", host_source)
    assert host_spec is not None and host_spec.loader is not None
    workflow_host = importlib.util.module_from_spec(host_spec)
    sys.modules[host_spec.name] = workflow_host
    host_spec.loader.exec_module(workflow_host)

    monkeypatch.delenv("CARRYTHROUGH_NOTION_SNAPSHOT_ENABLED", raising=False)
    assert workflow_host.workflow_name_from_source(SOURCE) == "carrythrough_notion_snapshot"
    assert workflow_host.load_workflow_file(SOURCE) is None
    monkeypatch.setenv("CARRYTHROUGH_NOTION_SNAPSHOT_ENABLED", "1")
    registered = workflow_host.load_workflow_file(SOURCE)
    assert registered is not None and registered.workflow_name == "carrythrough_notion_snapshot"
    assert registered.principal == "carrythrough-notion"
