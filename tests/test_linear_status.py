from __future__ import annotations

import asyncio
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "workflows"))
import linear_status as status
from linear_snapshot import SourceError


WORK = "01ARZ3NDEKTSV4RRFFQ69G5FAV"


def request(**changes):
    value = {"schema_version": status.REQUEST_SCHEMA, "request_id": f"linear-status:{WORK}:draft_pr_opened",
             "workspace_id": "internal", "workspace_slug": "carrythrough", "allowed_team_ids": ["team-1"],
             "allowed_project_ids": [], "issue_id": "issue-1", "work_item_id": WORK,
             "event": "draft_pr_opened", "state": "draft_pr_opened",
             "pr_url": "https://github.com/carrythroughsystems/carrythrough/pull/1",
             "slack_channel": "C123456789", "slack_thread_ts": None, "target_state_id": None}
    value.update(changes)
    return value


class Client:
    def __init__(self, error=None, foreign=False):
        self.error, self.foreign, self.writes = error, foreign, []

    def query(self, query, variables):
        if query == status.SCOPE_QUERY:
            return {"organization": {"id": "other" if self.foreign else "internal", "urlKey": "carrythrough"},
                    "issue": {"id": "issue-1", "team": {"id": "team-1"}, "project": None}}
        self.writes.append((query, variables))
        if self.error:
            raise self.error
        if query == status.STATE:
            return {"issueUpdate": {"success": True, "issue": {"id": "issue-1", "state": {"id": "done"}}}}
        return {"commentCreate": {"success": True, "comment": {"id": "comment-1"}}}


class Context:
    def __init__(self, fail_slack=False):
        self.steps, self.posts, self.fail_slack = {}, [], fail_slack
    async def step(self, key, fn):
        if key not in self.steps:
            self.steps[key] = await fn()
        return self.steps[key]
    async def post_to_slack(self, channel, text, **kwargs):
        self.posts.append((channel, text, kwargs))
        if self.fail_slack:
            raise TimeoutError()
        return {"ok": True, "channel": channel, "ts": "1791590000.123456"}


def test_comment_and_native_slack_are_checkpointed_once(monkeypatch):
    client, ctx = Client(), Context()
    monkeypatch.setattr(status, "GraphQL", lambda _: client)
    result = asyncio.run(status.handler(request(), ctx))
    replay = asyncio.run(status.handler(request(), ctx))
    assert result == replay
    assert result["linear_state"] == result["slack_state"] == "delivered"
    assert len(client.writes) == len(ctx.posts) == 1
    assert client.writes[0][0] == status.COMMENT
    assert "draft_pr_opened" in ctx.posts[0][1]
    assert "merged" not in ctx.posts[0][1]
    assert ctx.posts[0][2]["client_msg_id"]


def test_uncertain_writes_remain_pending_and_never_repeat(monkeypatch):
    client, ctx = Client(error=TimeoutError()), Context(fail_slack=True)
    monkeypatch.setattr(status, "GraphQL", lambda _: client)
    result = asyncio.run(status.handler(request(), ctx))
    assert result["linear_state"] == result["slack_state"] == "pending"
    assert asyncio.run(status.handler(request(), ctx)) == result
    assert len(client.writes) == len(ctx.posts) == 1


def test_out_of_scope_prevents_both_writes(monkeypatch):
    client, ctx = Client(foreign=True), Context()
    monkeypatch.setattr(status, "GraphQL", lambda _: client)
    result = asyncio.run(status.handler(request(), ctx))
    assert result["error"]["code"] == "out_of_scope"
    assert client.writes == ctx.posts == []


def test_revoked_grant_refuses_without_message(monkeypatch):
    def revoked(_):
        raise SourceError("access_denied")
    monkeypatch.setattr(status, "GraphQL", revoked)
    ctx = Context()
    assert asyncio.run(status.handler(request(), ctx))["error"]["code"] == "access_denied"
    assert ctx.posts == []


def test_draft_event_cannot_close_linear_state():
    with pytest.raises(ValueError, match="cannot advance"):
        status.Input.parse(request(target_state_id="done"))


def test_observed_merge_uses_only_configured_state(monkeypatch):
    client, ctx = Client(), Context()
    monkeypatch.setattr(status, "GraphQL", lambda _: client)
    result = asyncio.run(status.handler(request(event="merged", state="merged",
        request_id=f"linear-status:{WORK}:merged", target_state_id="done", slack_channel=None), ctx))
    assert result["linear_state"] == "delivered"
    assert result["slack_state"] == "not_requested"
    assert client.writes == [(status.STATE, {"id": "issue-1", "input": {"stateId": "done"}})]
