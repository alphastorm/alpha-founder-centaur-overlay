from __future__ import annotations

import asyncio
import json

import pytest

from test_omp_release_feed import FakeCtx, feed
import carrythrough_progress_feed as poller
import carrythrough_progress_feed_event as delivery


@pytest.fixture
def configured(monkeypatch):
    config = {"channels": {feed.MONOREPO: "C0123456789"}}
    monkeypatch.setattr(poller, "load_config", lambda: config)
    monkeypatch.setattr(delivery, "load_config", lambda: config)


def test_idle_does_not_start_or_post(configured, monkeypatch):
    assert poller.SCHEDULE["enabled"] is False  # L1 enables the configured schedule.
    monkeypatch.setattr(poller, "read_progress", lambda: {"conditions": []})
    ctx = FakeCtx()
    assert asyncio.run(poller.handler(poller.Input(), ctx)) == {"state": "polled", "events": 0}
    assert ctx.children == [] and ctx.posts == []


def test_repeated_condition_deduplicates_but_changed_condition_posts(configured, monkeypatch):
    condition = {"key": "uncertain:execution", "message": "1: Uncertain execution effects."}
    monkeypatch.setattr(poller, "read_progress", lambda: {"conditions": [condition]})
    ctx = FakeCtx()
    asyncio.run(poller.handler(poller.Input(), ctx))
    ctx.checkpoints.clear()  # a new scheduled poll, same native idempotency store
    asyncio.run(poller.handler(poller.Input(), ctx))
    assert len(ctx.new_child_keys) == 1
    condition = {**condition, "message": "2: Uncertain execution effects."}
    ctx.checkpoints.clear()
    asyncio.run(poller.handler(poller.Input(), ctx))
    assert len(ctx.new_child_keys) == 2


def test_native_slack_child_checkpoints_and_escapes(configured):
    ctx = FakeCtx()
    inp = delivery.Input(condition={"key": "loop:reconciler:stalled", "message": "Inspect <@U123> & retry"})
    asyncio.run(delivery.handler(inp, ctx))
    asyncio.run(delivery.handler(inp, ctx))
    assert len(ctx.posts) == 1
    assert "&lt;@U123&gt; &amp;" in str(ctx.posts)
    assert "client_msg_id" in str(ctx.posts)


def test_reader_uses_only_get_placeholder_and_keeps_only_conditions(monkeypatch):
    requests = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def read(self, bound):
            assert bound == poller.MAX_BYTES + 1
            return json.dumps({
                "schema_version": "carrythrough.operational-progress.v1", "conditions": [],
                "unneeded": "discarded",
            }).encode()

    class Opener:
        def open(self, request, timeout):
            requests.append(request)
            assert timeout == 10
            return Response()

    monkeypatch.setattr(poller, "build_opener", lambda _: Opener())
    assert poller.read_progress() == {"conditions": []}
    assert requests[0].method == "GET"
    assert requests[0].full_url == poller.PROGRESS_URL
    assert requests[0].headers["Authorization"] == "Bearer CARRYTHROUGH_PROGRESS_TOKEN"


def test_reader_failure_never_discloses_exception_or_credential(monkeypatch):
    class Opener:
        def open(self, *_args, **_kwargs):
            raise RuntimeError("credential=synthetic-never-print")

    monkeypatch.setattr(poller, "build_opener", lambda _: Opener())
    with pytest.raises(RuntimeError, match="^operational progress read failed$"):
        poller.read_progress()
