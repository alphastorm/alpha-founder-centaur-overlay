from __future__ import annotations

import asyncio
from collections import Counter
import copy
import datetime as dt
import inspect
import json
import os
from pathlib import Path
import re
import sys
import tomllib
import uuid
from urllib.parse import parse_qs, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[1]
CENTAUR = Path(os.environ.get("CENTAUR_144_SOURCE", "~/.cache/centaur-144-src")).expanduser()
API = Path(os.environ.get("CENTAUR_WORKFLOW_SOURCE", str(CENTAUR / "services/workflow-python"))).expanduser()
for directory in (API, ROOT / "retained-host/workflows"):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))

import _omp_feed as feed
import omp_release_feed as poller
import omp_release_feed_event as delivery

NOW = dt.datetime(2026, 10, 7, 12, 5, tzinfo=dt.timezone.utc)
RECENT = NOW - dt.timedelta(seconds=300)
OLD = RECENT - dt.timedelta(days=1)


def user(login, kind="User"):
    return {"login": login, "type": kind}


def issue(number=7, **changes):
    return {
        "id": 100 + number, "number": number, "title": "Upstream tracking: v18.8.3",
        "user": user(feed.ACTIONS_BOT, "Bot"), "created_at": feed.iso(RECENT + dt.timedelta(seconds=1)),
        "updated_at": feed.iso(NOW), "state": "open", "body": "PRIVATE ISSUE BODY", **changes,
    }


def pr(repo, number=1, **changes):
    return {
        "id": 200 + number, "number": number, "title": "prepare & <@U012345678> > release",
        "user": user("alpha-founder-source-alphastorm[bot]", "Bot"),
        "head": {"repo": {"full_name": repo}, "ref": "alpha-founder/order"},
        "created_at": feed.iso(RECENT + dt.timedelta(seconds=5)), "updated_at": feed.iso(NOW),
        "state": "open", "merged_at": None, "body": "PRIVATE PR BODY", **changes,
    }


def comment(**changes):
    return {
        "id": 301, "user": user("alphastorm-release"), "created_at": feed.iso(NOW),
        "issue_url": "https://api.github.com/repos/" + feed.GATEWAY + "/issues/7",
        "body": "release-driver: preparing — <@U012345678> & build > ready\nPRIVATE COMMENT LINK/BODY",
        **changes,
    }


def release(**changes):
    return {
        "id": 401, "author": user("alphastorm-release"), "tag_name": "v0.7.5",
        "published_at": feed.iso(NOW), "draft": False, "body": "PRIVATE RELEASE BODY", **changes,
    }


def run(repo, name, identifier, **changes):
    return {
        "id": identifier, "name": name, "actor": user("alphastorm"), "status": "completed",
        "conclusion": "failure", "head_branch": "main", "head_repository": {"full_name": repo},
        "created_at": feed.iso(OLD), "updated_at": feed.iso(NOW), **changes,
    }


class FakeGitHub(feed.GitHub):
    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []

    def get(self, path, **query):
        self.calls.append((path, query))
        key = (path, query.get("page", 1))
        if key in self.responses:
            return copy.deepcopy(self.responses[key])
        if path.endswith("/actions/runs"):
            return {"workflow_runs": []}
        if re.search(r"/issues/[0-9]+$", path):
            return copy.deepcopy(self.responses[path])
        return []


class FakeCtx:
    def __init__(self):
        self.checkpoints = {}
        self.children = []
        self.starts = {}
        self.new_child_keys = []
        self.posts = []

    async def step(self, name, fn, **kwargs):
        if name not in self.checkpoints:
            result = fn()
            self.checkpoints[name] = await result if inspect.isawaitable(result) else result
        return copy.deepcopy(self.checkpoints[name])

    async def start_workflow(self, name, input=None, *, idempotency_key=None):
        assert idempotency_key
        self.children.append((name, input, idempotency_key))
        created = idempotency_key not in self.starts
        self.starts.setdefault(idempotency_key, copy.deepcopy(input))
        if created:
            self.new_child_keys.append(idempotency_key)
        return {"created": created, "run_id": "fake-run", "task_id": "fake-task"}

    async def post_to_slack(self, channel, text, **kwargs):
        self.posts.append((channel, text, kwargs))
        return {"ok": True}


@pytest.fixture
def config(monkeypatch):
    value = {
        "channels": {feed.MONOREPO: "C0123456789", feed.GATEWAY: "G0123456789"},
        "app_bot_login": "alpha-founder-source-alphastorm[bot]",
        "release_bot_login": "alphastorm-release", "founder_login": "alphastorm",
    }
    monkeypatch.setattr(poller, "load_config", lambda: copy.deepcopy(value))
    monkeypatch.setattr(delivery, "load_config", lambda: copy.deepcopy(value))
    return value


def responses():
    result = {}
    for repo in feed.REPOSITORIES:
        base = "/repos/" + repo
        result[(base + "/issues", 1)] = [issue(), issue(8, created_at=feed.iso(OLD), state="closed", closed_at=feed.iso(NOW))]
        result[base + "/issues/7"] = result[(base + "/issues", 1)][0]
        result[(base + "/pulls", 1)] = [
            pr(repo),
            pr(repo, 2, user=user("alphastorm-release"), created_at=feed.iso(OLD), state="closed", merged_at=feed.iso(NOW)),
            pr(repo, 3, user=user("alphastorm-release"), created_at=feed.iso(OLD), state="closed", closed_at=feed.iso(NOW)),
        ]
    gateway = "/repos/" + feed.GATEWAY
    monorepo = "/repos/" + feed.MONOREPO
    result[(gateway + "/issues/comments", 1)] = [comment()]
    result[(gateway + "/releases", 1)] = [release()]
    result[(gateway + "/actions/runs", 1)] = {"workflow_runs": [
        run(feed.GATEWAY, "Upstream OMP canary", 501), run(feed.GATEWAY, "Signed release", 502),
        run(feed.GATEWAY, "Upstream OMP canary", 503, conclusion="success"),
    ]}
    result[(monorepo + "/actions/runs", 1)] = {"workflow_runs": [
        run(feed.MONOREPO, "Release worker", 601, conclusion="success"),
        run(feed.MONOREPO, "Release worker", 602),
        run(feed.MONOREPO, "Provider-free contracts", 603),
        run(feed.MONOREPO, "Provider-free contracts", 604, head_branch="feature"),
    ]}
    return result


def test_collects_all_pipeline_sources_with_conclusions_and_links(config):
    github = FakeGitHub(responses())
    events = feed.collect_events(github, config, RECENT, NOW)
    assert Counter(event["kind"] for event in events) == {
        "tracking": 4, "pr": 6, "release-driver": 1, "release": 1, "run": 5,
    }
    assert {(event["id"], event["state"]) for event in events if event["kind"] == "run"} == {
        (501, "failure"), (502, "failure"), (601, "success"), (602, "failure"), (603, "failure"),
    }
    for event in events:
        text = feed.message(event, config)
        assert text and len(text) <= feed.MAX_TEXT
        assert "PRIVATE" not in text and "<@" not in text
        if event["kind"] != "release-driver":
            assert "https://github.com/" + event["repo"] in text
    assert any(query.get("created", "").startswith(">=") for path, query in github.calls if path.endswith("/actions/runs"))


def test_driver_relays_exactly_escaped_first_line(config):
    event = next(event for event in feed.collect_events(FakeGitHub(responses()), config, RECENT, NOW)
                 if event["kind"] == "release-driver")
    assert event["detail"] == comment()["body"].split("\n")[0]
    assert feed.message(event, config) == "release-driver: preparing — &lt;@U012345678&gt; &amp; build &gt; ready"
    assert "\n" not in feed.message(event, config)


@pytest.mark.parametrize("state", ["awaiting_approve", "WaitingApproval", "waiting for founder"])
def test_driver_state_is_not_coupled_to_a_driver_enum(config, state):
    base = "/repos/" + feed.GATEWAY
    github = FakeGitHub({(base + "/issues", 1): [issue()],
                         (base + "/issues/comments", 1): [comment(body=f"release-driver: {state} — ready")]})
    event = next(event for event in feed.collect_events(github, config, RECENT, NOW) if event["kind"] == "release-driver")
    assert event["state"] == state
    assert feed.message(event, config) == f"release-driver: {state} — ready"


def test_untrusted_text_is_never_converted_before_author_filter(config):
    class Unreadable:
        def __str__(self):
            raise AssertionError("untrusted text was read before author filtering")

    base = "/repos/" + feed.GATEWAY
    github = FakeGitHub({
        (base + "/pulls", 1): [pr(feed.GATEWAY, user=user("outside"), title=Unreadable())],
        (base + "/issues/comments", 1): [comment(user=user("outside"), body=Unreadable())],
    })
    assert feed.collect_events(github, config, RECENT, NOW) == []
    event = {"repo": feed.GATEWAY, "kind": "pr", "id": 1, "number": 1, "state": "opened",
             "author": "outside", "author_type": "User", "detail": Unreadable()}
    assert feed.message(event, config) is None


@pytest.mark.parametrize("source", ["issue", "pr", "comment", "release", "run"])
def test_filters_authors_before_using_external_text(config, source):
    dataset = responses()
    base = "/repos/" + feed.GATEWAY
    endpoints = {"issue": "/issues", "pr": "/pulls", "comment": "/issues/comments", "release": "/releases", "run": "/actions/runs"}
    payload = dataset[(base + endpoints[source], 1)]
    rows = payload["workflow_runs"] if source == "run" else payload
    rows[:] = [rows[0]]
    row = rows[0]
    row[{"run": "actor", "release": "author"}.get(source, "user")] = user("outside-contributor")
    for field in ("body", "title", "name"):
        if field in row:
            row[field] = "UNTRUSTED <@U012345678> text"
    events = feed.collect_events(FakeGitHub(dataset), config, RECENT, NOW)
    kind = {"issue": "tracking", "comment": "release-driver"}.get(source, source)
    assert not any(event["repo"] == feed.GATEWAY and event["kind"] == kind for event in events)


@pytest.mark.parametrize("target", ["issue", "pr"])
def test_app_and_actions_identities_must_be_bots(config, target):
    dataset = responses()
    base = "/repos/" + feed.GATEWAY
    endpoint = "/issues" if target == "issue" else "/pulls"
    rows = dataset[(base + endpoint, 1)]
    rows[:] = [rows[0]]
    rows[0]["user"]["type"] = "User"
    events = feed.collect_events(FakeGitHub(dataset), config, RECENT, NOW)
    assert not any(event["repo"] == feed.GATEWAY and event["kind"] == ("tracking" if target == "issue" else "pr")
                   for event in events)


@pytest.mark.parametrize("title", ["Upstream tracking: v18.8.3 extra", "Upstream tracking: 18.8.3", "Upstream tracking: v18.8.3\n<@U1>"])
def test_tracking_title_is_exact(config, title):
    base = "/repos/" + feed.GATEWAY
    dataset = {(base + "/issues", 1): [issue(title=title)]}
    assert feed.collect_events(FakeGitHub(dataset), config, RECENT, NOW) == []


@pytest.mark.parametrize("issue_url", ["https://outside.invalid/issues/7", "https://api.github.com/repos/other/repo/issues/7"])
def test_comment_cannot_redirect_issue_lookup(config, issue_url):
    github = FakeGitHub({("/repos/" + feed.GATEWAY + "/issues/comments", 1): [comment(issue_url=issue_url)]})
    assert feed.collect_events(github, config, RECENT, NOW) == []
    assert all(any(path.startswith("/repos/" + repo + "/") for repo in feed.REPOSITORIES)
               for path, _ in github.calls)
    assert not any(re.search(r"/issues/[0-9]+$", path) for path, _ in github.calls)


def test_comment_on_nontracking_issue_is_ignored(config):
    base = "/repos/" + feed.GATEWAY
    github = FakeGitHub({(base + "/issues/comments", 1): [comment()], base + "/issues/7": issue(title="General discussion")})
    assert feed.collect_events(github, config, RECENT, NOW) == []
    assert (base + "/issues/7", {}) in github.calls


def test_skips_pr_disguised_as_issue_fork_pr_and_draft_release(config):
    base = "/repos/" + feed.GATEWAY
    github = FakeGitHub({
        (base + "/issues", 1): [issue(pull_request={"url": "ignored"})],
        (base + "/pulls", 1): [pr(feed.GATEWAY, head={"repo": {"full_name": "outside/repo"}}), pr(feed.GATEWAY, head={"repo": None})],
        (base + "/releases", 1): [release(draft=True)],
    })
    assert feed.collect_events(github, config, RECENT, NOW) == []


def test_keys_are_stable_across_payload_changes_and_separate_state_repo_kind(config):
    event = feed.collect_events(FakeGitHub(responses()), config, RECENT, NOW)[0]
    key = feed.event_key(event)
    assert key == feed.event_key(copy.deepcopy(event) | {"detail": "renamed", "at": feed.iso(NOW)})
    for changes in ({"state": "closed"}, {"repo": "other/repo"}, {"kind": "other"}, {"id": event["id"] + 1}):
        assert key != feed.event_key(event | changes)


def test_scheduled_poll_starts_one_child_per_event_then_returns(config, monkeypatch):
    github = FakeGitHub(responses())
    monkeypatch.setattr(poller, "GitHub", lambda: github)
    monkeypatch.setattr(poller, "utcnow", lambda: NOW)
    inp = poller.Input()
    ctx = FakeCtx()
    result = asyncio.run(poller.handler(inp, ctx))
    assert result == {"state": "polled", "since": feed.iso(NOW - dt.timedelta(seconds=feed.LOOKBACK_SECONDS)),
                      "until": feed.iso(NOW), "events": 17}
    assert poller.SCHEDULE["interval_seconds"] == 300 and feed.LOOKBACK_SECONDS == 1800
    assert len(ctx.children) == len(ctx.new_child_keys) == 17
    assert all(child[0] == delivery.WORKFLOW_NAME and set(child[1]) == {"event"} for child in ctx.children)
    assert all(child[2] == feed.event_key(child[1]["event"]) for child in ctx.children)
    assert ctx.posts == []
    before = len(github.calls), len(ctx.children)
    asyncio.run(poller.handler(inp, ctx))
    assert before == (len(github.calls), len(ctx.children))


def test_overlapping_ticks_start_no_duplicate_child_keys(config, monkeypatch):
    github = FakeGitHub(responses())
    monkeypatch.setattr(poller, "GitHub", lambda: github)
    monkeypatch.setattr(poller, "utcnow", lambda: NOW)
    first = FakeCtx()
    assert asyncio.run(poller.handler(poller.Input(), first))["events"] == 17
    previous_reads = len(github.calls)
    monkeypatch.setattr(poller, "utcnow", lambda: NOW + dt.timedelta(seconds=feed.INTERVAL_SECONDS))
    second = FakeCtx()
    second.starts = first.starts
    assert asyncio.run(poller.handler(poller.Input(), second))["events"] == 17
    assert len(github.calls) > previous_reads
    assert {child[2] for child in first.children} == {child[2] for child in second.children}
    assert len(first.new_child_keys) == 17 and second.new_child_keys == []
    assert len(second.starts) == 17


def test_one_failed_tick_does_not_stop_the_next_tick(config, monkeypatch):
    class OneFailedRead(FakeGitHub):
        failed = False

        def get(self, path, **query):
            if not self.failed:
                self.failed = True
                raise RuntimeError("transient GitHub read failure")
            return super().get(path, **query)

    github = OneFailedRead(responses())
    monkeypatch.setattr(poller, "GitHub", lambda: github)
    monkeypatch.setattr(poller, "utcnow", lambda: NOW)
    failed = FakeCtx()
    with pytest.raises(RuntimeError, match="transient GitHub read failure"):
        asyncio.run(poller.handler(poller.Input(), failed))
    assert not failed.children and not failed.posts
    monkeypatch.setattr(poller, "utcnow", lambda: NOW + dt.timedelta(seconds=feed.INTERVAL_SECONDS))
    succeeding = FakeCtx()
    assert asyncio.run(poller.handler(poller.Input(), succeeding))["events"] == 17
    assert len(succeeding.new_child_keys) == 17
    assert all(child[0] == delivery.WORKFLOW_NAME for child in succeeding.children)


def test_events_outside_lookback_or_in_the_future_are_never_posted(config, monkeypatch):
    github = FakeGitHub({("/repos/" + feed.GATEWAY + "/issues", 1): [
        issue(created_at=feed.iso(NOW - dt.timedelta(seconds=feed.LOOKBACK_SECONDS + 1))),
        issue(8, created_at=feed.iso(NOW + dt.timedelta(seconds=1))),
        issue(9, created_at=feed.iso(NOW - dt.timedelta(seconds=feed.LOOKBACK_SECONDS))),
        issue(10, created_at=feed.iso(NOW - dt.timedelta(seconds=1))),
    ]})
    monkeypatch.setattr(poller, "GitHub", lambda: github)
    monkeypatch.setattr(poller, "utcnow", lambda: NOW)
    ctx = FakeCtx()
    result = asyncio.run(poller.handler(poller.Input(), ctx))
    assert result["events"] == 2
    assert result["since"] == feed.iso(NOW - dt.timedelta(seconds=feed.LOOKBACK_SECONDS))
    assert {child[1]["event"]["number"] for child in ctx.children} == {9, 10}
    posted = []
    for _, payload, _ in ctx.children:
        child_ctx = FakeCtx()
        asyncio.run(delivery.handler(delivery.Input(**payload), child_ctx))
        posted.extend(child_ctx.posts)
    assert len(posted) == 2


@pytest.mark.parametrize("channel", ["", "<OMP_MONOREPO_CHANNEL_ID>", "CPLACEHOLDER", "#omp-monorepo", "C0EXAMPLE123"])
def test_placeholder_config_is_silent(tmp_path, monkeypatch, channel):
    path = tmp_path / "omp_channels.json"
    path.write_text(json.dumps({"channels": {repo: channel for repo in feed.REPOSITORIES},
                                "app_bot_login": "alpha-founder-source-alphastorm[bot]",
                                "release_bot_login": "alphastorm-release", "founder_login": "alphastorm"}))
    monkeypatch.setattr(feed, "CONFIG_PATH", path)
    monkeypatch.setattr(poller, "GitHub", lambda: pytest.fail("unconfigured feed must not read GitHub"))
    config = feed.load_config()
    assert config["channels"] == {}
    ctx = FakeCtx()
    assert asyncio.run(poller.handler(poller.Input(), ctx)) == {"state": "unconfigured", "events": 0}
    inp = delivery.Input(event={"repo": feed.MONOREPO, "id": 1})
    assert asyncio.run(delivery.handler(inp, ctx)) == {"state": "ignored"}
    assert not ctx.posts and not ctx.children


def test_partial_configuration_only_reads_mapped_repo(config):
    config["channels"].pop(feed.MONOREPO)
    github = FakeGitHub(responses())
    events = feed.collect_events(github, config, RECENT, NOW)
    assert events and all(event["repo"] == feed.GATEWAY for event in events)
    assert all(path.startswith("/repos/" + feed.GATEWAY) for path, _ in github.calls)


def test_event_post_is_checkpointed_with_stable_slack_client_id(config):
    event = next(event for event in feed.collect_events(FakeGitHub(responses()), config, RECENT, NOW) if event["kind"] == "pr")
    inp = delivery.Input(event=event)
    ctx = FakeCtx()
    assert asyncio.run(delivery.handler(inp, ctx))["state"] == "posted"
    asyncio.run(delivery.handler(inp, ctx))
    assert len(ctx.posts) == 1
    channel, text, args = ctx.posts[0]
    assert channel == config["channels"][event["repo"]]
    assert text == feed.message(event, config)
    assert args == {"client_msg_id": str(uuid.uuid5(uuid.NAMESPACE_URL, feed.event_key(event))),
                    "unfurl_links": False, "unfurl_media": False}


def test_delivery_rechecks_author_and_configuration(config):
    event = next(event for event in feed.collect_events(FakeGitHub(responses()), config, RECENT, NOW) if event["kind"] == "pr")
    ctx = FakeCtx()
    assert asyncio.run(delivery.handler(delivery.Input(event=event | {"author": "outside"}), ctx)) == {"state": "ignored"}
    config["channels"].pop(event["repo"])
    assert asyncio.run(delivery.handler(delivery.Input(event=event), ctx)) == {"state": "ignored"}
    assert not ctx.posts


def test_escaping_and_message_caps(config):
    assert feed.escape_text(" & < > ") == "&amp; &lt; &gt;"
    escaped = feed.escape_text("<&>" * 5000)
    assert len(escaped) <= feed.MAX_DETAIL
    assert "<" not in escaped and ">" not in escaped and re.search(r"&(?!amp;|lt;|gt;)", escaped) is None
    event = {"repo": feed.GATEWAY, "kind": "pr", "state": "opened", "id": 1, "number": 1,
             "author": config["release_bot_login"], "author_type": "User", "detail": "<&>" * 5000}
    assert len(feed.message(event, config)) <= feed.MAX_TEXT


def test_stdlib_get_uses_injected_token_and_never_follows_redirects(monkeypatch):
    calls = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def read(self, maximum):
            assert maximum == feed.MAX_RESPONSE_BYTES + 1
            return b'[{"id":1}]'

    class Opener:
        def open(self, request, *, timeout):
            calls.append((request, timeout))
            return Response()

    monkeypatch.setenv("GITHUB_TOKEN", "GITHUB_TOKEN")
    monkeypatch.setattr(feed, "build_opener", lambda handler: Opener())
    assert feed.GitHub().get("/repos/" + feed.GATEWAY + "/issues", since=feed.iso(RECENT)) == [{"id": 1}]
    request, timeout = calls[0]
    assert request.get_method() == "GET" and timeout == 30
    assert request.get_header("Authorization") == "Bearer GITHUB_TOKEN"
    assert urlsplit(request.full_url).netloc == "api.github.com"
    assert parse_qs(urlsplit(request.full_url).query)["since"] == [feed.iso(RECENT)]
    assert feed.NoRedirect().redirect_request(None, None, 302, "", {}, "https://outside.invalid") is None


def test_pagination_exhausts_pages_and_fails_closed_at_bound(monkeypatch):
    monkeypatch.setattr(feed, "PAGE_SIZE", 2)
    monkeypatch.setattr(feed, "MAX_PAGES", 2)
    path = "/repos/" + feed.GATEWAY + "/issues"
    github = FakeGitHub({(path, 1): [{"id": 1}, {"id": 2}], (path, 2): [{"id": 3}]})
    assert [row["id"] for row in github.items(path)] == [1, 2, 3]
    github.responses[(path, 2)].append({"id": 4})
    with pytest.raises(ValueError, match="pagination exceeds"):
        list(github.items(path))


def test_event_cap_fails_before_deliveries(config, monkeypatch):
    monkeypatch.setattr(feed, "MAX_EVENTS", 1)
    monkeypatch.setattr(poller, "GitHub", lambda: FakeGitHub(responses()))
    monkeypatch.setattr(poller, "utcnow", lambda: NOW)
    ctx = FakeCtx()
    with pytest.raises(ValueError, match="events exceed"):
        asyncio.run(poller.handler(poller.Input(), ctx))
    assert not ctx.children and not ctx.posts


@pytest.mark.parametrize("persona", ["omp_monorepo", "omp_session_gateway"])
def test_persona_layout_matches_centaur_discovery(persona):
    directory = ROOT / "tools/personas" / persona
    config = tomllib.loads((directory / "pyproject.toml").read_text())
    assert re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", persona)
    assert config["tool"]["centaur"] == {"type": "persona", "prompt_file": "PROMPT.md"}
    prompt = (directory / "PROMPT.md").read_text()
    assert "read-only" in prompt and "Alpha Founder work orders" in prompt and "GITHUB_TOKEN" in prompt
