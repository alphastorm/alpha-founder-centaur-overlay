"""Read-only GitHub collection and bounded Slack text shared by the OMP feed."""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

MONOREPO = "alphastorm/omp-monorepo"
GATEWAY = "alphastorm/omp-session-gateway"
REPOSITORIES = (MONOREPO, GATEWAY)
CONFIG_PATH = Path(__file__).with_name("omp_channels.json")
INTERVAL_SECONDS = 300
LOOKBACK_SECONDS = 6 * INTERVAL_SECONDS
RUN_CREATED_LOOKBACK = dt.timedelta(days=2)
MAX_EVENTS = 100
MAX_TEXT = 1500
MAX_DETAIL = 600
PAGE_SIZE = 100
MAX_PAGES = 10
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
ACTIONS_BOT = "github-actions[bot]"
TRACKING_TITLE = re.compile(r"Upstream tracking: (v[0-9]+\.[0-9]+\.[0-9]+)")
DRIVER_LINE = re.compile(r"release-driver: ([^\r\n]+?) — [^\r\n]+")
CHANNEL_ID = re.compile(r"[CG][A-Z0-9]{8,20}")
FAILURES = {"failure", "timed_out", "startup_failure", "action_required"}
CONCLUSIONS = FAILURES | {"success", "cancelled", "neutral", "skipped", "stale"}
RUN_NAMES = {
    GATEWAY: {"Upstream OMP canary", "Keyless release"},
    MONOREPO: {"Release worker", "Provider-free contracts"},
}


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def timestamp(value: Any) -> dt.datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(dt.timezone.utc) if parsed.tzinfo is not None else None


def iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def load_config() -> dict[str, Any]:
    raw = json.loads(CONFIG_PATH.read_text())
    channels = raw.get("channels", {})
    return {
        "channels": {
            repo: channel for repo in REPOSITORIES
            if isinstance(channel := channels.get(repo), str)
            and CHANNEL_ID.fullmatch(channel)
            and not any(marker in channel for marker in ("PLACEHOLDER", "EXAMPLE", "REPLACE", "TBD"))
        },
        **{
            key: value.casefold() if isinstance(value := raw.get(key), str) else ""
            for key in ("app_bot_login", "release_bot_login", "founder_login")
        },
    }


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GitHub:
    """Only GETs to api.github.com; the sandbox proxy replaces GITHUB_TOKEN."""

    def __init__(self):
        self.token = os.environ["GITHUB_TOKEN"]
        self.opener = build_opener(NoRedirect())

    def get(self, path: str, **query: Any) -> Any:
        request = Request(
            "https://api.github.com" + path + ("?" + urlencode(query) if query else ""),
            headers={
                "Authorization": "Bearer " + self.token,
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "alpha-founder-omp-status-feed",
            },
            method="GET",
        )
        with self.opener.open(request, timeout=30) as response:
            body = response.read(MAX_RESPONSE_BYTES + 1)
        if len(body) > MAX_RESPONSE_BYTES:
            raise ValueError("GitHub response exceeds feed limit")
        return json.loads(body)

    def items(self, path: str, *, key: str | None = None,
              updated_since: dt.datetime | None = None, **query: Any):
        for page in range(1, MAX_PAGES + 1):
            payload = self.get(path, per_page=PAGE_SIZE, page=page, **query)
            rows = payload[key] if key else payload
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise ValueError("unexpected GitHub list response")
            yield from rows
            if len(rows) < PAGE_SIZE:
                return
            # Only the PR endpoint is requested in descending updated order.
            oldest = timestamp(rows[-1].get("updated_at"))
            if updated_since is not None and oldest is not None and oldest < updated_since:
                return
        raise ValueError("GitHub pagination exceeds feed limit")


def author(user: Any) -> tuple[str, str]:
    if not isinstance(user, dict):
        return "", ""
    login = user.get("login")
    return login.casefold() if isinstance(login, str) else "", user.get("type", "")


def trusted_author(user: Any, config: dict[str, Any], allowed: tuple[str, ...]) -> bool:
    login, kind = author(user)
    if not login or login not in allowed:
        return False
    return kind == "Bot" if login in (ACTIONS_BOT, config["app_bot_login"]) else True


def tracking_issue(issue: dict[str, Any], config: dict[str, Any]) -> bool:
    if "pull_request" in issue or not trusted_author(issue.get("user"), config, (ACTIONS_BOT,)):
        return False
    title = issue.get("title")
    return isinstance(title, str) and TRACKING_TITLE.fullmatch(title) is not None


def positive_id(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def collect_events(github: GitHub, config: dict[str, Any],
                   since: dt.datetime, until: dt.datetime) -> list[dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}

    def add(repo, kind, identifier, state, when, user, **fields):
        at = timestamp(when)
        if not positive_id(identifier) or at is None or not since <= at <= until:
            return
        login, user_type = author(user)
        event = {"repo": repo, "kind": kind, "id": identifier, "state": state,
                 "at": iso(at), "author": login, "author_type": user_type, **fields}
        events[event_key(event)] = event
        if len(events) > MAX_EVENTS:
            raise ValueError("GitHub events exceed feed limit")

    for repo in REPOSITORIES:
        if repo not in config["channels"]:
            continue
        base = "/repos/" + repo
        issues = {}
        for issue in github.items(base + "/issues", state="all", since=iso(since),
                                  sort="updated", direction="desc"):
            if not tracking_issue(issue, config) or not positive_id(issue.get("number")):
                continue
            number = issue["number"]
            issues[number] = issue
            add(repo, "tracking", issue.get("id"), "opened", issue.get("created_at"),
                issue["user"], number=number, detail=issue["title"])
            if issue.get("state") == "closed":
                add(repo, "tracking", issue.get("id"), "closed", issue.get("closed_at"),
                    issue["user"], number=number, detail=issue["title"])

        for pr in github.items(base + "/pulls", state="all", sort="updated", direction="desc",
                               updated_since=since):
            if not trusted_author(pr.get("user"), config,
                                  (config["app_bot_login"], config["release_bot_login"])):
                continue
            head_repo = (pr.get("head") or {}).get("repo") or {}
            if not positive_id(pr.get("number")) or head_repo.get("full_name") != repo:
                continue
            fields = {"number": pr["number"], "detail": str(pr.get("title", ""))[:MAX_DETAIL]}
            add(repo, "pr", pr.get("id"), "opened", pr.get("created_at"), pr["user"], **fields)
            if pr.get("merged_at"):
                add(repo, "pr", pr.get("id"), "merged", pr["merged_at"], pr["user"], **fields)
            elif pr.get("state") == "closed":
                add(repo, "pr", pr.get("id"), "closed", pr.get("closed_at"), pr["user"], **fields)

        if repo == GATEWAY:
            for comment in github.items(base + "/issues/comments", since=iso(since),
                                         sort="updated", direction="desc"):
                if not trusted_author(comment.get("user"), config, (config["release_bot_login"],)):
                    continue
                created = timestamp(comment.get("created_at"))
                if created is None or not since <= created <= until:
                    continue
                match = re.fullmatch(r"https://api\.github\.com/repos/" + re.escape(repo)
                                     + r"/issues/([1-9][0-9]*)", str(comment.get("issue_url", "")))
                if match is None:
                    continue
                number = int(match[1])
                if number not in issues:
                    issues[number] = github.get(base + "/issues/" + str(number))
                if not tracking_issue(issues[number], config):
                    continue
                # Never retain or post the rest of a comment, even from the bot.
                line = str(comment.get("body", "")).split("\n", 1)[0].rstrip("\r")
                driver = DRIVER_LINE.fullmatch(line)
                if driver:
                    add(repo, "release-driver", comment.get("id"), driver[1], comment["created_at"],
                        comment["user"], number=number, detail=line[:MAX_DETAIL])

            # Newest first: a release published inside the lookback is on the first page.
            releases = github.get(base + "/releases", per_page=PAGE_SIZE)
            if not isinstance(releases, list) or any(not isinstance(row, dict) for row in releases):
                raise ValueError("unexpected GitHub list response")
            for release in releases:
                if not trusted_author(release.get("author"), config, (config["release_bot_login"],)):
                    continue
                tag = release.get("tag_name")
                if release.get("draft") or not isinstance(tag, str) or not 1 <= len(tag) <= 128:
                    continue
                add(repo, "release", release.get("id"), "published", release.get("published_at"),
                    release["author"], tag=tag)

        # Completion is filtered by updated_at, not created_at: a long run may finish
        # inside this poll. Two days of creation covers every run these workflows make
        # (the Studio release worker is the longest, hours) and keeps a busy repository
        # well under MAX_PAGES; 35 days of the gateway's CI alone is over 1,200 runs.
        for run in github.items(base + "/actions/runs", key="workflow_runs", status="completed",
                                created=">=" + iso(until - RUN_CREATED_LOOKBACK)):
            if not trusted_author(run.get("actor"), config,
                                  (ACTIONS_BOT, config["app_bot_login"], config["release_bot_login"],
                                   config["founder_login"])):
                continue
            name = run.get("name")
            conclusion = run.get("conclusion")
            if run.get("status") != "completed" or name not in RUN_NAMES[repo] or conclusion not in CONCLUSIONS:
                continue
            if (run.get("head_repository") or {}).get("full_name") != repo:
                continue
            if name == "Provider-free contracts" and run.get("head_branch") != "main":
                continue
            if name != "Release worker" and conclusion not in FAILURES:
                continue
            add(repo, "run", run.get("id"), conclusion, run.get("updated_at"), run["actor"], label=name)

    return sorted(events.values(), key=lambda event: (event["at"], event_key(event)))


def event_key(event: dict[str, Any]) -> str:
    return f"omp-feed:v1:{event['repo']}:{event['kind']}:{event['id']}:{event['state']}"


def escape_text(value: str, limit: int = MAX_DETAIL) -> str:
    result = []
    size = 0
    for char in " ".join(value.split()):
        escaped = {"&": "&amp;", "<": "&lt;", ">": "&gt;"}.get(char, char)
        if size + len(escaped) > limit - 1:
            return "".join(result) + "…"
        result.append(escaped)
        size += len(escaped)
    return "".join(result)


def message(event: dict[str, Any], config: dict[str, Any]) -> str | None:
    repo, kind, state = (event.get(key) for key in ("repo", "kind", "state"))
    if repo not in config["channels"] or not positive_id(event.get("id")):
        return None
    user = {"login": event.get("author"), "type": event.get("author_type")}
    allowed = {
        "tracking": (ACTIONS_BOT,),
        "pr": (config["app_bot_login"], config["release_bot_login"]),
        "release-driver": (config["release_bot_login"],),
        "release": (config["release_bot_login"],),
        "run": (ACTIONS_BOT, config["app_bot_login"], config["release_bot_login"], config["founder_login"]),
    }.get(kind)
    if allowed is None or not trusted_author(user, config, allowed):
        return None
    base = "https://github.com/" + repo
    detail = str(event.get("detail", ""))
    if kind in ("tracking", "pr", "release-driver"):
        if not positive_id(event.get("number")):
            return None
        if kind == "tracking":
            if state not in ("opened", "closed") or TRACKING_TITLE.fullmatch(detail) is None:
                return None
            summary = f"tracking issue #{event['number']} {state}: {escape_text(detail)}"
        elif kind == "pr":
            if state not in ("opened", "merged", "closed"):
                return None
            summary = f"PR #{event['number']} {state}: {escape_text(detail)}"
        else:
            driver = DRIVER_LINE.fullmatch(detail)
            if repo != GATEWAY or driver is None or driver[1] != state:
                return None
            # Relay exactly the escaped first line, never the driver's later links.
            return escape_text(detail)
        suffix = "/pull/" if kind == "pr" else "/issues/"
        link = base + suffix + str(event["number"])
    elif kind == "release":
        tag = event.get("tag")
        if repo != GATEWAY or state != "published" or not isinstance(tag, str) or not 1 <= len(tag) <= 128:
            return None
        summary = "release published: " + escape_text(tag)
        link = base + "/releases/tag/" + quote(tag, safe="")
    elif kind == "run":
        label = event.get("label")
        if label not in RUN_NAMES[repo] or state not in CONCLUSIONS:
            return None
        if label != "Release worker" and state not in FAILURES:
            return None
        summary = f"{label} completed: {state}"
        link = base + "/actions/runs/" + str(event["id"])
    else:
        return None
    text = f"{repo}: {summary}\n{link}"
    if len(text) > MAX_TEXT:
        raise ValueError("feed message exceeds limit")
    return text
