"""Opt-in, bounded Notion reads through a Centaur-custodied integration grant.

No workflow is registered unless CARRYTHROUGH_NOTION_SNAPSHOT_ENABLED=1.
The Authorization value is a proxy placeholder, never a token loaded by Python.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any
from uuid import UUID

WORKFLOW_NAME = "carrythrough_notion_snapshot"
WORKFLOW_PRINCIPAL = "carrythrough-notion"
CREDENTIAL_NAME = "NOTION_API_KEY"
REQUEST_SCHEMA = "carrythrough.notion-snapshot-request.v1"
RESULT_SCHEMA = "carrythrough.notion-snapshot-result.v1"
NOTION_VERSION = "2025-09-03"
LIMITS = {
    "max_pages": (20, 1, 50),
    "max_blocks": (500, 1, 5000),
    "max_rows": (100, 1, 500),
    "max_depth": (6, 0, 16),
    "max_bytes": (262144, 1024, 1048576),
}


def _id(value: Any) -> str:
    if (
        not isinstance(value, str)
        or re.fullmatch(
            r"[0-9a-fA-F]{32}|[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}",
            value,
        )
        is None
    ):
        raise ValueError("Notion identifier is invalid")
    return str(UUID(value))


class Input(dict):
    """Preserve original JSON rather than allowing host dataclass coercion."""

    @classmethod
    def parse(cls, raw: Any) -> Input:
        required = {"request_id", "workspace_id", "resource_id", "resource_kind"}
        if (
            not isinstance(raw, dict)
            or set(raw) - required - set(LIMITS) - {"schema_version"}
            or required - set(raw)
        ):
            raise ValueError("Notion input must have exactly the supported fields")
        value = cls(raw)
        if value.get("schema_version", REQUEST_SCHEMA) != REQUEST_SCHEMA:
            raise ValueError("Notion schema_version is invalid")
        value["schema_version"] = REQUEST_SCHEMA
        if not isinstance(value["request_id"], str) or not 8 <= len(value["request_id"]) <= 128:
            raise ValueError("Notion request_id is invalid")
        for field in ("workspace_id", "resource_id"):
            value[field] = _id(value[field])
        if value["resource_kind"] not in ("page", "database"):
            raise ValueError("Notion resource_kind is invalid")
        for name, (default, minimum, maximum) in LIMITS.items():
            limit = value.setdefault(name, default)
            if type(limit) is not int or not minimum <= limit <= maximum:
                raise ValueError("Notion read bound is invalid")
        return value


class ReadError(Exception):
    def __init__(self, code: str, retry_after_seconds: int | None = None):
        super().__init__(code)
        self.code = code
        self.retry_after_seconds = retry_after_seconds


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class NotionClient:
    def __init__(self, max_bytes: int):
        self.remaining = max_bytes
        # Keep the sandbox's managed proxy; never add a direct-network fallback.
        self.opener = urllib.request.build_opener(NoRedirect())

    def read(self, path: str, body: dict | None = None) -> dict:
        if not path.startswith("/v1/") or "#" in path:
            raise ReadError("invalid_result")
        request = urllib.request.Request(
            "https://api.notion.com" + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Authorization": "Bearer " + CREDENTIAL_NAME,
                "Notion-Version": NOTION_VERSION,
                "Content-Type": "application/json",
            },
            method="GET" if body is None else "POST",
        )
        try:
            with self.opener.open(request, timeout=30) as response:
                raw = response.read(self.remaining + 1)
            self.remaining -= len(raw)
            if self.remaining < 0:
                raise ReadError("bounds_exceeded")
            value = json.loads(raw)
        except urllib.error.HTTPError as error:
            code = {
                401: "access_denied",
                403: "access_denied",
                404: "not_found",
                429: "rate_limited",
            }.get(error.code, "unavailable")
            delay = error.headers.get("Retry-After", "")
            retry_after = min(int(delay), 86400) if delay.isdigit() else None
            raise ReadError(code, retry_after) from None
        except (OSError, ValueError) as error:
            raise ReadError("unavailable") from error
        return _record(value)


def _record(value: Any) -> dict:
    if not isinstance(value, dict):
        raise ReadError("invalid_result")
    if value.get("object") == "error" or (
        isinstance(value.get("errors"), list) and value["errors"]
    ):
        raise ReadError("partial_result")
    if value.get("archived") is True or value.get("in_trash") is True:
        raise ReadError("not_found")
    return value


def _client(max_bytes: int) -> NotionClient:
    return NotionClient(max_bytes)


def _unavailable(inp: Input, error: ReadError) -> dict:
    return {
        "schema_version": RESULT_SCHEMA,
        "request_id": inp["request_id"],
        "workspace_id": inp["workspace_id"],
        "resource_id": inp["resource_id"],
        "resource_kind": inp["resource_kind"],
        "state": "unavailable",
        "resource": None,
        "final_revision": None,
        "block_pages": [],
        "database_pages": [],
        "omissions": [],
        "error": {
            "code": error.code,
            "resource_id": inp["resource_id"],
            "retry_after_seconds": error.retry_after_seconds,
        },
    }


def capture(inp: Input, client: NotionClient) -> dict:
    """Only explicitly selected roots/descendants; no search, comments, or media I/O."""
    result = {
        "schema_version": RESULT_SCHEMA,
        "request_id": inp["request_id"],
        "workspace_id": inp["workspace_id"],
        "resource_id": inp["resource_id"],
        "resource_kind": inp["resource_kind"],
        "state": "available",
        "resource": None,
        "final_revision": None,
        "block_pages": [],
        "database_pages": [],
        "omissions": [],
        "error": None,
    }
    omissions: set[str] = set()
    page_count = 0
    object_count = 0
    seen_blocks: set[str] = set()

    def pages(parent: str, *, database: bool) -> list[dict]:
        nonlocal page_count, object_count
        cursor = None
        seen_cursors: set[str] = set()
        values: list[dict] = []
        label = "database" if database else "children"
        limit = inp["max_rows"] if database else inp["max_blocks"]
        while True:
            if page_count >= inp["max_pages"] or object_count >= limit:
                omissions.add(f"{label}_truncated:{parent}")
                break
            query: dict[str, int | str] = {"page_size": 100}
            if cursor is not None:
                query["start_cursor"] = cursor
            response = _record(
                client.read(
                    f"/v1/data_sources/{parent}/query"
                    if database
                    else f"/v1/blocks/{parent}/children?{urllib.parse.urlencode(query)}",
                    query if database else None,
                )
            )
            page_count += 1
            items = response.get("results")
            has_more, next_cursor = response.get("has_more"), response.get("next_cursor")
            if (
                not isinstance(items, list)
                or len(items) > 100
                or type(has_more) is not bool
                or has_more != (isinstance(next_cursor, str) and bool(next_cursor))
                or (not has_more and next_cursor is not None)
            ):
                raise ReadError("partial_result")
            for item in items:
                _record(item)
            room = limit - object_count
            if len(items) > room:
                omissions.add(f"{label}_truncated:{parent}")
            items = items[:room]
            object_count += len(items)
            page = {
                "parent_id": parent,
                "start_cursor": cursor,
                "next_cursor": next_cursor,
                "has_more": has_more,
                "results": items,
            }
            result["database_pages" if database else "block_pages"].append(page)
            values.extend(items)
            if not has_more:
                break
            if not isinstance(next_cursor, str):
                raise ReadError("partial_result")
            if next_cursor in seen_cursors:
                raise ReadError("partial_result")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return values

    def walk(parent: str, depth: int) -> None:
        for block in pages(parent, database=False):
            try:
                identifier = _id(block.get("id"))
            except ValueError as error:
                raise ReadError("invalid_result") from error
            if identifier in seen_blocks:
                raise ReadError("invalid_result")
            seen_blocks.add(identifier)
            if type(block.get("has_children")) is not bool:
                raise ReadError("partial_result")
            if block["has_children"]:
                if depth >= inp["max_depth"]:
                    omissions.add(f"children_truncated:{identifier}")
                else:
                    walk(identifier, depth + 1)

    try:
        identity = _record(client.read("/v1/users/me"))
        bot = identity.get("bot")
        if (
            identity.get("type") != "bot"
            or not isinstance(bot, dict)
            or _id(bot.get("workspace_id")) != inp["workspace_id"]
        ):
            raise ReadError("out_of_scope")
        path = (
            f"/v1/{'pages' if inp['resource_kind'] == 'page' else 'databases'}/{inp['resource_id']}"
        )
        resource = _record(client.read(path))
        if (
            resource.get("object") != inp["resource_kind"]
            or _id(resource.get("id")) != inp["resource_id"]
        ):
            raise ReadError("out_of_scope")
        revision = resource.get("last_edited_time")
        if not isinstance(revision, str) or not revision:
            raise ReadError("partial_result")
        result["resource"] = resource
        if inp["resource_kind"] == "page":
            walk(inp["resource_id"], 0)
        else:
            sources = resource.get("data_sources")
            if not isinstance(sources, list) or not sources or len(sources) > 50:
                raise ReadError("partial_result")
            identifiers = [_id(_record(source).get("id")) for source in sources]
            if len(set(identifiers)) != len(identifiers):
                raise ReadError("invalid_result")
            for identifier in sorted(identifiers):
                pages(identifier, database=True)
        final = _record(client.read(path))
        if final.get("last_edited_time") != revision or _id(final.get("id")) != inp["resource_id"]:
            raise ReadError("source_changed")
        result["final_revision"] = revision
        result["omissions"] = sorted(omissions)
        result["state"] = "truncated" if omissions else "available"
        if (
            len(json.dumps(result, separators=(",", ":"), ensure_ascii=False).encode())
            > inp["max_bytes"]
        ):
            raise ReadError("bounds_exceeded")
        return result
    except ReadError as error:
        return _unavailable(inp, error)
    except (ValueError, TypeError, KeyError):
        return _unavailable(inp, ReadError("invalid_result"))


async def _handler(inp, ctx) -> dict:
    inp = Input.parse(inp)
    client = _client(inp["max_bytes"])
    return await ctx.step("notion-snapshot-read", lambda: asyncio.to_thread(capture, inp, client))


# Native discovery requires a callable handler. Default import is intentionally inert,
# including WORKFLOW_ENABLE_MODE=all; no principal or grant is registered automatically.
handler = _handler if os.environ.get("CARRYTHROUGH_NOTION_SNAPSHOT_ENABLED") == "1" else None
