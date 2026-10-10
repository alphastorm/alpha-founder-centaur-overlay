"""Five-minute aggregate-only progress read under the existing ADR-0121 principal."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from typing import Any
from urllib.request import Request, build_opener

from api.workflow_engine import WorkflowContext
from _omp_feed import NoRedirect, load_config, MONOREPO

WORKFLOW_NAME = "carrythrough_progress_feed"
WORKFLOW_PRINCIPAL = "omp-release-feed"
SCHEDULE = {
    "schedule_id": WORKFLOW_NAME,
    "interval_seconds": 300,
    "enabled": False,
    "no_delivery": True,
}
PROGRESS_URL = "http://carrythrough-progress.carrythrough.svc:8000/api/maintenance/progress"
MAX_BYTES = 1024 * 1024


@dataclass
class Input:
    metadata: dict[str, Any] = field(default_factory=dict)


def read_progress() -> dict[str, Any]:
    # iron-proxy replaces this placeholder only for the existing feed principal
    # and the exact GET host/path grant. The actual credential never enters Python.
    request = Request(
        PROGRESS_URL, headers={"Authorization": "Bearer CARRYTHROUGH_PROGRESS_TOKEN"}, method="GET",
    )
    try:
        with build_opener(NoRedirect()).open(request, timeout=10) as response:
            body = response.read(MAX_BYTES + 1)
        if len(body) > MAX_BYTES:
            raise ValueError("progress response exceeds bound")
        result = json.loads(body)
        if result.get("schema_version") != "carrythrough.operational-progress.v1":
            raise ValueError("unexpected progress schema")
        conditions = result["conditions"]
        if not isinstance(conditions, list) or len(conditions) > 100:
            raise ValueError("invalid progress conditions")
        # Persist only the public, bounded conditions, not the entire response.
        return {"conditions": conditions}
    except Exception:
        raise RuntimeError("operational progress read failed") from None


def event_key(condition: dict[str, Any]) -> str:
    key, message = condition.get("key"), condition.get("message")
    if not isinstance(key, str) or not 1 <= len(key) <= 512:
        raise ValueError("invalid progress key")
    if not isinstance(message, str) or not 1 <= len(message) <= 1000:
        raise ValueError("invalid progress message")
    identity = json.dumps([key, message], separators=(",", ":"))
    return "carrythrough-progress:" + hashlib.sha256(identity.encode()).hexdigest()


async def handler(inp: Input, ctx: WorkflowContext) -> dict[str, Any]:
    if not load_config()["channels"].get(MONOREPO):
        return {"state": "unconfigured", "events": 0}
    document = await ctx.step("progress", read_progress)
    for condition in document["conditions"]:
        key = event_key(condition)
        await ctx.step(
            "start:" + key,
            lambda condition=condition, key=key: ctx.start_workflow(
                "carrythrough_progress_feed_event", {"condition": condition},
                idempotency_key=key,
            ),
        )
    return {"state": "polled", "events": len(document["conditions"])}
