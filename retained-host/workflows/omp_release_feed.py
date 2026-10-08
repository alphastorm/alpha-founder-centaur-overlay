"""Stateless five-minute retained-host poll over an overlapping GitHub window."""

from __future__ import annotations

from dataclasses import dataclass, field
import datetime as dt
from typing import Any

from api.workflow_engine import WorkflowContext
from _omp_feed import (
    GitHub, INTERVAL_SECONDS, LOOKBACK_SECONDS, collect_events,
    event_key, iso, load_config, timestamp, utcnow,
)

WORKFLOW_NAME = "omp_release_feed"
WORKFLOW_PRINCIPAL = "omp-release-feed"
SCHEDULE = {
    "schedule_id": WORKFLOW_NAME,
    "interval_seconds": INTERVAL_SECONDS,
    "enabled": True,
    "no_delivery": True,
}


@dataclass
class Input:
    metadata: dict[str, Any] = field(default_factory=dict)


async def handler(inp: Input, ctx: WorkflowContext) -> dict[str, Any]:
    config = load_config()
    if not config["channels"]:
        return {"state": "unconfigured", "events": 0}
    until_text = await ctx.step("window-until", lambda: iso(utcnow()))
    until = timestamp(until_text)
    if until is None:
        raise ValueError("invalid feed window")
    since = until - dt.timedelta(seconds=LOOKBACK_SECONDS)
    events = await ctx.step("github-events", lambda: collect_events(GitHub(), config, since, until))
    for event in events:
        key = event_key(event)
        await ctx.step(
            "start:" + key,
            lambda event=event, key=key: ctx.start_workflow(
                "omp_release_feed_event", {"event": event}, idempotency_key=key,
            ),
        )
    return {"state": "polled", "since": iso(since), "until": until_text, "events": len(events)}
