"""Five-minute retained-host feed with a durable seed and bounded poll handoffs."""

from __future__ import annotations

from dataclasses import dataclass, field
import datetime as dt
from typing import Any

from api.workflow_engine import WorkflowContext
from _omp_feed import (
    GitHub, INTERVAL_SECONDS, LOOKBACK_SECONDS, OVERLAP_SECONDS, collect_events,
    event_key, generation, iso, load_config, timestamp, utcnow,
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
    seeded_at: str | None = None
    since: str | None = None
    generation: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


async def handler(inp: Input, ctx: WorkflowContext) -> dict[str, Any]:
    config = load_config()
    if not config["channels"]:
        return {"state": "unconfigured", "events": 0}
    current = generation(config)
    if inp.since is None:
        # Scheduled ticks all name the same initial run. Its recorded seed, not
        # an overlap window before deployment, determines the first eligible event.
        seed = await ctx.step("seed-from-now", lambda: iso(utcnow()))
        child = await ctx.start_workflow(
            WORKFLOW_NAME,
            {"seeded_at": seed, "since": seed, "generation": current},
            idempotency_key="omp-feed:seed:" + current,
        )
        return {"state": "seeded" if child["created"] else "scheduled", "events": 0}
    if inp.generation != current:
        return {"state": "retired", "events": 0}
    seed, previous = timestamp(inp.seeded_at), timestamp(inp.since)
    if seed is None or previous is None or previous < seed:
        raise ValueError("invalid feed cursor")

    # A run sleeps once, polls once, then hands off. No infinite replay loop or
    # mutable cross-run database cursor is needed. Absurd releases sleeping tasks.
    await ctx.sleep_until("poll-after", previous + dt.timedelta(seconds=INTERVAL_SECONDS))
    until_text = await ctx.step("window-until", lambda: iso(utcnow()))
    until = timestamp(until_text)
    if until is None or until < previous:
        raise ValueError("invalid feed window")
    since = max(seed, previous - dt.timedelta(seconds=OVERLAP_SECONDS),
                until - dt.timedelta(seconds=LOOKBACK_SECONDS))
    events = await ctx.step("github-events", lambda: collect_events(GitHub(), config, since, until))
    for event in events:
        key = event_key(event)
        await ctx.step(
            "start:" + key,
            lambda event=event, key=key: ctx.start_workflow(
                "omp_release_feed_event", {"event": event, "generation": current}, idempotency_key=key,
            ),
        )
    await ctx.step(
        "continue",
        lambda: ctx.start_workflow(
            WORKFLOW_NAME,
            {"seeded_at": inp.seeded_at, "since": until_text, "generation": current},
            idempotency_key=f"omp-feed:poll:{current}:{until_text}",
        ),
    )
    return {"state": "polled", "since": iso(since), "until": until_text, "events": len(events)}
