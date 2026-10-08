"""One read-only pipeline event, one checkpointed stock Centaur Slack post."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import uuid

from api.workflow_engine import WorkflowContext
from _omp_feed import event_key, generation, load_config, message

WORKFLOW_NAME = "omp_release_feed_event"
WORKFLOW_PRINCIPAL = "omp-release-feed"


@dataclass
class Input:
    event: dict[str, Any] = field(default_factory=dict)
    generation: str = ""


async def handler(inp: Input, ctx: WorkflowContext) -> dict[str, Any]:
    config = load_config()
    if inp.generation != generation(config):
        return {"state": "retired"}
    text = message(inp.event, config)
    if text is None:
        return {"state": "ignored"}
    channel_id = config["channels"][inp.event["repo"]]
    client_msg_id = str(uuid.uuid5(uuid.NAMESPACE_URL, event_key(inp.event)))
    await ctx.step(
        "post",
        lambda: ctx.post_to_slack(
            channel_id, text, client_msg_id=client_msg_id, unfurl_links=False, unfurl_media=False,
        ),
    )
    return {"state": "posted", "event_key": event_key(inp.event)}
