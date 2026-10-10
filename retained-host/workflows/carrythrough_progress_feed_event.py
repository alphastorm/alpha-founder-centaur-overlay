"""One actionable condition revision, one checkpointed native Slack post."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import uuid

from api.workflow_engine import WorkflowContext
from _omp_feed import load_config, MONOREPO
from carrythrough_progress_feed import event_key

WORKFLOW_NAME = "carrythrough_progress_feed_event"
WORKFLOW_PRINCIPAL = "omp-release-feed"


@dataclass
class Input:
    condition: dict[str, Any] = field(default_factory=dict)


async def handler(inp: Input, ctx: WorkflowContext) -> dict[str, Any]:
    channel = load_config()["channels"].get(MONOREPO)
    if not channel:
        return {"state": "unconfigured"}
    key = event_key(inp.condition)
    text = "Carrythrough: " + inp.condition["message"]
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")[:1500]
    await ctx.step(
        "post",
        lambda: ctx.post_to_slack(
            channel, text, client_msg_id=str(uuid.uuid5(uuid.NAMESPACE_URL, key)),
            unfurl_links=False, unfurl_media=False,
        ),
    )
    return {"state": "posted", "condition_key": inp.condition["key"]}
