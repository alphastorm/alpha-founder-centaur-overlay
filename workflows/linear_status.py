"""One scoped Linear comment/state observation and existing Centaur Slack transport."""

from __future__ import annotations

import asyncio
import re
import uuid

from linear_snapshot import GraphQL, SourceError, check_workspace, string, validate_scope_input

WORKFLOW_NAME = "carrythrough_linear_status"
WORKFLOW_PRINCIPAL = "carrythrough-linear"
REQUEST_SCHEMA = "carrythrough.linear-status-request.v1"
RESULT_SCHEMA = "carrythrough.linear-status-result.v1"
SCOPE_QUERY = "query CarrythroughStatusScope($id: String!) { organization { id urlKey } issue(id: $id) { id team { id } project { id } } }"
COMMENT = "mutation CarrythroughStatus($input: CommentCreateInput!) { commentCreate(input: $input) { success comment { id } } }"
STATE = "mutation CarrythroughState($id: String!, $input: IssueUpdateInput!) { issueUpdate(id: $id, input: $input) { success issue { id state { id } } } }"


class Input(dict):
    @classmethod
    def parse(cls, raw):
        fields = {"schema_version", "request_id", "workspace_id", "workspace_slug", "allowed_team_ids", "allowed_project_ids", "issue_id", "work_item_id", "event", "state", "pr_url", "slack_channel", "slack_thread_ts", "target_state_id"}
        if not isinstance(raw, dict) or set(raw) != fields or raw["schema_version"] != REQUEST_SCHEMA:
            raise ValueError("unsupported Linear status request")
        inp = cls(raw)
        validate_scope_input(inp)
        string(inp["issue_id"], 64, r"[A-Za-z0-9_-]+")
        string(inp["work_item_id"], 26, r"[0-9A-HJKMNP-TV-Z]{26}")
        if inp["event"] not in {"draft_pr_opened", "merged", "closed"} or inp["state"] != inp["event"]:
            raise ValueError("status must describe the exact observed event")
        if inp["request_id"] != f"linear-status:{inp['work_item_id']}:{inp['event']}":
            raise ValueError("status effect identity mismatch")
        string(inp["pr_url"], 512, r"https://github[.]com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pull/[1-9][0-9]*")
        if inp["target_state_id"] is not None:
            string(inp["target_state_id"], 64, r"[A-Za-z0-9_-]+")
            if inp["event"] == "draft_pr_opened":
                raise ValueError("draft PR creation cannot advance the Linear state")
        if inp["slack_channel"] is not None:
            string(inp["slack_channel"], 32, r"[CG][A-Z0-9]{8,31}")
        if inp["slack_thread_ts"] is not None:
            if inp["slack_channel"] is None or not re.fullmatch(r"[0-9]{10,16}\.[0-9]{6}", inp["slack_thread_ts"]):
                raise ValueError("Slack thread must belong to a selected channel")
        return inp


def scope(inp, client):
    data = client.query(SCOPE_QUERY, {"id": inp["issue_id"]})
    check_workspace(data, inp)
    issue = data.get("issue")
    if not isinstance(issue, dict) or issue.get("id") != inp["issue_id"]:
        raise SourceError("not_found")
    team, project = issue.get("team"), issue.get("project")
    if not isinstance(team, dict) or team.get("id") not in inp["allowed_team_ids"] or (inp["allowed_project_ids"] and (not isinstance(project, dict) or project.get("id") not in inp["allowed_project_ids"])):
        raise SourceError("out_of_scope")


def post_linear(inp, client):
    """Catch uncertainty inside the checkpointed step: never throw a write for retry."""
    try:
        if inp["target_state_id"] is not None:
            response = client.query(STATE, {"id": inp["issue_id"], "input": {"stateId": inp["target_state_id"]}})
            update = response.get("issueUpdate", {})
            if update.get("success") is not True or update.get("issue", {}).get("id") != inp["issue_id"] or update.get("issue", {}).get("state", {}).get("id") != inp["target_state_id"]:
                return {"state": "pending", "error": SourceError("invalid_result").result()}
        else:
            body = f"Carrythrough WorkItem {inp['work_item_id']}: {inp['event']}.\nPR: {inp['pr_url']}\n\nEffect: {inp['request_id']}"
            response = client.query(COMMENT, {"input": {"issueId": inp["issue_id"], "body": body}})
            comment = response.get("commentCreate", {})
            if comment.get("success") is not True or not isinstance(comment.get("comment", {}).get("id"), str):
                return {"state": "pending", "error": SourceError("invalid_result").result()}
        return {"state": "delivered", "error": None}
    except Exception:
        # Failure after bytes left the process is not proof that no comment/state changed.
        return {"state": "pending", "error": SourceError("unavailable").result()}


async def handler(inp, ctx):
    inp = Input.parse(inp)
    result = {"schema_version": RESULT_SCHEMA, "request_id": inp["request_id"],
              "linear_state": "failed", "slack_state": "failed" if inp["slack_channel"] else "not_requested", "error": None}
    try:
        client = GraphQL(262144)
        await asyncio.to_thread(scope, inp, client)
    except SourceError as error:
        result["error"] = error.result()
        return result
    linear = await ctx.step("linear-status", lambda: asyncio.to_thread(post_linear, inp, client))
    result["linear_state"], result["error"] = linear["state"], linear["error"]
    if inp["slack_channel"] is not None:
        async def post_slack():
            try:
                kwargs = {"client_msg_id": str(uuid.uuid5(uuid.NAMESPACE_URL, inp["request_id"])),
                          "unfurl_links": False, "unfurl_media": False}
                if inp["slack_thread_ts"]:
                    kwargs["thread_ts"] = inp["slack_thread_ts"]
                response = await ctx.post_to_slack(inp["slack_channel"],
                    f"Carrythrough WorkItem `{inp['work_item_id']}`: {inp['event']}. {inp['pr_url']}", **kwargs)
                if not isinstance(response, dict) or response.get("ok") is not True or response.get("channel") != inp["slack_channel"] or not response.get("ts"):
                    return "pending"
                return "delivered"
            except Exception:
                return "pending"
        result["slack_state"] = await ctx.step("slack-status", post_slack)
    return result
