# Alpha Founder Centaur Overlay

This public repository contains the non-secret organization overlay for Alpha Founder: Gate B qualification assets, deterministic investor-source intake, and read-only OMP repository companions with a retained-host status feed.

It contains:

- one read-only local attestation tool;
- a local qualification workflow and a bounded Drive intake workflow;
- one Claude Code skill;
- one sandbox prompt extension, one prompt fragment, one persona, and one inert sandbox marker file;
- two discoverable OMP repository personas and a retained-host-only GitHub-to-Slack feed;
- deterministic local tests.

The overlay contains no Alpha Founder product data, provider credentials, GitHub credentials or secrets. Qualification and deployment pin an immutable commit SHA; `main` is never a runtime selector. The qualification assets remain local and non-mutating. Drive intake reads a selected Google source and uploads acquired bytes only to the supplied Alpha Founder source-slot capability.

## Studio Drive intake

`workflows/drive_intake.py` exports `alpha_founder_drive_intake` for Centaur **0.1.144 (`acf52068`)**. Alpha Founder supplies the `alpha-founder.drive-intake-request.v1` JSON contract and receives `alpha-founder.drive-intake-result.v1`. Generated request/result schemas in `tests/schemas/` were exported from Alpha Founder commit **`163eed54`** using `model_json_schema()`; Alpha Founder remains the owner of the wire contract. No Pydantic dependency is needed in the workflow host.

- `zip_file`: one explicitly selected ZIP is one packet.
- `zip_drops`: each direct child is inventoried; ZIPs are acquired, non-ZIP children are `unsupported_type`, and shortcuts are reported without following them. No recursive drop-folder scan occurs.
- `company_folder`: descendants form one packet, with safe relative paths. Folders are traversed, not downloaded; binary sources retain their bytes. Google Docs export as DOCX, Sheets as XLSX (not one-tab CSV), and Slides as PDF. Acquired paths append the export suffix while original Drive names/MIME types remain unchanged. Materialized-name collisions insert `~<file_id>` before the suffix, preserving downstream file-type detection. Unsupported Google-native types, download restrictions and the Google 10 MB export ceiling remain visible item statuses.

Discovery exhausts `nextPageToken` with Shared Drive flags and `trashed=false`. Limits bound file objects (including traversed child folders), global listing pages, folder depth (root = 0) and newly acquired bytes. Reaching a limit only marks a scan incomplete when work remains; the first global bound is the result's `incomplete_reason`. Per-file limits produce `too_large` rather than an invented global reason. `complete` is not a claim of readable evidence: every item retains its separate acquisition status, and preparation/coverage belong to alpha-diligence. Duplicate Drive IDs are observed once; renames preserve IDs. Matching binary MD5 (preferred over a rename-only version change), or a matching version when no comparable MD5 exists, reuses the supplied `known` SHA-256 without downloading. Unchanged files consume no new-upload byte budget.

### Principal and Google grant

Create an existing Centaur principal with foreign ID **`alpha-founder-drive-intake`**. The workflow declares `WORKFLOW_PRINCIPAL = "alpha-founder-drive-intake"`; in .144 a string resolves an existing foreign ID/OID and an unknown reference fails startup. `True` would instead register `workflow-alpha-founder-drive-intake`, which is not this workflow's identity. Workflow-host sandboxing must be enabled (`WORKFLOW_HOST_SANDBOX=true`).

Configure one Console-managed Google OAuth credential with read-only Drive scope (`https://www.googleapis.com/auth/drive.readonly`) and grant its proxy-injection wrapper **only to this principal**, never the default/all-channel principal or investor readers. Share each selected root with that credential's Google account. A configured binding restricts traversal, not the account-wide OAuth scope. Iron-proxy performs bearer injection and token refresh; the workflow neither reads nor accepts a Google token, and does not fall back to a requester's identity. Root 401/invalid-grant, 403 and 404 become `invalid_grant`, `access_denied` and `root_not_found` with actionable messages. Revocation stops further acquisition in that run.

The host must mount both workflow trees:

```text
KUBERNETES_WORKFLOW_DIRS=/home/agent/github/paradigmxyz/centaur/workflows:/home/agent/github/carrythroughsystems/alpha-founder-centaur-overlay/workflows
```

Centaur's workflow host inserts each tree and its parent into Python's import paths, so `workflows.gsuite.drive.GoogleDriveReadonlyClient` and `workflows.gsuite.http.build_http` resolve from Centaur. The intake subclass expands .144's ETL-only listing fields and applies explicit HTTP timeouts; it does not copy OAuth/refresh logic. The .144 sandbox Dockerfile provides google-api-python-client, httplib2 and PySocks. The actual deployed image, grant and folder sharing still require deployment proof.

### Upload capability and recovery

The request's `upload_url` is an HTTP(S) base such as `http://alpha-founder-patch-intake.alpha-founder.svc:8091/v1/source`, and `upload_token` is a 43-character one-use source-slot capability, **not a Google credential**. Permit workflow-run pods to reach this intake through the separately enabled Drive-intake NetworkPolicy. The workflow sends streaming `PUT <upload_url>/<file_id>` with `Authorization: Bearer <upload_token>` and checks the intake's `{sha256, bytes}` receipt. It never returns the capability or a sandbox-local filename. HTTP redirects are not followed.

Downloads use MediaIoBaseDownload into an automatically removed temporary file with an enforced byte ceiling and running SHA-256; metadata is rechecked before upload to detect source changes. Google and upload I/O use 60-second socket timeouts and at most three attempts for transient errors. .144 ignores `ctx.step` retry/timeout arguments, so none are relied on here. Discovery and completed per-file acquisitions are checkpointed. A crash after an upload but before checkpoint completion may repeat the PUT: identical bytes converge on the existing source slot; changed bytes are rejected, not overwritten. A new sync is required for recorded failures. This workflow performs no company creation, diligence execution or paid run; Alpha Founder owns those decisions.

### Provider-free tests

Tests import the native helpers from `~/.cache/centaur-144-src` (override `CENTAUR_144_SOURCE` for another explicit checkout), exercising real Google request construction and MediaIoBaseDownload against a fake HTTP transport. An in-process fake intake implements streaming, receipts and idempotent slots; tests open no sockets and make no Google/provider calls. Every emitted result is checked against the vendored JSON schema. Run the complete overlay suite:

```sh
uv run --no-project --with pytest --with jsonschema --with google-api-python-client --with httplib2 --with pysocks pytest -q
```

## Retained-host OMP channels (Centaur `ae9dfdb8`)

`tools/personas/omp_monorepo/` and `tools/personas/omp_session_gateway/` each contain `PROMPT.md` and a `pyproject.toml` with `[tool.centaur] type = "persona"`. At `ae9dfdb8`, `tool_discovery.rs` scans children and grandchildren of each tools source, uses the directory basename as the persona ID, and reads `prompt_file`. These IDs satisfy SlackbotV2's `[A-Za-z0-9][A-Za-z0-9._-]*` pattern. `PersonaRegistry` makes the selected prompt available as `/home/agent/AGENTS_PERSONA.md`; the public tools-source visibility makes both personas discoverable even with public repo-cache access. This matches the `centaur-founder-kit` packaging precedent, not the older standalone `personas/qualification-reviewer.md` file.

### Separate retained-host mount

The name `retained-host/workflows/` is a deployment boundary, not a Centaur convention: only the retained host mounts it. Keep Studio intake in the existing `workflows/` tree. **Do not mount that existing overlay tree at this new ref on the retained host**: with `WORKFLOW_ENABLE_MODE=all`, `drive_intake.py` declares the Studio-only `alpha-founder-drive-intake` principal, and an unknown principal fails workflow startup.

Create the feed principal before loading the new workflows. Set these exact api-rs environment values, preserving Centaur's native workflow tree:

```text
WORKFLOW_HOST_SANDBOX=true
WORKFLOW_ENABLE_MODE=all
WORKFLOW_DIRS=/var/lib/centaur/repos/paradigmxyz/centaur/workflows:/var/lib/centaur/repos/carrythroughsystems/alpha-founder-centaur-overlay/retained-host/workflows
KUBERNETES_WORKFLOW_DIRS=/home/agent/github/paradigmxyz/centaur/workflows:/home/agent/github/carrythroughsystems/alpha-founder-centaur-overlay/retained-host/workflows
```

Pin the overlay to the integrated immutable commit (`OVERLAY_REF` below); never use a branch at runtime. Replace the existing tools extra source, rather than adding a second copy of the same personas:

```text
KUBERNETES_TOOLS_EXTRA_SOURCES=[{"ref":"<OVERLAY_REF>","repo":"carrythroughsystems/alpha-founder-centaur-overlay","subdir":"tools","visibility":"public"}]
```

The retained-host repo cache and sandbox mount must contain **that same ref**, including `retained-host/workflows/`; an old tools pin or mixed cache checkout is not a deployment of this feed. This change does not deploy or alter the Studio intake mount.

### Channel configuration

Edit the one non-secret mapping in `retained-host/workflows/omp_channels.json` before pinning the configured commit. Set `channels["carrythroughsystems/omp-monorepo"]` and `channels["carrythroughsystems/omp-session-gateway"]` to actual `C…`/`G…` conversation IDs. The supplied `<…>` placeholders, blank/name values, and explicit placeholder/example markers are inactive: unmapped repositories are neither read nor posted, and an entirely inactive mapping performs no GitHub requests or child starts. The feed's App and release-bot logins are configurable there; `founder_login` allows the founder's workflow-dispatch/schedule runs to be recognized, not to authorize any action. Defaults are `carrythroughsystems[bot]`, `alphastorm-release`, and `alphastorm`.

Set chart value `slackbotv2.channelDefaults` to this JSON object shape after replacing both key placeholders with the same real IDs (the chart renders `SLACKBOTV2_CHANNEL_DEFAULTS`):

```json
{
  "<OMP_MONOREPO_CHANNEL_ID>": {"persona": "omp_monorepo"},
  "<OMP_SESSION_GATEWAY_CHANNEL_ID>": {"persona": "omp_session_gateway"}
}
```

Channel defaults apply when a session is created, not retroactively to existing threads. Do not add buttons, release dispatches or approval endpoints to Slack. Have the stock bot join both channels through the normal operator setup; `chat:write` without `chat:write.public` does not establish membership in an arbitrary channel.

### Principals and read-only grants

Use Console admin forms; none of these steps need a release-bot write token in Centaur:

1. Identify/precreate the two conversation principals at **`/console/principals/new`**. Normal SlackbotV2 thread keys `slack:CHANNEL:THREAD` derive foreign IDs `slack-channel-<lowercase-channel-id>`. If the actual thread key includes a team (`slack:TEAM:CHANNEL:THREAD`), the verified alternate is `slack-channel-<lowercase-team-id>-<lowercase-channel-id>`. Match the actual identity; do not create a persona-named principal. Display names can be `#omp-monorepo` and `#omp-session-gateway`.
2. Create a third principal with foreign ID **`omp-release-feed`** before changing the mounts. Both `omp_release_feed` and `omp_release_feed_event` declare `WORKFLOW_PRINCIPAL = "omp-release-feed"`. A named principal lets both workflows share one explicit identity and grant. A string resolves an existing foreign ID/OID at `ae9dfdb8`; `True` would auto-register separate `workflow-<slugged-workflow-name>` identities instead.
3. Create **one** fine-grained GitHub PAT with resource owner `carrythroughsystems`, covering **both** `carrythroughsystems/omp-monorepo` and `carrythroughsystems/omp-session-gateway`. Grant **read-only** Metadata, Contents, Issues, Pull requests and Actions; no writes, workflow dispatch, administration or release-signing authority. The founder chose to share this read-only credential with the two channel principals and the feed principal. It need not belong to the gateway release machine account.
4. At **`/console/secrets/static/new`**, create **one** static secret for that PAT, with kind **`github_token`**, Replace mode, proxy value **`GITHUB_TOKEN`**, match headers **`Authorization`**, and no body/path/query matching or required-match flag. Store the actual PAT using the **Control plane** secret source (`source_type=control_plane`), not in this public repository. The profile supplies canonical `require: false` and rules for `api.github.com`, `github.com` and `api.githubcopilot.com`, with empty method/path filters. Read-only enforcement therefore comes from the PAT's repository permissions, not from the persona or HTTP-rule filters.
5. Open each of the **three** `/console/principals/<principal-oid>` pages and grant the **same** static secret (`POST /console/principals/<oid>/grants`, form `grantable=static:<secret-oid>`). Inspect inherited roles/grants and requester-principal credentials too: remove conflicting or write-capable GitHub grants. Disable channel sandbox workflow-write capability; Slack must not start a release/order as a workaround. Do not grant this secret to the default/all-channel role. Keep release-bot and App write credentials outside all Slack/conversation/requester and feed principals.

`args.rs` injects `GITHUB_TOKEN` as a placeholder into both session and workflow-host sandbox environments; the `github_token` profile replaces the Authorization placeholder for the principal's granted secret, preserving Bearer or Git HTTPS Basic authentication. The personas use `GH_TOKEN="$GITHUB_TOKEN"` for `gh`; the verified sandbox Dockerfile installs both `git` and `gh`. Do not confuse the tools/repo-cache fetch credential (`KUBERNETES_TOOLS_GITHUB_TOKEN_SECRET`) with a channel/feed grant, and do not mount a release token into those sandboxes.

The feed reads GitHub with stdlib `urllib` GETs through the injected proxy token. It calls `ctx.post_to_slack` rather than a Slack tool: at `ae9dfdb8`, api-rs performs `chat.postMessage` with its existing `SLACK_BOT_TOKEN`. **No sandbox Slack-token grant or Slack write credential is needed for any of these three principals.** The native RPC uses the server token, so the Python feed's fixed mapping and author filters, not a per-principal Slack grant, constrain its destination/content.

### Feed behavior and recovery bounds

- Every five-minute `SCHEDULE` run independently polls `[now - 30 minutes, now]`, starts one `omp_release_feed_event` child per event with its stable idempotency key, and returns. Native `ctx.step` checkpoints replay within that tick; there is no seed, generation, continuation chain, sleep or cross-run cursor.
- The six-interval overlap lets the next scheduled tick recover from failed or skipped ticks while child keys deduplicate previously started events. First deployment may post up to 30 minutes of history. Events older than the lookback are never posted, and outages beyond it are not backfilled. Channel/bot configuration is read afresh each tick and delivery. The feed is a bounded status summary, not a lossless event ledger.
- Event keys contain repository, kind, GitHub ID and state; changed titles do not resend an event. The event child checkpoints the post and supplies a deterministic UUID `client_msg_id`. Key propagation and checkpoint replay are exercised locally with fakes and a native API import proof, not a live Absurd database. Exactly-once delivery across a Slack-accepted/post-checkpoint crash still depends on Slack's client-ID behavior and is not claimed as live-proven.
- Sources are exact-title tracking issues authored by `github-actions[bot]`; opened/merged/closed PRs **authored** by the App Bot or release account in the same head repository; release-bot driver comments on those tracking issues; release-bot gateway releases (the newest page only); failed gateway `Upstream OMP canary`/`Keyless release` runs (`signed-release.yml`); all completed monorepo `Release worker` runs; and failed monorepo `Provider-free contracts` runs on `main`. Run actors must be the configured founder/App/release account or `github-actions[bot]`. Failure includes `failure`, `timed_out`, `startup_failure` and `action_required`; cancelled runs are reported only for `Release worker`.
- Everything fetched from GitHub is untrusted. Author filters run before external text is used. Only the driver's first line is relayed, with no later body links; issue/PR/release bodies and third-party comments never become events. Other messages use escaped, capped summaries and locally constructed GitHub links. `&`, `<` and `>` are escaped; messages are capped at 1,500 characters and unfurls are off. Requests refuse redirects, have a 30-second timeout and a 2 MiB response bound. At most 100 events and ten 100-row pages per endpoint are accepted; exceeding a bound fails that tick before event delivery, and the next tick polls independently.
- Actions candidates cover the preceding two days by creation time, while completion eligibility uses the poll's `updated_at` interval. Two days covers every run these workflows make (the Studio release worker, the longest, runs hours) and keeps each repository under the page bound: on 2026-10-08 the gateway had 1,281 completed runs in 35 days, which failed every tick of the first deployment.

### Local proof and remaining deployment prerequisites

`tests/test_omp_release_feed.py` exercises fake ctx checkpoints/starts/posts and fake GitHub REST responses, stable keys, all sources, author/type filtering, escaping/caps, placeholder silence, overlapping-tick deduplication, recovery after a failed tick, lookback boundaries and persona packaging. Run the existing complete overlay suite once after changes:

```sh
uv run --no-project --with pytest --with jsonschema --with google-api-python-client --with httplib2 --with pysocks pytest -q
```

The legacy intake tests still use `CENTAUR_144_SOURCE`; feed tests can select an extracted native API via `CENTAUR_WORKFLOW_SOURCE=<temp>/services/workflow-python`. For the separate real-API import proof, extract `git archive ae9dfdb8 services/workflow-python` from the Centaur checkout into a temporary directory, put its `services/workflow-python` and this overlay's `retained-host/workflows` on `sys.path`, then use native `workflow_host.discover_workflows()` against only the retained-host tree. No live workflow/campaign, GitHub write, or Slack/Centaur call is part of these checks.

Actual channel IDs, membership, principal/grant provisioning, read-only PAT access, the retained-host image/proxy egress, cache pinning, and Slack delivery remain operator prerequisites. The workflow display names and permitted actors come from the pipeline contract, not Centaur's source. GitHub listing captures current issue/PR state, not every transient close/reopen between polls. Native source confirms discovery, placeholders, child idempotency propagation and Slack posting APIs; it does not prove this retained host has been deployed or that Slack provides crash-window deduplication.

No license is granted for this repository's contents.
