# Alpha Founder Centaur Overlay

This public repository contains the non-secret organization overlay for Alpha Founder: the original Gate B qualification assets and a deterministic investor-source intake workflow.

It contains:

- one read-only local attestation tool;
- a local qualification workflow and a bounded Drive intake workflow;
- one Claude Code skill;
- one sandbox prompt extension, one prompt fragment, one persona, and one inert sandbox marker file;
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
KUBERNETES_WORKFLOW_DIRS=/home/agent/github/paradigmxyz/centaur/workflows:/home/agent/github/alphastorm/alpha-founder-centaur-overlay/workflows
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

No license is granted for this repository's contents.
