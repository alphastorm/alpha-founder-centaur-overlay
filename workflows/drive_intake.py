"""Bounded Drive discovery and same-execution byte handoff to Alpha Founder."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import http.client
import json
import re
import tempfile
import time
from typing import Any
from urllib.parse import urlsplit

from workflows.gsuite.drive import GoogleDriveReadonlyClient
from workflows.gsuite.http import build_http

WORKFLOW_NAME = "carrythrough_drive_intake"
# A string selects an EXISTING foreign id in .144; True would create a different id.
WORKFLOW_PRINCIPAL = "alpha-founder-drive-intake"
REQUEST_SCHEMA = "carrythrough.drive-intake-request.v1"
SUPPORTED_REQUEST_SCHEMAS = (REQUEST_SCHEMA, "alpha-founder.drive-intake-request.v1")
RESULT_SCHEMA = "carrythrough.drive-intake-result.v1"
DRIVE_ID = re.compile(r"[A-Za-z0-9_-]{10,128}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
CAPABILITY = re.compile(r"[A-Za-z0-9_-]{43}\Z")
FOLDER = "application/vnd.google-apps.folder"
SHORTCUT = "application/vnd.google-apps.shortcut"
ZIP_TYPES = {"application/zip", "application/x-zip-compressed", "multipart/x-zip"}
EXPORTS = {
    "application/vnd.google-apps.document": (
        "docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ),
    "application/vnd.google-apps.spreadsheet": (
        "xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ),
    "application/vnd.google-apps.presentation": ("pdf", "application/pdf"),
}
EXPORT_MAX_BYTES = 10_000_000
IO_TIMEOUT_SECONDS = 60
IO_ATTEMPTS = 3
CHUNK_BYTES = 256 * 1024
PAGE_SIZE = 100
FILE_FIELDS = (
    "id,name,mimeType,size,md5Checksum,version,modifiedTime,webViewLink,"
    "trashed,capabilities(canDownload),shortcutDetails(targetId,targetMimeType)"
)
LIMITS = {
    "max_file_bytes": (512 * 1024 * 1024, 1, 2 * 1024 * 1024 * 1024),
    "max_files": (500, 1, 10_000),
    "max_total_bytes": (2 * 1024 * 1024 * 1024, 1, 16 * 1024 * 1024 * 1024),
    "max_depth": (6, 0, 16),
    "max_pages": (50, 1, 1_000),
}


def _object(value: Any, allowed: set[str], required: set[str], field: str) -> dict:
    if not isinstance(value, dict) or set(value) - allowed or required - set(value):
        raise ValueError(f"{field} must have exactly the supported fields")
    return value


def _string(value: Any, field: str, minimum: int, maximum: int, pattern=None) -> str:
    if (not isinstance(value, str) or not minimum <= len(value) <= maximum
            or (pattern is not None and pattern.fullmatch(value) is None)):
        raise ValueError(f"{field} is invalid")
    return value


class Input(dict):
    """Strict stdlib parser; not a dataclass (host coercion drops unknown keys).

    The host passes non-dataclass Input values through, so handler validates the
    original JSON instead of accepting a host-coerced, potentially lossy value.
    """

    @classmethod
    def parse(cls, raw: Any) -> Input:
        required = {
            "request_id", "binding_id", "root_file_id", "root_kind", "intake_shape",
            "limits", "upload_url", "upload_token",
        }
        data = dict(_object(raw, required | {"schema_version", "acquire", "known"},
                            required, "input"))
        if data.get("schema_version", REQUEST_SCHEMA) not in SUPPORTED_REQUEST_SCHEMAS:
            raise ValueError("schema_version is invalid")
        data["schema_version"] = REQUEST_SCHEMA
        _string(data["request_id"], "request_id", 8, 128)
        _string(data["binding_id"], "binding_id", 1, 64)
        _string(data["root_file_id"], "root_file_id", 10, 128, DRIVE_ID)
        shape = data["intake_shape"]
        if shape not in ("zip_file", "zip_drops", "company_folder"):
            raise ValueError("intake_shape is invalid")
        if data["root_kind"] != ("file" if shape == "zip_file" else "folder"):
            raise ValueError("root_kind does not match intake_shape")
        acquire = data.get("acquire", True)
        if type(acquire) is not bool:
            raise ValueError("acquire must be a boolean")
        data["acquire"] = acquire
        limits = _object(data["limits"], set(LIMITS), set(), "limits")
        data["limits"] = {}
        for name, (default, minimum, maximum) in LIMITS.items():
            value = limits.get(name, default)
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError(f"limits.{name} is invalid")
            data["limits"][name] = value
        known = data.get("known", {})
        if not isinstance(known, dict) or len(known) > 10_000:
            raise ValueError("known is invalid")
        data["known"] = {}
        for file_id, fingerprint in known.items():
            _string(file_id, "known file id", 10, 128, DRIVE_ID)
            fingerprint = _object(fingerprint, {"md5_checksum", "version", "sha256"},
                                  {"sha256"}, "known fingerprint")
            _string(fingerprint["sha256"], "known sha256", 64, 64, SHA256)
            for key in ("md5_checksum", "version"):
                if fingerprint.get(key) is not None:
                    _string(fingerprint[key], f"known {key}", 0, 64)
            data["known"][file_id] = dict(fingerprint)
        _string(data["upload_token"], "upload_token", 43, 43, CAPABILITY)
        url = _string(data["upload_url"], "upload_url", 1, 2048)
        try:
            parts = urlsplit(url)
            port = parts.port
        except ValueError:
            raise ValueError("upload_url is invalid") from None
        if (parts.scheme not in ("http", "https") or not parts.hostname
                or parts.username is not None or parts.password is not None
                or parts.query or parts.fragment or any(ord(c) <= 32 for c in url)
                or (port is not None and not 1 <= port <= 65535)):
            raise ValueError("upload_url must be an HTTP source-slot base URL")
        return cls(data)


def _status(exc: Exception) -> int | None:
    return getattr(getattr(exc, "resp", None), "status", None)


def _reasons(exc: Exception) -> set[str]:
    try:
        content = json.loads(getattr(exc, "content", b"{}"))
        error = content.get("error", {})
        if isinstance(error, str):
            return {error}
        return {str(item.get("reason", "")) for item in error.get("errors", [])}
    except (ValueError, TypeError, AttributeError):
        return set()


def _retry(fn):
    from googleapiclient.errors import HttpError

    for attempt in range(IO_ATTEMPTS):
        try:
            return fn()
        except (HttpError, OSError, http.client.HTTPException) as exc:
            status = _status(exc)
            retryable = status == 429 or (status is not None and 500 <= status <= 599)
            retryable = retryable or isinstance(exc, (OSError, http.client.HTTPException))
            if not retryable or attempt + 1 == IO_ATTEMPTS:
                raise
            time.sleep(0.25 * 2 ** attempt)


class DriveClient(GoogleDriveReadonlyClient):
    """The native read-only client with intake fields and bounded media I/O.

    .144 list_docs hardcodes ETL fields and creates an unbounded HTTP transport.
    This specialization retains its paginated API and Shared Drive semantics,
    using the same build_http credential boundary, with expanded fields/timeouts.
    """

    def __init__(self):
        self._service = None

    @property
    def service(self):
        if self._service is None:
            from googleapiclient.discovery import build

            transport = build_http()
            transport.timeout = IO_TIMEOUT_SECONDS
            self._service = build("drive", "v3", http=transport, cache_discovery=False)
        return self._service

    def get(self, file_id: str) -> dict:
        return _retry(lambda: self.service.files().get(
            fileId=file_id, fields=FILE_FIELDS, supportsAllDrives=True
        ).execute(num_retries=0))

    def list_docs(self, *, query: str, page_size: int, page_token=None) -> dict:
        kwargs = {
            "q": query, "pageSize": page_size, "fields": f"nextPageToken,files({FILE_FIELDS})",
            "includeItemsFromAllDrives": True, "supportsAllDrives": True,
            "orderBy": "name",
        }
        if page_token:
            kwargs["pageToken"] = page_token
        return _retry(lambda: self.service.files().list(**kwargs).execute(num_retries=0))

    def download(self, item: dict, sink, export: tuple[str, str] | None) -> None:
        from googleapiclient.http import MediaIoBaseDownload

        if export:
            request = self.service.files().export_media(fileId=item["file_id"], mimeType=export[1])
        else:
            request = self.service.files().get_media(fileId=item["file_id"], supportsAllDrives=True)
        downloader = MediaIoBaseDownload(sink, request, chunksize=CHUNK_BYTES)
        done = False
        while not done:
            _, done = _retry(lambda: downloader.next_chunk(num_retries=0))

    def close(self):
        if self._service is not None:
            self._service.close()


def _client() -> DriveClient:
    return DriveClient()


def _error(exc: Exception, *, root=False, file_id=None) -> dict:
    status, reasons = _status(exc), _reasons(exc)
    if status == 401 or "invalid_grant" in reasons:
        code, message = "invalid_grant", "Reconnect the Google account granted to the intake principal."
    elif status == 403:
        code, message = "access_denied", "Share the source with the intake Google account."
    elif root and status == 404:
        code, message = "root_not_found", "The configured Drive root was not found."
    elif status == 429 or (status is not None and 500 <= status <= 599):
        code, message = "rate_limited", "Drive requests failed after bounded retries; sync again later."
    else:
        code, message = "listing_failed", "Drive metadata could not be read; check the source and sync again."
    return {"code": code, "message": message, "file_id": file_id}


def _datetime(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            return parsed.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        pass
    return None


def _metadata(raw: dict) -> dict:
    file_id = _string(raw.get("id"), "Drive file id", 10, 128, DRIVE_ID)
    name = _string(raw.get("name"), "Drive name", 1, 1024)
    mime = _string(raw.get("mimeType"), "Drive MIME type", 1, 255)
    size = raw.get("size")
    if size is not None:
        size = int(size)
        if size < 0:
            raise ValueError("Drive size is invalid")
    result = {"file_id": file_id, "name": name, "mime_type": mime, "size": size}
    for source, target, maximum in (
        ("md5Checksum", "md5_checksum", 64), ("version", "version", 64),
        ("webViewLink", "web_view_link", 2048),
    ):
        value = raw.get(source)
        result[target] = _string(value, target, 1, maximum) if value else None
    result["modified_time"] = _datetime(raw.get("modifiedTime"))
    result["can_download"] = raw.get("capabilities", {}).get("canDownload") is not False
    result["trashed"] = raw.get("trashed") is True
    return result


def _component(name: str) -> str:
    value = "".join(f"%{ord(c):02X}" if c in "%/\\" or ord(c) < 32 else c for c in name)
    return value.replace(".", "%2E") if value in (".", "..") else value


def _materialized_component(meta: dict) -> str:
    name = _component(meta["name"])
    export = EXPORTS.get(meta["mime_type"])
    if export and not name.lower().endswith("." + export[0]):
        name += "." + export[0]
    return name


def _acquisition(status: str, *, size=None, sha256=None, export=None, message="") -> dict:
    return {"status": status, "bytes": size, "sha256": sha256,
            "export_format": export, "message": message}


def _is_zip(meta: dict) -> bool:
    return meta["mime_type"] in ZIP_TYPES or (
        not meta["mime_type"].startswith("application/vnd.google-apps.")
        and meta["name"].lower().endswith(".zip")
    )


def _item(meta: dict, path: str) -> dict:
    if len(path) > 4096:
        raise ValueError("Drive relative path exceeds the wire limit")
    return {key: value for key, value in meta.items() if key not in ("can_download", "trashed")} | {
        "path": path, "acquisition": _acquisition("not_requested"),
    }


def _packet(meta: dict, kind: str) -> dict:
    return {"source_item_id": meta["file_id"], "kind": kind, "name": meta["name"],
            "web_view_link": meta["web_view_link"], "modified_time": meta["modified_time"],
            "complete": True, "items": []}


def _incomplete(result: dict, reason: str, packet=None):
    result["complete"] = False
    if result["incomplete_reason"] is None:
        result["incomplete_reason"] = reason
    if packet is not None:
        packet["complete"] = False


def _add_error(result: dict, error: dict):
    if len(result["errors"]) < 100:
        result["errors"].append(error)


def _discover(inp: Input, client: DriveClient) -> dict:
    result = {
        "schema_version": RESULT_SCHEMA, "request_id": inp["request_id"],
        "observed_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "root": None, "complete": True, "incomplete_reason": None, "packets": [], "errors": [],
    }
    metadata = {}
    try:
        root = _metadata(client.get(inp["root_file_id"]))
        if root["file_id"] != inp["root_file_id"]:
            raise ValueError("Drive returned a different root")
        if root["trashed"]:
            result["complete"] = False
            _add_error(result, {"code": "root_not_found", "message": "The configured Drive root is trashed.",
                                "file_id": inp["root_file_id"]})
            return {"result": result, "metadata": metadata}
    except Exception as exc:
        result["complete"] = False
        _add_error(result, _error(exc, root=True, file_id=inp["root_file_id"]))
        return {"result": result, "metadata": metadata}
    kind = "folder" if root["mime_type"] == FOLDER else "file"
    result["root"] = {key: root[key] for key in ("file_id", "name", "mime_type", "web_view_link")} | {
        "kind": kind,
    }
    if kind != inp["root_kind"]:
        result["complete"] = False
        _add_error(result, {"code": "root_kind_mismatch", "message": "The configured root has the wrong kind.",
                            "file_id": root["file_id"]})
        return {"result": result, "metadata": metadata}
    if inp["intake_shape"] == "zip_file":
        packet = _packet(root, "zip")
        packet["items"].append(_item(root, _materialized_component(root)))
        result["packets"].append(packet)
        metadata[root["file_id"]] = root
        return {"result": result, "metadata": metadata}

    company_packet = _packet(root, "folder") if inp["intake_shape"] == "company_folder" else None
    if company_packet is not None:
        result["packets"].append(company_packet)
    seen = {root["file_id"]}
    paths = set()
    count = pages = 0
    limits = inp["limits"]

    def walk(folder_id: str, prefix: str, depth: int) -> bool:
        nonlocal count, pages
        token = None
        tokens = set()
        while True:
            if pages >= limits["max_pages"]:
                _incomplete(result, "max_pages", company_packet)
                return False
            try:
                page = client.list_docs(query=f"'{folder_id}' in parents and trashed = false",
                                        page_size=PAGE_SIZE, page_token=token)
                pages += 1
                if not isinstance(page.get("files", []), list):
                    raise ValueError("Drive listing is invalid")
                for raw in page.get("files", []):
                    meta = _metadata(raw)
                    if meta["trashed"] or meta["file_id"] in seen:
                        continue
                    if count >= limits["max_files"]:
                        _incomplete(result, "max_files", company_packet)
                        return False
                    count += 1
                    seen.add(meta["file_id"])
                    component = _materialized_component(meta)
                    path = prefix + component
                    while path in paths:
                        stem, dot, suffix = component.rpartition(".")
                        if dot and stem and suffix and meta["mime_type"] != FOLDER:
                            component = stem + "~" + meta["file_id"] + dot + suffix
                        else:
                            component += "~" + meta["file_id"]
                        path = prefix + component
                    paths.add(path)
                    if company_packet is not None and meta["mime_type"] == FOLDER:
                        if depth >= limits["max_depth"]:
                            _incomplete(result, "max_depth", company_packet)
                        elif not walk(meta["file_id"], path + "/", depth + 1):
                            return False
                        continue
                    packet = company_packet
                    if packet is None:
                        packet = _packet(meta, "folder" if meta["mime_type"] == FOLDER else "zip")
                        result["packets"].append(packet)
                    packet["items"].append(_item(meta, path))
                    metadata[meta["file_id"]] = meta
                next_token = page.get("nextPageToken")
                if not next_token:
                    return True
                if not isinstance(next_token, str) or next_token in tokens:
                    raise ValueError("Drive pagination did not advance")
                tokens.add(next_token)
                token = next_token
            except Exception as exc:
                result["complete"] = False
                if company_packet is not None:
                    company_packet["complete"] = False
                _add_error(result, _error(exc, file_id=folder_id))
                return False

    walk(root["file_id"], "", 0)
    return {"result": result, "metadata": metadata}


class ByteLimitExceeded(Exception):
    def __init__(self, reason: str):
        self.reason = reason


class BoundedWriter:
    def __init__(self, stream, maximum: int, reason: str):
        self.stream, self.maximum, self.reason = stream, maximum, reason
        self.bytes = 0
        self.sha256 = hashlib.sha256()

    def write(self, data: bytes):
        if self.bytes + len(data) > self.maximum:
            raise ByteLimitExceeded(self.reason)
        self.stream.write(data)
        self.sha256.update(data)
        self.bytes += len(data)
        return len(data)


class UploadFailure(Exception):
    pass


def _upload(inp: Input, file_id: str, stream, size: int, sha256: str):
    parts = urlsplit(inp["upload_url"])
    connection_cls = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    path = parts.path.rstrip("/") + "/" + file_id
    headers = {"Authorization": "Bearer " + inp["upload_token"], "Content-Length": str(size),
               "Content-Type": "application/octet-stream"}
    for attempt in range(IO_ATTEMPTS):
        connection = connection_cls(parts.hostname, parts.port, timeout=IO_TIMEOUT_SECONDS)
        try:
            stream.seek(0)
            # HTTPConnection streams the file, and does not follow redirects.
            connection.request("PUT", path, body=stream, headers=headers)
            response = connection.getresponse()
            payload = response.read(4097)
            if response.status == 429 or 500 <= response.status <= 599:
                if attempt + 1 < IO_ATTEMPTS:
                    time.sleep(0.25 * 2 ** attempt)
                    continue
                raise UploadFailure("Source upload failed after bounded retries.")
            if response.status != 200 or len(payload) > 4096:
                raise UploadFailure("Source upload was rejected; request a fresh sync.")
            try:
                receipt = json.loads(payload)
            except (ValueError, TypeError):
                raise UploadFailure("Source upload returned an invalid receipt.") from None
            if (not isinstance(receipt, dict) or receipt.get("sha256") != sha256
                    or type(receipt.get("bytes")) is not int or receipt["bytes"] != size):
                raise UploadFailure("Source upload receipt did not match the acquired bytes.")
            return
        except (OSError, http.client.HTTPException):
            if attempt + 1 == IO_ATTEMPTS:
                raise UploadFailure("Source upload failed after bounded retries.") from None
            time.sleep(0.25 * 2 ** attempt)
        finally:
            connection.close()


def _matches(meta: dict, known: dict) -> bool:
    # A rename can increment version without changing a binary's md5.
    if meta.get("md5_checksum") and known.get("md5_checksum"):
        return meta["md5_checksum"] == known["md5_checksum"]
    return bool(meta.get("version") and known.get("version") == meta["version"])


def _acquire(inp: Input, item: dict, meta: dict, client: DriveClient, remaining: int) -> dict:
    mime = item["mime_type"]
    export = EXPORTS.get(mime)
    fmt = export[0] if export else None

    def done(status, *, size=None, sha256=None, message="", bound=None, error=None):
        return {"acquisition": _acquisition(status, size=size, sha256=sha256, export=fmt, message=message),
                "bound": bound, "error": error}

    if mime == SHORTCUT:
        return done("shortcut", message="Shortcuts are inventoried, not followed.")
    if (mime == FOLDER or (mime.startswith("application/vnd.google-apps.") and not export)
            or (inp["intake_shape"] in ("zip_file", "zip_drops") and not _is_zip(meta))):
        return done("unsupported_type", message="This source type is not acquired by this binding.")
    if not meta["can_download"]:
        return done("download_disallowed", message="The source does not permit downloading.")
    if item["size"] is not None and item["size"] > inp["limits"]["max_file_bytes"]:
        return done("too_large", message="Source size exceeds max_file_bytes.")
    if not inp["acquire"]:
        return done("not_requested")
    known = inp["known"].get(item["file_id"])
    if known and _matches(meta, known):
        return done("unchanged", size=item["size"], sha256=known["sha256"])
    if remaining <= 0 or (item["size"] is not None and item["size"] > remaining):
        return done("too_large", message="Source exceeds the remaining max_total_bytes budget.",
                    bound="max_total_bytes")
    caps = [(inp["limits"]["max_file_bytes"], "too_large"), (remaining, "max_total_bytes")]
    if export:
        caps.append((EXPORT_MAX_BYTES, "export_too_large"))
    maximum, reason = min(caps, key=lambda cap: cap[0])
    try:
        with tempfile.TemporaryFile(mode="w+b") as stream:
            sink = BoundedWriter(stream, maximum, reason)
            client.download(item, sink, export)
            current = _metadata(client.get(item["file_id"]))
            changed = (
                current["file_id"] != item["file_id"] or current["trashed"]
                or current["mime_type"] != mime or not current["can_download"]
                or (meta.get("md5_checksum") and current.get("md5_checksum") != meta["md5_checksum"])
                or (not meta.get("md5_checksum") and meta.get("version")
                    and current.get("version") != meta["version"])
                or (not export and item["size"] is not None and sink.bytes != item["size"])
            )
            if changed:
                return done("failed", message="Source changed during acquisition; sync again.")
            digest = sink.sha256.hexdigest()
            _upload(inp, item["file_id"], stream, sink.bytes, digest)
            return done("uploaded", size=sink.bytes, sha256=digest)
    except ByteLimitExceeded as exc:
        bound = "max_total_bytes" if exc.reason == "max_total_bytes" else None
        status = "export_too_large" if exc.reason == "export_too_large" else "too_large"
        return done(status, message="Acquisition exceeded the configured byte limit.", bound=bound)
    except UploadFailure as exc:
        message = str(exc)
        return done("failed", message=message, error={"code": "upload_failed", "message": message,
                                                     "file_id": item["file_id"]})
    except Exception as exc:
        if export and "exportSizeLimitExceeded" in _reasons(exc):
            return done("export_too_large", message="Google export exceeds its 10 MB limit.")
        if _status(exc) == 403 and "invalid_grant" not in _reasons(exc):
            return done("download_disallowed", message="Google refused downloading this source.")
        error = _error(exc, file_id=item["file_id"])
        return done("failed", message=error["message"], error=error)


async def handler(inp, ctx) -> dict:
    inp = Input.parse(inp)
    client = _client()
    try:
        discovery = await ctx.step("discover", lambda: asyncio.to_thread(_discover, inp, client))
        result = discovery["result"]
        total = 0
        revoked = False
        for packet in result["packets"]:
            for item in packet["items"]:
                if revoked:
                    item["acquisition"] = _acquisition("failed", message="Reconnect the Google account.")
                    continue
                outcome = await ctx.step(
                    "acquire:" + item["file_id"],
                    lambda item=item: asyncio.to_thread(
                        _acquire, inp, item, discovery["metadata"][item["file_id"]], client,
                        inp["limits"]["max_total_bytes"] - total,
                    ),
                )
                item["acquisition"] = outcome["acquisition"]
                if outcome["acquisition"]["status"] == "uploaded":
                    total += outcome["acquisition"]["bytes"]
                if outcome["bound"]:
                    _incomplete(result, outcome["bound"], packet)
                if outcome["error"]:
                    _add_error(result, outcome["error"])
                    if outcome["error"]["code"] == "invalid_grant":
                        revoked = True
                        result["complete"] = packet["complete"] = False
        return result
    finally:
        client.close()
