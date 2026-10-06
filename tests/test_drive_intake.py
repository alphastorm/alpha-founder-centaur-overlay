from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import parse_qs, unquote, urlsplit

import httplib2
from jsonschema import Draft202012Validator, FormatChecker
import pytest

ROOT = Path(__file__).resolve().parents[1]
CENTAUR = Path(os.environ.get("CENTAUR_144_SOURCE", "~/.cache/centaur-144-src")).expanduser()
# Mirror .144 configure_workflow_import_paths, including the overlay-first order.
for directory in (CENTAUR / "workflows", ROOT / "workflows"):
    for path in (directory.parent, directory, directory.parent / "services" / "api"):
        if path.is_dir() and str(path) not in sys.path:
            sys.path.insert(0, str(path))
SPEC = importlib.util.spec_from_file_location("overlay_drive_intake", ROOT / "workflows" / "drive_intake.py")
assert SPEC is not None and SPEC.loader is not None
intake = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = intake
SPEC.loader.exec_module(intake)

SCHEMAS = ROOT / "tests" / "schemas"
REQUEST_SCHEMA = json.loads((SCHEMAS / "drive_intake_request.json").read_text())
RESULT_SCHEMA = json.loads((SCHEMAS / "drive_intake_result.json").read_text())
REQUEST = Draft202012Validator(REQUEST_SCHEMA, format_checker=FormatChecker())
RESULT = Draft202012Validator(RESULT_SCHEMA, format_checker=FormatChecker())


def file_id(number: int) -> str:
    return f"drivefile{number:06d}"


def request(root: str, shape="company_folder", **changes) -> dict:
    result = {
        "schema_version": intake.REQUEST_SCHEMA, "request_id": "sync-request-0001",
        "binding_id": "source-binding", "root_file_id": root,
        "root_kind": "file" if shape == "zip_file" else "folder", "intake_shape": shape,
        "limits": {}, "upload_url": "http://intake.invalid:8091/v1/source",
        "upload_token": "C" * 43,
    }
    result.update(changes)
    REQUEST.validate(result)
    return result


class FakeDriveHttp:
    """Google transport, not a fake client: real discovery and media code run."""

    def __init__(self):
        self.files = {}
        self.contents = {}
        self.children = {}
        self.pages = {}
        self.failures = {}
        self.calls = []
        self.timeout = None
        self.closed = False
        self.changed = {}
        self.get_counts = {}

    def add(self, number, name, mime="application/pdf", data=b"payload", parent=None,
            md5=True, **changes) -> str:
        identifier = file_id(number)
        metadata = {
            "id": identifier, "name": name, "mimeType": mime, "version": "1",
            "modifiedTime": "2026-10-06T00:00:00Z", "trashed": False,
            "webViewLink": "https://drive.google.com/file/d/" + identifier + "/view",
            "capabilities": {"canDownload": True},
        }
        if mime != intake.FOLDER and not mime.startswith("application/vnd.google-apps."):
            metadata["size"] = str(len(data))
            if md5:
                metadata["md5Checksum"] = hashlib.md5(data).hexdigest()
        metadata.update(changes)
        self.files[identifier] = metadata
        self.contents[identifier] = data
        if parent:
            self.children.setdefault(parent, []).append(identifier)
        return identifier

    def fail(self, operation, identifier, *failures):
        self.failures[(operation, identifier)] = list(failures)

    @staticmethod
    def response(status, body, **headers):
        return httplib2.Response({"status": str(status), "content-type": "application/json", **headers}), body

    def request(self, uri, method="GET", body=None, headers=None, **kwargs):
        parts = urlsplit(uri)
        query = {key: values[0] for key, values in parse_qs(parts.query).items()}
        tail = unquote(parts.path.split("/files", 1)[1])
        operation = "list" if tail in ("", "/") else "get"
        identifier = tail.strip("/").split("/")[0]
        if operation == "list":
            match = re.fullmatch(r"'([A-Za-z0-9_-]+)' in parents and trashed = false", query["q"])
            assert match, query
            identifier = match.group(1)
        elif tail.endswith("/export"):
            operation = "export"
        elif query.get("alt") == "media":
            operation = "media"
        headers = {key.lower(): value for key, value in (headers or {}).items()}
        self.calls.append((operation, identifier, query, headers))
        failures = self.failures.get((operation, identifier), [])
        if failures:
            failure = failures.pop(0)
            if isinstance(failure, Exception):
                raise failure
            status, reason = failure if isinstance(failure, tuple) else (failure, "backendError")
            payload = {"error": {"message": "credential-like text must never leak",
                                  "errors": [{"reason": reason}]}}
            return self.response(status, json.dumps(payload).encode())
        if operation == "list":
            token = query.get("pageToken")
            if (identifier, token) in self.pages:
                payload = self.pages[(identifier, token)]
            else:
                payload = {"files": [self.files[key] for key in self.children.get(identifier, [])]}
            return self.response(200, json.dumps(payload).encode())
        if identifier not in self.files:
            return self.response(404, b'{"error":{"errors":[{"reason":"notFound"}]}}')
        if operation == "get":
            self.get_counts[identifier] = self.get_counts.get(identifier, 0) + 1
            payload = self.files[identifier]
            if self.get_counts[identifier] > 1 and identifier in self.changed:
                payload = payload | self.changed[identifier]
            return self.response(200, json.dumps(payload).encode())
        data = self.contents[identifier]
        match = re.fullmatch(r"bytes=(\d+)-(\d+)", headers["range"])
        assert match, headers
        start, end = map(int, match.groups())
        if not data:
            return self.response(200, b"", **{"content-length": "0"})
        end = min(end, len(data) - 1)
        return self.response(206, data[start:end + 1], **{
            "content-type": "application/octet-stream", "content-length": str(end - start + 1),
            "content-range": f"bytes {start}-{end}/{len(data)}",
        })

    def close(self):
        self.closed = True

    def calls_for(self, operation):
        return [call for call in self.calls if call[0] == operation]


class FakeIntakeServer:
    """In-process source slots; no sockets or external network calls."""

    def __init__(self):
        self.slots = {}
        self.attempts = []
        self.statuses = []
        self.receipt = None
        self.creations = 0
        self.connections = []

    def connection(self, host, port, *, timeout):
        server = self

        class Connection:
            closed = False

            def request(self, method, path, *, body, headers):
                assert method == "PUT"
                assert not isinstance(body, (bytes, str))
                chunks = []
                while chunk := body.read(3):
                    chunks.append(chunk)
                payload = b"".join(chunks)
                assert headers["Content-Length"] == str(len(payload))
                assert headers["Authorization"] == "Bearer " + "C" * 43
                identifier = path.rsplit("/", 1)[1]
                server.attempts.append((host, port, timeout, path, payload))
                if identifier in server.slots and server.slots[identifier] != payload:
                    self.status = 409
                else:
                    if identifier not in server.slots:
                        server.creations += 1
                        server.slots[identifier] = payload
                    self.status = server.statuses.pop(0) if server.statuses else 200
                receipt = {"bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
                self.payload = (server.receipt if server.receipt is not None
                                else json.dumps(receipt).encode())

            def getresponse(self):
                connection = self

                class Response:
                    status = connection.status

                    def read(self, maximum):
                        assert maximum == 4097
                        return connection.payload[:maximum]

                return Response()

            def close(self):
                self.closed = True

        connection = Connection()
        self.connections.append(connection)
        return connection


class Checkpoints:
    def __init__(self, crash=None):
        self.values = {}
        self.calls = []
        self.crash = crash

    async def step(self, name, fn, **kwargs):
        assert not kwargs, "Do not rely on .144's ignored retry/timeout step arguments"
        self.calls.append(name)
        if name in self.values:
            return copy.deepcopy(self.values[name])
        value = fn()
        if inspect.isawaitable(value):
            value = await value
        if self.crash == name:
            self.crash = None
            raise RuntimeError("crash before checkpoint put")
        self.values[name] = copy.deepcopy(value)
        return value


@pytest.fixture
def transports(monkeypatch):
    google = FakeDriveHttp()
    server = FakeIntakeServer()
    monkeypatch.setattr(intake, "build_http", lambda: google)
    monkeypatch.setattr(intake.http.client, "HTTPConnection", server.connection)
    monkeypatch.setattr(intake.http.client, "HTTPSConnection", server.connection)
    monkeypatch.setattr(intake.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(intake, "CHUNK_BYTES", 4)
    return google, server


def run(inp, ctx=None):
    result = asyncio.run(intake.handler(inp, ctx or Checkpoints()))
    RESULT.validate(result)
    assert inp["upload_token"] not in json.dumps(result)
    assert "credential-like" not in json.dumps(result)
    for packet in result["packets"]:
        for item in packet["items"]:
            assert not item["path"].startswith("/")
            assert ".." not in item["path"].split("/")
    return result


def acquisitions(result):
    return {item["file_id"]: item["acquisition"]
            for packet in result["packets"] for item in packet["items"]}


def test_native_helpers_resolve_with_both_host_workflow_trees():
    assert intake.WORKFLOW_NAME == "alpha_founder_drive_intake"
    assert intake.WORKFLOW_PRINCIPAL == "alpha-founder-drive-intake"
    assert Path(inspect.getfile(intake.GoogleDriveReadonlyClient)) == CENTAUR / "workflows/gsuite/drive.py"
    assert Path(inspect.getfile(intake.build_http)) == CENTAUR / "workflows/gsuite/http.py"
    assert issubclass(intake.DriveClient, intake.GoogleDriveReadonlyClient)
    assert "163eed54" in REQUEST_SCHEMA["$comment"]
    assert "163eed54" in RESULT_SCHEMA["$comment"]


def test_input_defaults_match_vendored_request(transports):
    google, server = transports
    root = google.add(1, "company", intake.FOLDER)
    parsed = intake.Input.parse(request(root))
    REQUEST.validate(parsed)
    assert parsed["limits"]["max_files"] == 500
    assert parsed["acquire"] is True
    result = run(parsed)
    assert result["complete"] is True
    assert result["packets"][0]["items"] == []
    assert server.attempts == []


@pytest.mark.parametrize("change", [
    {"unknown": True}, {"schema_version": "wrong"}, {"request_id": "short"},
    {"root_file_id": "short"}, {"root_kind": "file"}, {"intake_shape": "other"},
    {"acquire": 1}, {"limits": {"max_files": True}}, {"limits": {"max_depth": 17}},
    {"limits": {"max_pages": 0}}, {"limits": {"unknown": 1}}, {"known": []},
    {"known": {file_id(2): {"sha256": "bad"}}},
    {"known": {file_id(2): {"sha256": "a" * 64, "extra": 1}}},
    {"upload_token": "bad"}, {"upload_url": "file:///tmp/source"},
    {"upload_url": "http://user:pass@intake.invalid/source"},
    {"upload_url": "http://intake.invalid/source?token=value"},
    {"upload_url": "http://intake.invalid:99999/source"},
])
def test_invalid_input_is_rejected_before_any_io(change, transports):
    google, server = transports
    inp = request(file_id(1)) | change
    with pytest.raises(ValueError):
        asyncio.run(intake.handler(inp, Checkpoints()))
    assert google.calls == []
    assert server.attempts == []


def test_more_than_one_listing_page_is_fully_enumerated(transports):
    google, server = transports
    root = google.add(1, "drops", intake.FOLDER)
    children = [google.add(number, f"company-{number}.zip", "application/zip")
                for number in range(2, 105)]
    google.pages[(root, None)] = {"files": [google.files[key] for key in children[:100]], "nextPageToken": "page2"}
    google.pages[(root, "page2")] = {"files": [google.files[key] for key in children[100:]]}
    result = run(request(root, "zip_drops", acquire=False))
    assert result["complete"] is True
    assert result["incomplete_reason"] is None
    assert len(result["packets"]) == 103
    assert set(acquisitions(result)) == set(children)
    pages = google.calls_for("list")
    assert len(pages) == 2 and pages[1][2]["pageToken"] == "page2"
    for _, _, query, _ in pages:
        assert query["supportsAllDrives"] == "true"
        assert query["includeItemsFromAllDrives"] == "true"
        for field in ("nextPageToken", "md5Checksum", "version", "size", "modifiedTime",
                      "webViewLink", "canDownload", "shortcutDetails"):
            assert field in query["fields"]
    assert google.timeout == intake.IO_TIMEOUT_SECONDS
    assert google.closed
    assert not google.calls_for("media") and not server.attempts


@pytest.mark.parametrize("bound", ["max_files", "max_pages", "max_depth", "max_total_bytes"])
def test_each_global_bound_reports_incomplete(bound, transports):
    google, server = transports
    root = google.add(1, "company", intake.FOLDER)
    first = google.add(2, "first.pdf", data=b"abc", parent=root)
    second = google.add(3, "second.pdf", data=b"def", parent=root)
    limits = {bound: 0 if bound == "max_depth" else (3 if bound == "max_total_bytes" else 1)}
    if bound == "max_pages":
        google.pages[(root, None)] = {"files": [google.files[first]], "nextPageToken": "page2"}
        google.pages[(root, "page2")] = {"files": [google.files[second]]}
    elif bound == "max_depth":
        folder = google.add(4, "nested", intake.FOLDER, parent=root)
        google.add(5, "deep.pdf", parent=folder)
    result = run(request(root, limits=limits))
    assert result["complete"] is False
    assert result["incomplete_reason"] == bound
    assert result["packets"][0]["complete"] is False
    if bound == "max_total_bytes":
        assert acquisitions(result)[second]["status"] == "too_large"
        assert second not in server.slots
        assert sum(map(len, server.slots.values())) == 3
    if bound == "max_pages":
        assert len(google.calls_for("list")) == 1


@pytest.mark.parametrize("bound,value", [("max_files", 2), ("max_pages", 2), ("max_depth", 1), ("max_total_bytes", 6)])
def test_exact_bounds_with_no_remaining_work_are_complete(bound, value, transports):
    google, _ = transports
    root = google.add(1, "company", intake.FOLDER)
    if bound == "max_depth":
        folder = google.add(4, "nested", intake.FOLDER, parent=root)
        google.add(2, "first.pdf", data=b"abc", parent=folder)
    else:
        first = google.add(2, "first.pdf", data=b"abc", parent=root)
        second = google.add(3, "second.pdf", data=b"def", parent=root)
        if bound == "max_pages":
            google.pages[(root, None)] = {"files": [google.files[first]], "nextPageToken": "page2"}
            google.pages[(root, "page2")] = {"files": [google.files[second]]}
    result = run(request(root, limits={bound: value}))
    assert result["complete"] is True
    assert result["incomplete_reason"] is None
    assert result["packets"][0]["complete"] is True


def test_zip_file_streams_bytes_and_identity(transports):
    google, server = transports
    root = google.add(1, "winslow.zip", "application/zip", data=b"PK-source-archive")
    result = run(request(root, "zip_file"))
    assert result["root"]["file_id"] == root
    assert result["root"]["kind"] == "file"
    assert result["packets"][0]["source_item_id"] == root
    acquired = acquisitions(result)[root]
    assert acquired == {
        "status": "uploaded", "bytes": 17, "sha256": hashlib.sha256(b"PK-source-archive").hexdigest(),
        "export_format": None, "message": "",
    }
    assert server.slots == {root: b"PK-source-archive"}
    assert all(call[2] == intake.IO_TIMEOUT_SECONDS for call in server.attempts)
    assert all(connection.closed for connection in server.connections)
    assert all(call[2]["supportsAllDrives"] == "true" for call in google.calls_for("get"))


def test_zip_drops_records_non_zip_children_without_recursing(transports):
    google, server = transports
    root = google.add(1, "drops", intake.FOLDER)
    zip_id = google.add(2, "winslow.zip", "application/zip", parent=root)
    text = google.add(3, "readme.txt", "text/plain", parent=root)
    folder = google.add(4, "nested", intake.FOLDER, parent=root)
    google.add(5, "not-an-immediate-drop.zip", "application/zip", parent=folder)
    shortcut = google.add(6, "link.zip", intake.SHORTCUT, parent=root,
                          shortcutDetails={"targetId": zip_id, "targetMimeType": "application/zip"})
    result = run(request(root, "zip_drops"))
    assert {key: value["status"] for key, value in acquisitions(result).items()} == {
        zip_id: "uploaded", text: "unsupported_type", folder: "unsupported_type", shortcut: "shortcut",
    }
    assert list(server.slots) == [zip_id]
    assert [call[1] for call in google.calls_for("list")] == [root]


def test_company_folder_nested_paths_and_duplicate_names(transports):
    google, server = transports
    root = google.add(1, "company", intake.FOLDER)
    folder = google.add(2, "financials", intake.FOLDER, parent=root)
    nested = google.add(3, "year", intake.FOLDER, parent=folder)
    pdf = google.add(4, "deck.pdf", parent=root)
    office = google.add(5, "model.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", parent=nested)
    duplicate = google.add(6, "deck.pdf", parent=root)
    unsafe = google.add(7, "../notes\\name.txt", "text/plain", parent=folder)
    result = run(request(root))
    assert len(result["packets"]) == 1
    packet = result["packets"][0]
    assert packet["kind"] == "folder" and packet["source_item_id"] == root
    paths = {item["file_id"]: item["path"] for item in packet["items"]}
    assert paths[pdf] == "deck.pdf"
    assert paths[office] == "financials/year/model.xlsx"
    assert paths[duplicate] == "deck.pdf~" + duplicate
    assert paths[unsafe] == "financials/..%2Fnotes%5Cname.txt"
    assert set(server.slots) == {pdf, office, duplicate, unsafe}


def test_repeated_page_file_ids_are_emitted_once(transports):
    google, _ = transports
    root = google.add(1, "drops", intake.FOLDER)
    child = google.add(2, "once.zip", "application/zip")
    google.pages[(root, None)] = {"files": [google.files[child]], "nextPageToken": "page2"}
    google.pages[(root, "page2")] = {"files": [google.files[child]]}
    result = run(request(root, "zip_drops", acquire=False))
    assert len(result["packets"]) == 1 and result["complete"] is True


def test_rename_preserves_file_id_and_md5_skips_changed_version(transports):
    google, server = transports
    root = google.add(1, "original.zip", "application/zip")
    before = run(request(root, "zip_file"))
    known = {root: {"md5_checksum": google.files[root]["md5Checksum"], "version": "1",
                    "sha256": acquisitions(before)[root]["sha256"]}}
    media_before = len(google.calls_for("media"))
    uploads_before = len(server.attempts)
    google.files[root].update(name="renamed.zip", version="2")
    after = run(request(root, "zip_file", known=known))
    assert after["packets"][0]["source_item_id"] == before["packets"][0]["source_item_id"]
    assert after["packets"][0]["name"] == "renamed.zip"
    assert acquisitions(after)[root]["status"] == "unchanged"
    assert len(google.calls_for("media")) == media_before
    assert len(server.attempts) == uploads_before


def test_known_native_version_match_skips_export(transports):
    google, server = transports
    root = google.add(1, "company", intake.FOLDER)
    doc = google.add(2, "native", "application/vnd.google-apps.document", parent=root)
    result = run(request(root, known={doc: {"version": "1", "sha256": "a" * 64}}))
    assert acquisitions(result)[doc]["status"] == "unchanged"
    assert acquisitions(result)[doc]["export_format"] == "docx"
    assert not google.calls_for("export") and not server.attempts


def test_conflicting_md5_is_not_unchanged_even_when_version_matches(transports):
    google, server = transports
    root = google.add(1, "changed.zip", "application/zip")
    result = run(request(root, "zip_file", known={root: {"version": "1", "md5_checksum": "old", "sha256": "a" * 64}}))
    assert acquisitions(result)[root]["status"] == "uploaded"
    assert root in server.slots


def test_size_precheck_never_downloads(transports):
    google, server = transports
    root = google.add(1, "large.zip", "application/zip", data=b"abcdef")
    result = run(request(root, "zip_file", limits={"max_file_bytes": 5}))
    assert acquisitions(result)[root]["status"] == "too_large"
    assert not google.calls_for("media") and not server.attempts


def test_unknown_size_stream_limit_never_uploads(transports):
    google, server = transports
    root = google.add(1, "large.zip", "application/zip", data=b"abcdefgh")
    google.files[root].pop("size")
    result = run(request(root, "zip_file", limits={"max_file_bytes": 5}))
    assert acquisitions(result)[root]["status"] == "too_large"
    assert len(google.calls_for("media")) == 2
    assert not server.attempts


def test_unknown_size_total_budget_marks_incomplete(transports):
    google, server = transports
    root = google.add(1, "large.zip", "application/zip", data=b"abcdefgh")
    google.files[root].pop("size")
    result = run(request(root, "zip_file", limits={"max_total_bytes": 5}))
    assert result["complete"] is False
    assert result["incomplete_reason"] == "max_total_bytes"
    assert acquisitions(result)[root]["status"] == "too_large"
    assert not server.attempts


def test_cannot_download_is_explicit(transports):
    google, server = transports
    root = google.add(1, "restricted.zip", "application/zip", capabilities={"canDownload": False})
    result = run(request(root, "zip_file"))
    assert acquisitions(result)[root]["status"] == "download_disallowed"
    assert not google.calls_for("media") and not server.attempts


@pytest.mark.parametrize("native,fmt,mime", [
    ("document", "docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
    ("spreadsheet", "xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    ("presentation", "pdf", "application/pdf"),
])
def test_native_exports_use_complete_representations(native, fmt, mime, transports):
    google, server = transports
    root = google.add(1, "company", intake.FOLDER)
    doc = google.add(2, "native-source", "application/vnd.google-apps." + native,
                     data=b"complete-export", parent=root)
    result = run(request(root))
    acquired = acquisitions(result)[doc]
    assert acquired["status"] == "uploaded" and acquired["export_format"] == fmt
    assert server.slots[doc] == b"complete-export"
    assert all(call[2]["mimeType"] == mime for call in google.calls_for("export"))
    assert not google.calls_for("media")


def test_google_export_size_error_is_not_an_access_error(transports):
    google, server = transports
    root = google.add(1, "company", intake.FOLDER)
    doc = google.add(2, "doc", "application/vnd.google-apps.document", parent=root)
    google.fail("export", doc, (403, "exportSizeLimitExceeded"))
    result = run(request(root))
    assert acquisitions(result)[doc]["status"] == "export_too_large"
    assert not server.attempts


def test_streamed_export_ceiling_is_enforced(transports, monkeypatch):
    google, server = transports
    root = google.add(1, "company", intake.FOLDER)
    doc = google.add(2, "doc", "application/vnd.google-apps.document", data=b"abcdefgh", parent=root)
    monkeypatch.setattr(intake, "EXPORT_MAX_BYTES", 5)
    result = run(request(root))
    assert acquisitions(result)[doc]["status"] == "export_too_large"
    assert not server.attempts


def test_shortcut_target_is_never_followed(transports):
    google, server = transports
    root = google.add(1, "company", intake.FOLDER)
    shortcut = google.add(2, "shortcut", intake.SHORTCUT, parent=root,
                          shortcutDetails={"targetId": file_id(99), "targetMimeType": "application/pdf"})
    result = run(request(root))
    assert acquisitions(result)[shortcut]["status"] == "shortcut"
    assert not google.calls_for("media") and not server.attempts
    assert file_id(99) not in [call[1] for call in google.calls]


@pytest.mark.parametrize("failure,code", [
    ((404, "notFound"), "root_not_found"), ((403, "forbidden"), "access_denied"),
    ((401, "authError"), "invalid_grant"), ((400, "invalid_grant"), "invalid_grant"),
])
def test_root_error_mapping_never_retries_revocation(failure, code, transports):
    google, server = transports
    root = google.add(1, "root", intake.FOLDER)
    google.fail("get", root, failure)
    result = run(request(root))
    assert result["root"] is None and result["complete"] is False
    assert result["errors"][0]["code"] == code
    assert len(google.calls) == 1 and not server.attempts


def test_root_kind_mismatch_is_visible(transports):
    google, _ = transports
    root = google.add(1, "not-a-folder.zip", "application/zip")
    result = run(request(root))
    assert result["root"]["kind"] == "file" and result["complete"] is False
    assert result["errors"][0]["code"] == "root_kind_mismatch"
    assert result["packets"] == []


@pytest.mark.parametrize("status", [429, 503])
def test_transient_root_errors_are_bounded_and_rate_limited(status, transports):
    google, _ = transports
    root = google.add(1, "root", intake.FOLDER)
    google.fail("get", root, status, status, status, status)
    result = run(request(root))
    assert result["errors"][0]["code"] == "rate_limited"
    assert len(google.calls_for("get")) == intake.IO_ATTEMPTS


def test_transient_listing_and_media_requests_recover(transports):
    google, server = transports
    root = google.add(1, "company", intake.FOLDER)
    child = google.add(2, "file.pdf", parent=root)
    google.fail("list", root, 503)
    google.fail("media", child, 429)
    result = run(request(root))
    assert result["complete"] is True and acquisitions(result)[child]["status"] == "uploaded"
    assert len(google.calls_for("list")) == 2 and child in server.slots


def test_failed_listing_retains_partial_packet_and_error(transports):
    google, _ = transports
    root = google.add(1, "company", intake.FOLDER)
    folder = google.add(2, "inaccessible", intake.FOLDER, parent=root)
    google.fail("list", folder, (403, "forbidden"))
    result = run(request(root))
    assert result["complete"] is False and result["packets"][0]["complete"] is False
    assert result["errors"][0]["code"] == "access_denied"


def test_invalid_grant_mid_acquisition_stops_remaining_downloads(transports):
    google, server = transports
    root = google.add(1, "company", intake.FOLDER)
    first = google.add(2, "first.pdf", parent=root)
    second = google.add(3, "second.pdf", parent=root)
    google.fail("media", first, (401, "invalid_grant"))
    result = run(request(root))
    assert result["complete"] is False
    assert result["errors"][0]["code"] == "invalid_grant"
    assert acquisitions(result)[second]["status"] == "failed"
    assert [call[1] for call in google.calls_for("media")] == [first]
    assert not server.attempts


def test_source_change_is_not_uploaded(transports):
    google, server = transports
    root = google.add(1, "source.zip", "application/zip")
    google.changed[root] = {"md5Checksum": "changed", "version": "2"}
    result = run(request(root, "zip_file"))
    assert acquisitions(result)[root]["status"] == "failed"
    assert "changed during acquisition" in acquisitions(result)[root]["message"]
    assert not server.attempts


def test_upload_retries_same_bytes_after_server_5xx_and_is_idempotent(transports):
    google, server = transports
    root = google.add(1, "source.zip", "application/zip", data=b"same-packet")
    server.statuses = [503, 200]
    first = run(request(root, "zip_file"))
    second = run(request(root, "zip_file"))
    assert acquisitions(first)[root]["status"] == acquisitions(second)[root]["status"] == "uploaded"
    assert len(server.attempts) == 3 and server.creations == 1
    assert [attempt[4] for attempt in server.attempts] == [b"same-packet"] * 3


def test_upload_retry_budget_is_finite(transports):
    google, server = transports
    root = google.add(1, "source.zip", "application/zip")
    server.statuses = [503] * 4
    result = run(request(root, "zip_file"))
    assert acquisitions(result)[root]["status"] == "failed"
    assert result["errors"][0]["code"] == "upload_failed"
    assert len(server.attempts) == intake.IO_ATTEMPTS


@pytest.mark.parametrize("status", [302, 401, 409, 413])
def test_permanent_upload_rejection_is_not_retried_or_redirected(status, transports):
    google, server = transports
    root = google.add(1, "source.zip", "application/zip")
    server.statuses = [status]
    result = run(request(root, "zip_file"))
    assert acquisitions(result)[root]["status"] == "failed"
    assert result["errors"][0]["code"] == "upload_failed"
    assert len(server.attempts) == 1


@pytest.mark.parametrize("receipt", [b"not-json", b"{}", b'{"sha256":"wrong","bytes":7}', b"x" * 5000])
def test_bad_upload_receipt_never_claims_success(receipt, transports):
    google, server = transports
    root = google.add(1, "source.zip", "application/zip")
    server.receipt = receipt
    result = run(request(root, "zip_file"))
    assert acquisitions(result)[root]["status"] == "failed"
    assert result["errors"][0]["code"] == "upload_failed"


def test_crash_before_acquisition_checkpoint_repeats_only_idempotent_upload(transports):
    google, server = transports
    root = google.add(1, "source.zip", "application/zip")
    inp = request(root, "zip_file")
    ctx = Checkpoints(crash="acquire:" + root)
    with pytest.raises(RuntimeError, match="crash before checkpoint"):
        asyncio.run(intake.handler(inp, ctx))
    assert "discover" in ctx.values and "acquire:" + root not in ctx.values
    result = run(inp, ctx)
    assert acquisitions(result)[root]["status"] == "uploaded"
    assert len(server.attempts) == 2 and server.creations == 1
    calls = len(google.calls)
    uploads = len(server.attempts)
    replayed = run(inp, ctx)
    assert replayed == result
    assert len(google.calls) == calls and len(server.attempts) == uploads


def test_recovery_accounts_for_checkpointed_bytes_before_next_file(transports):
    google, server = transports
    root = google.add(1, "company", intake.FOLDER)
    first = google.add(2, "first.pdf", data=b"abc", parent=root)
    second = google.add(3, "second.pdf", data=b"def", parent=root)
    inp = request(root, limits={"max_total_bytes": 3})
    ctx = Checkpoints(crash="acquire:" + second)
    with pytest.raises(RuntimeError, match="crash before checkpoint"):
        asyncio.run(intake.handler(inp, ctx))
    result = run(inp, ctx)
    assert acquisitions(result)[first]["status"] == "uploaded"
    assert acquisitions(result)[second]["status"] == "too_large"
    assert result["incomplete_reason"] == "max_total_bytes"
    assert len(server.attempts) == 1
