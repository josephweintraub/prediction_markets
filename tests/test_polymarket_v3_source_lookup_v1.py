"""Bounded synthetic HTTP, raw retention, source and marker fault fixtures."""
from contextlib import ExitStack
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import shutil
import stat
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

from scripts import collect_polymarket_v3_source_lookup_v1 as lookup

HEAD = "a" * 40
DISK = shutil._ntuple_diskusage(10**12, 0, 10**12)


class Clock:
    def __init__(self): self.value = 0.0
    def __call__(self): return self.value
    def sleep(self, seconds): self.value += seconds


class LookupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.input, self.output = self.root / "runtime.json", self.root / "lookup"
        self.source = {"head": HEAD, "tracked_worktree_clean": True, "files": {}}
        blobs, self.runtimes, observations = {}, {}, []
        for index, address in enumerate(lookup.IMPLEMENTATIONS):
            raw = bytes([0x60, index]); sha = hashlib.sha256(raw).hexdigest()
            blobs[sha], self.runtimes[address] = "0x" + raw.hex(), (len(raw), sha)
            for block in ((88978537, 93452331) if index == 0 else (93452332, 94796039)):
                observations.append({"implementation_address": address, "block_number": block,
                    "runtime": {"bytes": len(raw), "sha256": sha}, "runtime_blob_key": sha})
        self.runtime = {"schema_version": "polymarket_v3_runtime_probe_v1", "status": "bounded_v3_runtime_observations_complete",
            "chain_id": 137, "address": lookup.shared.ADDRESS, "source_equivalence_admitted": False,
            "analysis_admitted": False, "source_plan_binding": {"sha256": lookup.PLAN_SHA, "source_head": lookup.PLAN_HEAD},
            "runtime_blobs": blobs, "runtime_observations": observations}
        self.sha = self.save_input()
        self.clock, self.calls, self.queue = Clock(), [], []
        self.mutate = None
        self.source_changed = False

    def save_input(self):
        raw = lookup.encode(self.runtime); self.input.write_bytes(raw)
        return lookup.digest(raw)

    def response(self, index=0, **changes):
        value = {"chainId": "137", "address": lookup.IMPLEMENTATIONS[index], "runtimeMatch": "exact_match",
                 "stdJsonInput": {"sources": {"Exchange.sol": {"content": "contract Exchange {}",
                    "urls": ["https://untrusted.invalid/never-follow-this"]}}}, "compilation": {}, "metadata": {}}
        args = dict(body=json.dumps(value).encode(), status=200, url=lookup.URLS[index], content_type="application/json")
        args.update(changes)
        return lookup.Response(**args)

    def transport(self, url, **kwargs):
        self.calls.append({"url": url, "clock": self.clock(), **kwargs})
        if self.queue:
            value = self.queue.pop(0)
            if isinstance(value, BaseException): raise value
        else:
            value = self.response(lookup.URLS.index(url))
        if self.mutate: value = self.mutate(url, value)
        return value

    def execute(self, **changes):
        args = dict(run_dir=self.output, transport=self.transport, clock=self.clock, sleep=self.clock.sleep)
        args.update(changes)
        snapshots = 0
        def source(_):
            nonlocal snapshots
            snapshots += 1
            return {**self.source, "head": "b" * 40} if self.source_changed and snapshots > 1 else self.source
        with ExitStack() as stack:
            stack.enter_context(patch.object(lookup, "RUNTIME_SHA", self.sha))
            stack.enter_context(patch.object(lookup, "RUNTIMES", self.runtimes))
            stack.enter_context(patch.object(lookup, "source_snapshot", side_effect=source))
            stack.enter_context(patch.object(lookup.shared, "RUN_ROOT", self.root))
            stack.enter_context(patch.object(shutil, "disk_usage", return_value=DISK))
            stack.enter_context(patch.object(lookup.inventory, "resource_state", return_value={
                "ram_bytes": {"MemAvailable": 16 * 1024**3}, "disk_bytes": {"free": 10**12}}))
            return lookup._collect(self.input, self.sha, HEAD, **args)

    def marker(self): return self.output / "controls/COMPLETE.json"

    def test_two_literal_lookups_retain_exact_raw_bytes_and_no_source_admission(self):
        result = self.execute()
        self.assertEqual(result["status"], lookup.COMPLETE)
        self.assertEqual([item["url"] for item in self.calls], list(lookup.URLS))
        self.assertEqual(result["http_attempts"], 2)
        for entry in result["attempts"]:
            data = (self.output / entry["body"]["path"]).read_bytes()
            self.assertEqual(entry["body"]["sha256"], lookup.digest(data))
            self.assertEqual(entry["received_body_sha256"], lookup.digest(data))
            self.assertEqual(entry["content_type"], "application/json")
            self.assertEqual(entry["description"]["source_entries_with_urls_not_followed"], 1)
        self.assertFalse(result["source_equivalence_admitted"])
        self.assertFalse(result["compiler_executed"])
        self.assertEqual(result["rpc_calls"], 0)
        self.assertEqual(result["embedded_urls_followed"], 0)
        marker = json.loads(self.marker().read_text())
        self.assertEqual(marker["result_sha256"], lookup.digest((self.output / "controls/RESULT.json").read_bytes()))

    def test_literal_404_and_unsupported_response_are_retained_unavailable(self):
        self.queue = [self.response(0, status=404, body=b"literal missing", content_type="text/plain"),
                      self.response(1, status=400, body=b"unsupported fields", content_type="text/html")]
        result = self.execute()
        self.assertEqual(result["status"], lookup.COMPLETE)
        self.assertEqual(result["lookups"][0]["availability"], "unavailable_http_404")
        self.assertEqual([item["status"] for item in result["attempts"]], [404, 400])
        self.assertEqual((self.output / result["attempts"][0]["body"]["path"]).read_bytes(), b"literal missing")
        self.assertEqual(len(self.calls), 2)

    def test_four_failed_transports_charge_full_wire_reservations_without_secrets(self):
        self.queue = [OSError("https://SECRET.invalid/key") for _ in range(4)]
        result = self.execute()
        self.assertEqual(result["status"], lookup.COMPLETE)
        self.assertEqual(result["http_attempts"], 4)
        self.assertEqual(result["response_bytes_charged"], lookup.MAX_TOTAL_RESPONSE)
        self.assertTrue(all(item["body"] is None and item["status"] is None for item in result["attempts"]))
        for path in (self.output / "controls").iterdir(): self.assertNotIn(b"SECRET", path.read_bytes())

    def test_retry_after_is_honored_between_all_serialized_calls(self):
        self.queue = [self.response(0, status=429, retry_after="5"), self.response(0), self.response(1)]
        result = self.execute()
        self.assertEqual(result["http_attempts"], 3)
        self.assertGreaterEqual(self.calls[1]["clock"] - self.calls[0]["clock"], 5)
        self.assertEqual(result["attempts"][0]["retry_after"], "5")
        self.assertLessEqual(max(call["timeout"] for call in self.calls), 30)

    def test_two_503_per_address_stop_at_four_and_do_not_raise_caps(self):
        self.queue = [self.response(index // 2, status=503, retry_after="2") for index in range(4)]
        result = self.execute()
        self.assertEqual(result["status"], lookup.COMPLETE)
        self.assertEqual(result["http_attempts"], 4)
        self.assertEqual([item["address_attempt"] for item in result["attempts"]], [1, 2, 1, 2])

    def test_retry_after_beyond_deadline_blocks_and_preserves_first_response(self):
        self.queue = [self.response(0, status=429, retry_after="2000")]
        result = self.execute()
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["http_attempts"], 1)
        self.assertFalse(self.marker().exists())
        self.assertTrue((self.output / "responses/response_001.raw").exists())

    def test_arbitrarily_long_retry_after_cannot_become_an_early_retry(self):
        self.queue = [self.response(0, status=429, retry_after="9" * 100)]
        result = self.execute()
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(len(self.calls), 1)
        self.assertFalse(self.marker().exists())

    def test_partial_response_is_preserved_and_charged_conservatively(self):
        self.queue = [self.response(0, body=b'{"partial":', complete=False, error_code="incomplete_http_body"),
                      self.response(0), self.response(1)]
        result = self.execute()
        self.assertEqual(result["status"], lookup.COMPLETE)
        first = result["attempts"][0]
        self.assertFalse(first["body_complete"])
        self.assertEqual(first["charged_response_bytes"], lookup.MAX_RESPONSE)
        self.assertEqual((self.output / first["body"]["path"]).read_bytes(), b'{"partial":')

    def test_interrupted_response_preserves_bytes_and_never_seals(self):
        self.queue = [self.response(0, body=b"partial", complete=False, error_code="interrupted_http_body")]
        result = self.execute()
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["response_bytes_charged"], lookup.MAX_RESPONSE)
        self.assertFalse(self.marker().exists())
        self.assertEqual((self.output / "responses/response_001.raw").read_bytes(), b"partial")

    def test_close_failed_response_prefix_is_retained_with_full_unknown_charge(self):
        self.queue = [self.response(0, body=b"observed prefix", complete=False,
                                   error_code="http_response_close_failed"), self.response(0), self.response(1)]
        result = self.execute()
        first = result["attempts"][0]
        self.assertEqual(first["status"], 200)
        self.assertEqual(first["content_type"], "application/json")
        self.assertEqual(first["response_error_code"], "http_response_close_failed")
        self.assertFalse(first["body_complete"])
        self.assertEqual(first["charged_response_bytes"], lookup.MAX_RESPONSE)
        self.assertEqual((self.output / first["body"]["path"]).read_bytes(), b"observed prefix")
        self.assertFalse(result["source_equivalence_admitted"])

    def test_detector_sized_body_is_retained_never_accepted_as_complete(self):
        self.queue = [self.response(0, body=b"x" * lookup.MAX_RESPONSE, complete=False,
                                    error_code="response_body_at_detector_limit"), self.response(1)]
        result = self.execute()
        self.assertEqual(result["status"], lookup.COMPLETE)
        first = result["attempts"][0]
        self.assertFalse(first["body_complete"])
        self.assertEqual(first["body"]["bytes"], lookup.MAX_RESPONSE)
        self.assertEqual(first["charged_response_bytes"], lookup.MAX_RESPONSE)

    def test_wrong_url_status_type_and_oversize_transport_fail_closed(self):
        for change in ({"url": "https://evil.invalid"}, {"status": True},
                       {"body": b"x" * (lookup.MAX_RESPONSE + 1)}):
            with self.subTest(keys=list(change)):
                self.output = self.root / ("bad-" + list(change)[0])
                self.queue = [self.response(0, **change)]
                result = self.execute()
                self.assertEqual(result["status"], "blocked")
                self.assertFalse(self.marker().exists())
                self.assertEqual(len(result["attempts"]), 1)

    def test_malformed_duplicate_json_and_embedded_urls_are_never_executed(self):
        for index, body in enumerate((b'{"x":1,"x":2}', b'[1,2]', b'{"x":NaN}', b'not json')):
            self.output = self.root / str(index)
            self.queue = [self.response(0, body=body), self.response(1)]
            result = self.execute()
            self.assertEqual(result["status"], lookup.COMPLETE)
            self.assertIn(result["attempts"][0]["description"]["schema_status"], ("invalid_or_unsupported_json", "json_not_object"))
            self.assertEqual(result["embedded_urls_followed"], 0)

    def test_encoded_response_is_retained_without_decompression(self):
        self.queue = [self.response(0, body=b"compressed bytes", content_encoding="gzip"), self.response(1)]
        result = self.execute()
        self.assertEqual(result["attempts"][0]["description"]["schema_status"], "encoded_body_not_decompressed")

    def test_source_change_and_same_length_input_change_block_completion(self):
        self.source_changed = True
        self.assertEqual(self.execute()["status"], "blocked")
        self.assertFalse(self.marker().exists())
        self.output, self.source_changed = self.root / "input-change", False
        def mutate(url, response):
            if len(self.calls) >= 3:
                data = self.input.read_bytes()
                self.input.write_bytes(data.replace(b'"analysis_admitted":false', b'"analysis_admitted":true ', 1))
            return response
        self.mutate = mutate
        self.assertEqual(self.execute()["status"], "blocked")
        self.assertFalse(self.marker().exists())

    def test_response_artifact_same_length_mutation_blocks_completion(self):
        def mutate(url, response):
            path = self.output / "responses/response_001.raw"
            if path.exists():
                raw = path.read_bytes(); path.write_bytes(b"x" * len(raw))
            return response
        self.mutate = mutate
        result = self.execute()
        self.assertEqual(result["status"], "blocked")
        self.assertFalse(self.marker().exists())

    def test_external_input_or_source_mutation_after_result_blocks_marker(self):
        original = lookup.shared.Controls.write
        for target in ("input", "source"):
            with self.subTest(target=target):
                self.output, self.source_changed = self.root / ("late-result-" + target), False
                self.sha = self.save_input()
                def write(controls, name, value, **kwargs):
                    returned = original(controls, name, value, **kwargs)
                    if name == "RESULT.json":
                        if target == "source": self.source_changed = True
                        else:
                            raw = self.input.read_bytes()
                            changed = raw.replace(b'"analysis_admitted":false', b'"analysis_admitted":true ', 1)
                            self.assertEqual(len(changed), len(raw))
                            self.input.write_bytes(changed)
                    return returned
                with patch.object(lookup.shared.Controls, "write", autospec=True, side_effect=write):
                    with self.assertRaises(lookup.shared.HistoryBlocked): self.execute()
                self.assertFalse(self.marker().exists())

    def test_external_input_or_source_mutation_after_marker_staging_blocks_commit(self):
        real_fsync = lookup.shared.os.fsync
        for target in ("input", "source"):
            with self.subTest(target=target):
                self.output, self.source_changed = self.root / ("late-pending-" + target), False
                self.sha = self.save_input()
                changed = False
                def fsync(fd):
                    nonlocal changed
                    returned = real_fsync(fd)
                    pending = self.output / "controls/.COMPLETE.pending"
                    if not changed and pending.exists() and stat.S_ISDIR(os.fstat(fd).st_mode):
                        changed = True
                        if target == "source": self.source_changed = True
                        else:
                            raw = self.input.read_bytes()
                            replacement = raw.replace(b'"analysis_admitted":false', b'"analysis_admitted":true ', 1)
                            self.assertEqual(len(replacement), len(raw))
                            self.input.write_bytes(replacement)
                    return returned
                with patch.object(lookup.shared.os, "fsync", side_effect=fsync):
                    with self.assertRaises(lookup.shared.HistoryBlocked): self.execute()
                self.assertTrue(changed)
                self.assertTrue((self.output / "controls/.COMPLETE.pending").exists())
                self.assertFalse(self.marker().exists())

    def test_response_symlink_or_extra_artifact_is_not_adopted(self):
        for malicious in ("symlink", "extra"):
            self.output = self.root / malicious
            def mutate(url, response, malicious=malicious):
                path = self.output / "responses/response_001.raw"
                if path.exists():
                    if malicious == "symlink":
                        saved = self.root / "saved-response"; saved.write_bytes(path.read_bytes())
                        path.unlink(); path.symlink_to(saved)
                    else:
                        (path.parent / "unowned.raw").write_bytes(b"not adopted")
                return response
            self.mutate = mutate
            result = self.execute()
            self.assertEqual(result["status"], "blocked")
            self.assertFalse(self.marker().exists())

    def test_owner_and_request_journal_same_length_mutations_block_marker(self):
        original = lookup.shared.Controls.journal
        for name in ("OWNER.json", "REQUESTS.jsonl"):
            self.output = self.root / name.replace(".", "-")
            def journal(controls, value, name=name):
                returned = original(controls, value)
                if value["event"] == "finish":
                    path = controls.root / name; data = path.read_bytes()
                    path.write_bytes((b"x" if data[:1] != b"x" else b"y") + data[1:])
                return returned
            with patch.object(lookup.shared.Controls, "journal", autospec=True, side_effect=journal):
                with self.assertRaises(lookup.shared.HistoryBlocked): self.execute()
            self.assertFalse(self.marker().exists())

    def test_marker_file_and_directory_fsync_faults_leave_no_complete(self):
        original, real_fsync = lookup.shared.Controls.write, lookup.shared.os.fsync
        for fail_directory in (False, True):
            self.output = self.root / ("directory" if fail_directory else "file")
            def write(controls, name, value, **kwargs):
                def fsync(fd):
                    if not fail_directory or stat.S_ISDIR(os.fstat(fd).st_mode): raise OSError("fixture fsync")
                    return real_fsync(fd)
                if name == "COMPLETE.json":
                    with patch.object(lookup.shared.os, "fsync", side_effect=fsync):
                        return original(controls, name, value, **kwargs)
                return original(controls, name, value, **kwargs)
            with patch.object(lookup.shared.Controls, "write", autospec=True, side_effect=write):
                with self.assertRaises(OSError): self.execute()
            self.assertFalse(self.marker().exists())
            self.assertTrue((self.output / "controls/.COMPLETE.pending").exists())

    def test_failed_body_publication_retains_status_and_hash_but_does_not_retry(self):
        original = lookup.Bodies.write
        def fail(store, identifier, body):
            with patch.object(lookup.os, "fsync", side_effect=OSError("fixture body fsync")):
                return original(store, identifier, body)
        with patch.object(lookup.Bodies, "write", autospec=True, side_effect=fail):
            result = self.execute()
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["http_attempts"], 1)
        self.assertEqual(result["attempts"][0]["status"], 200)
        self.assertEqual(result["attempts"][0]["received_body_sha256"], lookup.digest(self.response().body))
        self.assertTrue((self.output / "responses/.response_001.pending").exists())
        self.assertFalse(self.marker().exists())

    def test_partial_body_file_write_keeps_pending_and_never_seals(self):
        original_write, original_fdopen = lookup.Bodies.write, lookup.os.fdopen
        class Partial:
            def __init__(self, fd, mode): self.stream = original_fdopen(fd, mode)
            def __enter__(self): self.stream.__enter__(); return self
            def __exit__(self, *args): return self.stream.__exit__(*args)
            def write(self, raw):
                self.stream.write(raw[:len(raw) // 2])
                self.stream.flush()
                raise OSError("fixture partial write")
        def write(store, identifier, raw):
            with patch.object(lookup.os, "fdopen", side_effect=Partial):
                return original_write(store, identifier, raw)
        with patch.object(lookup.Bodies, "write", autospec=True, side_effect=write): result = self.execute()
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["http_attempts"], 1)
        self.assertEqual((self.output / "responses/.response_001.pending").stat().st_size, len(self.response().body) // 2)
        self.assertFalse(self.marker().exists())

    def test_parent_completion_competitor_is_never_overwritten(self):
        original = lookup.shared.atomic_completion
        def commit(temporary, destination):
            if destination.name == "COMPLETE.json": destination.write_bytes(b"competing certificate")
            return original(temporary, destination)
        with patch.object(lookup.shared, "atomic_completion", side_effect=commit):
            with self.assertRaisesRegex(lookup.shared.HistoryBlocked, "atomic_completion_refused"):
                self.execute()
        self.assertEqual(self.marker().read_bytes(), b"competing certificate")

    def test_existing_directory_and_bad_runtime_binding_precede_transport(self):
        self.output.mkdir()
        with self.assertRaises(lookup.shared.HistoryBlocked): self.execute()
        self.assertEqual(self.calls, [])
        self.output = self.root / "unused"
        self.runtime["runtime_observations"].pop(); self.sha = self.save_input()
        with self.assertRaises(lookup.shared.HistoryBlocked): self.execute()
        self.assertEqual(self.calls, [])

    def test_guard_precedes_reads_and_own_test_blob_is_bound(self):
        self.assertIn("tests/test_polymarket_v3_source_lookup_v1.py", lookup.SOURCE_FILES)
        with patch.object(lookup, "require_production_host", side_effect=RuntimeError("fixture")), \
                patch.object(lookup, "read_runtime") as read:
            with self.assertRaises(RuntimeError): lookup.collect(Path("/never"), "x", HEAD, run_dir=Path("/never"))
            read.assert_not_called()

    def test_source_requires_committed_exact_bytes(self):
        with patch.object(lookup.inventory, "code_state", return_value={"status": "inspected", "head": HEAD, "tracked_worktree_clean": True}), \
                patch.object(lookup.subprocess, "run", return_value=type("Git", (), {"returncode": 0, "stdout": b"wrong"})()):
            with self.assertRaisesRegex(lookup.shared.HistoryBlocked, "source_blob"):
                lookup.source_snapshot(HEAD)

    def test_own_test_blob_is_independently_checked_not_only_listed(self):
        visited = []
        def git(command, **kwargs):
            name = command[-1].split(":", 1)[1]; visited.append(name)
            data = (lookup.REPO / name).read_bytes()
            if name == "tests/test_polymarket_v3_source_lookup_v1.py": data += b"changed"
            return type("Git", (), {"returncode": 0, "stdout": data})()
        with patch.object(lookup.inventory, "code_state", return_value={"status": "inspected", "head": HEAD, "tracked_worktree_clean": True}), \
                patch.object(lookup.subprocess, "run", side_effect=git):
            with self.assertRaisesRegex(lookup.shared.HistoryBlocked, "source_blob"):
                lookup.source_snapshot(HEAD)
        self.assertEqual(visited[-1], "tests/test_polymarket_v3_source_lookup_v1.py")

    def test_deadline_after_response_preserves_bytes_but_never_seals(self):
        def mutate(url, response):
            self.clock.value = lookup.MAX_RUNTIME
            return response
        self.mutate = mutate
        result = self.execute()
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["http_attempts"], 1)
        self.assertTrue((self.output / "responses/response_001.raw").exists())
        self.assertFalse(self.marker().exists())


class GETTransportTests(unittest.TestCase):
    class Headers:
        def __init__(self, values): self.values = values
        def get_all(self, name, default): return self.values.get(name, default)

    class HTTP:
        def __init__(self, status, body, headers=None):
            self.status, self.body, self.pos = status, body, 0
            self.headers = GETTransportTests.Headers(headers or {"Content-Type": ["application/json"]})
            self.amounts, self.closed = [], False
        def geturl(self): return lookup.URLS[0]
        def __enter__(self): return self
        def __exit__(self, *args): self.closed = True
        def close(self): self.closed = True
        def read(self, amount):
            self.amounts.append(amount)
            value = self.body[self.pos:self.pos + amount]; self.pos += len(value)
            return value

    def get(self, response):
        captured = {}
        class Opener:
            def open(self, request, timeout):
                captured["request"], captured["timeout"] = request, timeout
                return response
        def build(*handlers): captured["handlers"] = handlers; return Opener()
        with patch.object(lookup.urllib.request, "build_opener", side_effect=build):
            result = lookup.get_response(lookup.URLS[0], byte_limit=lookup.MAX_RESPONSE, timeout=5)
        return result, captured

    def test_get_disables_proxy_redirect_auth_and_uses_fixed_selected_fields(self):
        http = self.HTTP(200, b"{}")
        result, seen = self.get(http)
        self.assertEqual(result.body, b"{}")
        self.assertEqual(seen["request"].method, "GET")
        self.assertEqual(seen["request"].full_url, lookup.URLS[0])
        self.assertEqual(seen["handlers"][0].proxies, {})
        self.assertIsInstance(seen["handlers"][1], lookup.NoRedirect)
        self.assertIsNone(seen["handlers"][1].redirect_request(None, None, 302, "", {}, "https://evil.invalid"))
        self.assertEqual(seen["request"].get_header("Accept-encoding"), "identity")
        self.assertIsNone(seen["request"].get_header("Authorization"))
        self.assertIn("prediction-markets", seen["request"].get_header("User-agent"))
        self.assertTrue(http.closed)
        self.assertTrue(all(amount <= 64 * 1024 for amount in http.amounts))

    def test_http_404_error_body_status_and_type_are_preserved(self):
        headers = self.Headers({"Content-Type": ["text/plain"]})
        error = urllib.error.HTTPError(lookup.URLS[0], 404, "missing", headers, io.BytesIO(b"literal404"))
        class Opener:
            def open(self, *_args, **_kwargs): raise error
        with patch.object(lookup.urllib.request, "build_opener", return_value=Opener()):
            value = lookup.get_response(lookup.URLS[0], byte_limit=lookup.MAX_RESPONSE, timeout=5)
        self.assertEqual((value.status, value.body, value.content_type), (404, b"literal404", "text/plain"))

    def test_body_detector_and_declared_length_mismatch_are_not_complete(self):
        value, _ = self.get(self.HTTP(200, b"x" * lookup.MAX_RESPONSE))
        self.assertFalse(value.complete)
        self.assertEqual(len(value.body), lookup.MAX_RESPONSE)
        value, _ = self.get(self.HTTP(200, b"short", {"Content-Length": ["100"], "Content-Type": ["application/json"]}))
        self.assertFalse(value.complete)
        self.assertEqual(value.error_code, "declared_http_length_differs")

    def test_redirect_status_is_returned_without_following_location(self):
        value, _ = self.get(self.HTTP(302, b"redirect refused", {"Content-Type": ["text/plain"]}))
        self.assertEqual(value.status, 302)
        self.assertEqual(value.body, b"redirect refused")

    def test_read_timeout_retains_prior_streamed_bytes(self):
        wire = self.HTTP(200, b"")
        count = 0
        def read(_):
            nonlocal count
            count += 1
            if count == 1: return b"first piece"
            raise TimeoutError("not a saved error message")
        wire.read = read
        value, _ = self.get(wire)
        self.assertEqual(value.body, b"first piece")
        self.assertFalse(value.complete)
        self.assertEqual(value.error_code, "http_body_read_failed")

    def test_close_failure_does_not_discard_already_observed_prefix(self):
        class BrokenClose(self.HTTP):
            def close(self):
                self.closed = True
                raise OSError("https://SECRET.invalid/close-error")
        wire = BrokenClose(200, b"observed prefix")
        value, _ = self.get(wire)
        self.assertTrue(wire.closed)
        self.assertEqual(value.body, b"observed prefix")
        self.assertEqual(value.content_type, "application/json")
        self.assertEqual(value.status, 200)
        self.assertFalse(value.complete)
        self.assertEqual(value.error_code, "http_response_close_failed")
        self.assertNotIn("SECRET", str(value))

    def test_incomplete_read_preserves_exact_partial_bytes(self):
        wire = self.HTTP(200, b"")
        def read(_): raise http_client_partial
        http_client_partial = http.client.IncompleteRead(b"partial", 100)
        wire.read = read
        value, _ = self.get(wire)
        self.assertFalse(value.complete)
        self.assertEqual(value.body, b"partial")
        self.assertEqual(value.error_code, "incomplete_http_body")

    def test_nonfixed_url_is_refused_before_opener_creation(self):
        with patch.object(lookup.urllib.request, "build_opener") as build:
            with self.assertRaises(lookup.shared.HistoryBlocked):
                lookup.get_response("https://evil.invalid", byte_limit=lookup.MAX_RESPONSE, timeout=5)
            build.assert_not_called()


if __name__ == "__main__":
    unittest.main()
