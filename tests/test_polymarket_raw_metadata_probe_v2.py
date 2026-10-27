"""Small synthetic fixtures only. Never contact a metadata endpoint."""
import gzip
import http.client
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from scripts import collect_polymarket_raw_metadata_probe_v2 as probe

HEAD = "1" * 40
SOURCE = {"head": HEAD, "files": {"fixture.py": {"bytes": 1, "sha256": "2" * 64}}}
FREE = shutil._ntuple_diskusage(10**12, 0, 10**12)


def clob_row(number=1):
    return {"condition_id": "0x" + f"{number:064x}", "question_id": "0x" + f"{number:064x}",
            "version": "v1", "closed": True, "tokens": [
                {"token_id": str(number * 2), "outcome": "A", "winner": False, "price": .99999},
                {"token_id": str(number * 2 + 1), "outcome": "B", "winner": False, "price": .00001}]}


def clob_page(cursor="MTAwMA==", rows=None, **changes):
    rows = [clob_row()] if rows is None else rows
    body = {"limit": 1000, "count": len(rows), "next_cursor": cursor, "data": rows}
    body.update(changes)
    return json.dumps(body, separators=(",", ":")).encode()


def gamma_row(stream, number=1):
    return {"id": str(number), "conditionId": "0x" + f"{number:064x}",
            "questionID": "0x" + f"{number:064x}", "version": stream["version"],
            "closed": stream["closed"], "clobTokenIds": json.dumps([str(number * 2), str(number * 2 + 1)]),
            "positionIds": None, "tags": [{"label": "Sports"}], "outcomePrices": "[\"1\",\"0\"]"}


def gamma_page(stream, *, cursor=None, rows=None, number=1):
    if rows is None:
        rows = [gamma_row(stream, number + index) for index in range(10 if cursor is not None else 1)]
    body = {"markets": rows}
    if cursor is not None:
        body["next_cursor"] = cursor
    return json.dumps(body, separators=(",", ":")).encode()


class Response:
    def __init__(self, raw, *, status=200, headers=None, url=None):
        self.raw, self.status, self.url = raw, status, url
        self.headers = {"Content-Type": "application/json", "Content-Length": str(len(raw))}
        if headers:
            self.headers.update(headers)
        self.stream, self.read_bytes, self.closed = io.BytesIO(raw), 0, False

    def read(self, size):
        value = self.stream.read(size)
        self.read_bytes += len(value)
        return value

    def geturl(self):
        return self.url

    def close(self):
        self.closed = True


def terminal_responses():
    return [Response(clob_page(probe.END_CURSOR))] + [Response(gamma_page(stream)) for stream in probe.STREAMS[1:]]


def capped_responses():
    result = [Response(clob_page()), Response(clob_page("MjAwMA==", [clob_row(2)]))]
    for stream in probe.STREAMS[1:]:
        result += [Response(gamma_page(stream, cursor="opaque-first_-", number=1)),
                   Response(gamma_page(stream, cursor="opaque-second_-", number=11))]
    return result


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.parent = Path(self.temp.name).resolve()
        self.output = self.parent / "run"
        self.calls, self.responses = [], []

    def transport(self, spec, timeout):
        self.calls.append((spec, timeout))
        value = self.responses.pop(0)
        if isinstance(value, BaseException):
            raise value
        value.url = value.url or spec["url"]
        return value

    def run_fixture(self, responses=None, *, source_effect=None, transport=None):
        self.responses = capped_responses() if responses is None else responses
        with patch.object(probe, "source_snapshot", side_effect=source_effect, return_value=SOURCE), \
                patch.object(probe, "available_ram", return_value=10**12), \
                patch.object(probe.shutil, "disk_usage", return_value=FREE):
            return probe.run_probe(self.output, HEAD, transport=transport or self.transport)

    def read(self, relative):
        return json.loads((self.output / relative).read_bytes())

    def body(self, attempt=1):
        return gzip.decompress((self.output / f"responses/a{attempt:02d}.body.gz").read_bytes())

    def assert_failed(self, value, code):
        self.assertEqual(value["status"], "failed_unaccepted")
        self.assertEqual(value["error_code"], code)
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.assertFalse(value["analysis_admitted"])

    def test_five_fixed_streams_two_pages_each_never_full_universe(self):
        value = self.run_fixture()
        self.assertEqual(value["status"], "probe_complete_universe_incomplete")
        self.assertEqual(value["successful_pages"], 10)
        self.assertEqual(len(self.calls), 10)
        self.assertEqual(value["raw_rows_in_validated_pages"], 82)
        manifest = self.read("manifest.json")
        self.assertEqual([s["id"] for s in manifest["streams"]], [s["id"] for s in probe.STREAMS])
        self.assertTrue(all(s["stopped_at_page_cap"] for s in manifest["streams"]))
        self.assertTrue(all(not s["pagination_terminal_observed"] for s in manifest["streams"]))
        self.assertFalse(manifest["universe_complete"])
        self.assertFalse(manifest["analysis_admitted"])
        self.assertFalse(manifest["normalized_labels_emitted"])
        self.assertFalse(manifest["family_or_payout_admitted"])
        self.assertFalse(manifest["snapshot_consistency_certified"])
        self.assertEqual(self.body(), clob_page())
        self.assertEqual(len(self.read("rows/a01.json")["records"]), 1)
        complete = self.read("COMPLETE.json")
        self.assertEqual(complete["manifest"]["sha256"], probe.digest((self.output / "manifest.json").read_bytes()))

    def test_stream_scope_parameters_and_continuation_are_exact(self):
        self.run_fixture()
        first = parse_qs(urlsplit(self.calls[0][0]["url"]).query)
        self.assertEqual(first, {"next_cursor": ["MA=="]})
        for index, stream in enumerate(probe.STREAMS[1:]):
            one = parse_qs(urlsplit(self.calls[2 + 2 * index][0]["url"]).query)
            two = parse_qs(urlsplit(self.calls[3 + 2 * index][0]["url"]).query)
            expected = {"version": [stream["version"]], "closed": [str(stream["closed"]).lower()],
                        "limit": ["10"], "include_tag": ["true"], "order": ["id"], "ascending": ["true"]}
            self.assertEqual(one, expected)
            self.assertEqual(two, expected | {"after_cursor": ["opaque-first_-"]})
            self.assertNotIn("cursor", one)
            self.assertNotIn("offset", one)

    def test_all_explicit_terminal_proofs_remain_probe_only(self):
        value = self.run_fixture(terminal_responses())
        self.assertEqual(value["status"], "probe_complete_terminal_cursors_observed")
        self.assertEqual(value["successful_pages"], 5)
        self.assertTrue(value["all_streams_terminal_observed"])
        self.assertFalse(value["universe_complete"])
        self.assertTrue(all(s["pagination_terminal_observed"] for s in self.read("manifest.json")["streams"]))

    def test_observed_gamma_schema_field_preserves_raw_pages_rows_and_cursors(self):
        responses = capped_responses()
        for index in range(2, len(responses)):
            body = json.loads(responses[index].raw)
            body["$schema"] = "https://gamma-api.polymarket.com/schemas/MarketsKeysetListResponse.json"
            responses[index] = Response(json.dumps(body, indent=1).encode() + b"\n")
        expected_raw = [response.raw for response in responses]
        value = self.run_fixture(responses)
        self.assertEqual(value["status"], "probe_complete_universe_incomplete")
        self.assertEqual(value["successful_pages"], 10)
        self.assertEqual(value["raw_rows_in_validated_pages"], 82)
        for index, raw in enumerate(expected_raw, 1):
            self.assertEqual(self.body(index), raw)
            rows = json.loads(raw).get("markets", json.loads(raw).get("data"))
            self.assertEqual(len(self.read(f"rows/a{index:02d}.json")["records"]), len(rows))
        for stream in self.read("manifest.json")["streams"][1:]:
            self.assertTrue(stream["returned_scope_fields_verified"])
            self.assertTrue(stream["stopped_at_page_cap"])
            self.assertFalse(stream["pagination_terminal_observed"])
            self.assertEqual(stream["next_cursor"], "opaque-second_-")
        self.assertFalse(value["universe_complete"])
        self.assertFalse(value["analysis_admitted"])

    def test_gamma_schema_field_on_short_or_empty_terminal_preserves_all_rows(self):
        stream = probe.STREAMS[1]
        for index, rows in enumerate(([], [None, {}, {"conditionId": None}, gamma_row(stream)])):
            self.output = self.parent / f"schema_terminal_{index}"
            body = json.loads(gamma_page(stream, rows=rows))
            body["$schema"] = probe.GAMMA_SCHEMA_URI
            raw = json.dumps(body, indent=2).encode()
            responses = terminal_responses()
            responses[1] = Response(raw)
            value = self.run_fixture(responses)
            self.assertEqual(value["status"], "probe_complete_terminal_cursors_observed")
            self.assertEqual(value["raw_rows_in_validated_pages"], 4 + len(rows))
            self.assertEqual(self.body(2), raw)
            self.assertEqual(len(self.read("rows/a02.json")["records"]), len(rows))
            receipt = self.read("manifest.json")["streams"][1]
            self.assertTrue(receipt["pagination_terminal_observed"])
            self.assertIsNone(receipt["next_cursor"])
            self.assertFalse(value["universe_complete"])

    def test_gamma_schema_field_wrong_type_value_and_unknown_keys_fail_closed(self):
        invalid = (None, False, 1, [], {}, "", "http://gamma-api.polymarket.com/schemas/MarketsKeysetListResponse.json",
                   probe.GAMMA_SCHEMA_URI + "/", "https://example.invalid/MarketsKeysetListResponse.json")
        for index, schema in enumerate(invalid):
            self.output = self.parent / f"schema_invalid_{index}"
            body = json.loads(gamma_page(probe.STREAMS[1])) | {"$schema": schema}
            raw = json.dumps(body).encode()
            value = self.run_fixture([terminal_responses()[0], Response(raw)])
            self.assert_failed(value, "gamma_schema_uri_invalid")
            self.assertEqual(self.body(2), raw)
        self.output = self.parent / "schema_unknown_key"
        body = json.loads(gamma_page(probe.STREAMS[1])) | {"$schema": probe.GAMMA_SCHEMA_URI, "extra": "unaccepted"}
        raw = json.dumps(body).encode()
        self.assert_failed(self.run_fixture([terminal_responses()[0], Response(raw)]), "page_envelope_schema")
        self.assertEqual(self.body(2), raw)

    def test_gamma_schema_field_does_not_weaken_pagination_or_scope_checks(self):
        stream = probe.STREAMS[1]
        for body, code in (({"markets": [gamma_row(stream)] * 10}, "gamma_full_page_missing_cursor"),
                           ({"markets": [], "next_cursor": "opaque"}, "gamma_short_page_has_cursor"),
                           ({"markets": [], "next_cursor": None}, "invalid_cursor")):
            body["$schema"] = probe.GAMMA_SCHEMA_URI
            with self.assertRaisesRegex(probe.ProbeError, code):
                probe.page_envelope(json.dumps(body).encode(), stream, None, {None}, set())
        for index, change in enumerate(({"version": "v2"}, {"closed": True})):
            self.output = self.parent / f"schema_scope_{index}"
            body = {"$schema": probe.GAMMA_SCHEMA_URI, "markets": [gamma_row(stream) | change]}
            self.assert_failed(self.run_fixture([terminal_responses()[0], Response(json.dumps(body).encode())]),
                               "gamma_returned_scope_conflict")

    def test_gamma_schema_field_is_not_permitted_in_clob_envelope(self):
        raw = clob_page(probe.END_CURSOR, **{"$schema": probe.GAMMA_SCHEMA_URI})
        self.assert_failed(self.run_fixture([Response(raw)]), "page_envelope_schema")
        self.assertEqual(self.body(), raw)

    def test_real_blank_identifier_counts_remain_raw_rows(self):
        rows = [clob_row(index + 1) for index in range(1000)]
        for index in range(165, 219):
            rows[index]["condition_id"] = rows[index]["question_id"] = ""
            for token in rows[index]["tokens"]:
                token["token_id"] = ""
        for token in rows[10]["tokens"]:
            token["token_id"] = ""
        for index in (12, 13, 14):
            rows[index]["question_id"] = ""
        raw = clob_page(probe.END_CURSOR, rows)
        value = self.run_fixture([Response(raw), *terminal_responses()[1:]])
        self.assertEqual(value["successful_pages"], 5)
        self.assertEqual(self.body(), raw)
        summary = self.read("rows/a01.json")
        self.assertEqual(summary["raw_row_count"], 1000)
        self.assertEqual(len(summary["records"]), 1000)
        self.assertEqual(summary["identity_issue_counts"]["condition_id_empty"], 54)
        self.assertEqual(summary["identity_issue_counts"]["question_id_empty"], 57)
        self.assertEqual(summary["identity_issue_counts"]["token_id_empty"], 110)
        self.assertEqual(summary["literal_winner_counts"], {"literal_false": 2000})
        self.assertEqual(summary["records"][165]["locator"], "clob/p00/r000165")

    def test_scalar_and_unsupported_rows_are_retained_counted_and_flagged(self):
        raw = clob_page(probe.END_CURSOR, [None, False, 1, "x", [], {}, {"tokens": [None, {"winner": 1}]}])
        value = self.run_fixture([Response(raw), *terminal_responses()[1:]])
        self.assertEqual(value["successful_pages"], 5)
        summary = self.read("rows/a01.json")
        self.assertEqual(len(summary["records"]), 7)
        self.assertEqual(sum(summary["row_types"].values()), 7)
        self.assertEqual(summary["identity_issue_counts"]["row_not_object"], 5)
        self.assertEqual(summary["identity_issue_counts"]["winner_unsupported_shape"], 1)
        self.assertEqual(self.body(), raw)

    def test_missing_gamma_scope_does_not_erase_terminal_cursor_proof(self):
        rows = [None, {}, {"version": None, "closed": None}]
        responses = terminal_responses()
        responses[1] = Response(gamma_page(probe.STREAMS[1], rows=rows))
        value = self.run_fixture(responses)
        self.assertEqual(value["successful_pages"], 5)
        receipt = self.read("manifest.json")["streams"][1]
        self.assertTrue(receipt["pagination_terminal_observed"])
        self.assertFalse(receipt["returned_scope_fields_verified"])
        summary = self.read("rows/a02.json")
        self.assertEqual(summary["identity_issue_counts"]["scope_unverifiable"], 3)
        self.assertEqual(summary["raw_row_count"], 3)

    def test_explicit_wrong_gamma_version_or_closed_fails_with_body_and_counts(self):
        for index, change in enumerate(({"version": "v2"}, {"closed": True}, {"version": 2}, {"closed": "false"})):
            self.output = self.parent / f"scope_{index}"
            row = gamma_row(probe.STREAMS[1]) | change
            raw = gamma_page(probe.STREAMS[1], rows=[row])
            value = self.run_fixture([terminal_responses()[0], Response(raw)])
            self.assert_failed(value, "gamma_returned_scope_conflict")
            self.assertEqual(self.body(2), raw)
            finish = self.read("attempts/a02.FINISH.json")
            self.assertEqual(finish["page"]["count"], 1)
            self.assertTrue(finish["page"]["pagination_terminal_observed"])
            self.assertTrue(finish["page"]["row_summary"]["scope_conflict_counts"])

    def test_prices_and_literal_winner_fields_never_emit_payout_or_claim(self):
        self.run_fixture(terminal_responses())
        self.assertIn(b'0.99999', self.body())
        self.assertFalse(json.loads(self.body())["data"][0]["tokens"][0]["winner"])
        for relative in ("rows/a01.json", "rows/a02.json"):
            summary = self.read(relative)
            self.assertNotIn("Y", summary)
            self.assertNotIn("winning_outcome", summary)
            self.assertNotIn("category", summary)
        self.assertFalse(self.read("manifest.json")["family_or_payout_admitted"])

    def test_gamma_asset_arrays_are_only_shape_evidence(self):
        row = gamma_row(probe.STREAMS[1]) | {"clobTokenIds": '["",7]', "positionIds": "not-json"}
        result = probe.inspect_rows([row], probe.STREAMS[1], 0)
        self.assertEqual(result["identity_issue_counts"]["clobTokenIds_empty_id"], 1)
        self.assertEqual(result["identity_issue_counts"]["clobTokenIds_unsupported_id_shape"], 1)
        self.assertEqual(result["identity_issue_counts"]["positionIds_unsupported_shape"], 1)
        self.assertEqual(result["raw_row_count"], 1)

    def test_provider_cursor_conventions_are_not_interchanged(self):
        gamma = probe.STREAMS[1]
        for cursor in ("opaque-url_safe-_", "LTE="):
            value = probe.page_envelope(gamma_page(gamma, cursor=cursor), gamma, None, {None}, set())
            self.assertFalse(value["pagination_terminal_observed"])
        for cursor in (None, "", "not base64", "Zg===", 1):
            with self.subTest(cursor=cursor):
                with self.assertRaisesRegex(probe.ProbeError, "invalid_cursor"):
                    probe.page_envelope(clob_page(cursor), probe.STREAMS[0], "MA==", {"MA=="}, set())
        for body, code in (({"markets": [gamma_row(gamma)] * 10}, "gamma_full_page_missing_cursor"),
                           ({"markets": [], "next_cursor": "opaque"}, "gamma_short_page_has_cursor"),
                           ({"markets": [], "next_cursor": None}, "invalid_cursor"),
                           ({"markets": [], "next_cursor": ""}, "invalid_cursor")):
            with self.assertRaisesRegex(probe.ProbeError, code):
                probe.page_envelope(json.dumps(body).encode(), gamma, None, {None}, set())

    def test_empty_terminal_and_missing_clob_cursor(self):
        raw = clob_page(probe.END_CURSOR, [])
        value = self.run_fixture([Response(raw), *terminal_responses()[1:]])
        self.assertEqual(value["successful_pages"], 5)
        self.output = self.parent / "empty_nonterminal"
        self.assert_failed(self.run_fixture([Response(clob_page(rows=[]))]), "empty_nonterminal_page")
        body = json.loads(clob_page()); del body["next_cursor"]
        with self.assertRaisesRegex(probe.ProbeError, "page_envelope_schema"):
            probe.page_envelope(json.dumps(body).encode(), probe.STREAMS[0], "MA==", {"MA=="}, set())

    def test_repeated_cursor_and_repeated_rows_with_different_cursor_fail(self):
        self.assert_failed(self.run_fixture([Response(clob_page("MA=="))]), "repeated_cursor")
        self.output = self.parent / "repeated_rows"
        self.assert_failed(self.run_fixture([Response(clob_page()), Response(clob_page("MjAwMA=="))]), "repeated_page_rows")
        gamma = probe.STREAMS[1]
        with self.assertRaisesRegex(probe.ProbeError, "repeated_cursor"):
            probe.page_envelope(gamma_page(gamma, cursor="x"), gamma, "x", {"x"}, set())

    def test_envelope_and_json_errors_fail_closed(self):
        cases = [(clob_page(count=True), "page_count_or_data_schema"),
                 (clob_page(limit=False), "page_count_or_data_schema"),
                 (clob_page(count=2), "page_count_or_data_schema"),
                 (clob_page(data={}), "page_count_or_data_schema"),
                 (clob_page(error="ignored"), "page_envelope_schema"),
                 (b'{"data":[],"data":[]}', "duplicate_json_key"),
                 (b'{"x":NaN}', "nonfinite_json"), (b'{"x":1e999}', "nonfinite_json"),
                 (b'\xff', "invalid_json"), (b'<html/>', "invalid_json")]
        for index, (raw, code) in enumerate(cases):
            self.output = self.parent / f"invalid_{index}"
            self.assert_failed(self.run_fixture([Response(raw)]), code)
            self.assertEqual(self.body(), raw)

    def test_gzip_is_deterministic_exact_raw_bytes_and_both_hashes_reopen(self):
        raw = b'{  "kept formatting": [1, 2] }\n'
        one, two = probe.gzip_bytes(raw), probe.gzip_bytes(raw)
        self.assertEqual(one, two)
        self.assertEqual(one[4:8], b'\0\0\0\0')
        self.assertFalse(one[3] & 8)
        self.assertEqual(gzip.decompress(one), raw)
        self.run_fixture(terminal_responses())
        proof = self.read("attempts/a01.FINISH.json")["body"]
        probe.verify_body(self.output, proof)
        self.assertEqual(proof["sha256"], probe.digest((self.output / proof["path"]).read_bytes()))
        self.assertEqual(proof["uncompressed_sha256"], probe.digest(self.body()))
        with self.assertRaisesRegex(probe.ProbeError, "decompressed_body_reopen_differs"):
            probe.verify_body(self.output, proof | {"uncompressed_bytes": 1})

    def test_retryable_refusals_and_transport_failures_charge_same_scope(self):
        busy = b'{"error":"busy"}'
        value = self.run_fixture([Response(busy, status=429), OSError("api_key=DO_NOT_DISCLOSE"), *terminal_responses()])
        self.assertEqual(value["http_attempts"], 7)
        self.assertEqual(len({self.calls[index][0]["url"] for index in range(3)}), 1)
        self.assertEqual(self.read("manifest.json")["response_bytes_charged"],
                         len(busy) + probe.CAPS["individual_response_bytes"] + sum(len(r.raw) for r in terminal_responses()))
        self.assertEqual(self.body(2), b"")
        self.assertFalse(self.read("attempts/a02.FINISH.json")["body_complete"])

    def test_twenty_attempt_cap_and_secret_exception_redaction(self):
        value = self.run_fixture([Response(b'{}', status=503) for _ in range(20)])
        self.assert_failed(value, "http_attempt_limit")
        self.assertEqual(len(self.calls), 20)
        self.assertEqual(value["response_bytes_charged"], 40)
        self.output = self.parent / "secret"
        value = self.run_fixture([RuntimeError("password=DO_NOT_DISCLOSE")])
        self.assert_failed(value, "request_runtime_error")
        for file in self.output.rglob("*"):
            if file.is_file():
                self.assertNotIn(b"DO_NOT_DISCLOSE", file.read_bytes())

    def test_unknown_body_charge_exhausts_shared_32mib_budget(self):
        value = self.run_fixture([OSError("secret") for _ in range(20)])
        self.assert_failed(value, "total_response_byte_limit")
        self.assertEqual(len(self.calls), 8)
        self.assertEqual(value["response_bytes_charged"], 32 * 1024**2)
        self.assertEqual(value["response_bytes_observed"], 0)
        self.assertEqual(len(list((self.output / "responses").glob("*.gz"))), 8)

    def test_close_fault_retains_body_and_charges_full_allowance(self):
        class CloseFault(Response):
            def close(self):
                raise OSError("password=DO_NOT_DISCLOSE")
        raw = clob_page()
        value = self.run_fixture([CloseFault(raw)])
        self.assert_failed(value, "response_close_error")
        self.assertEqual(self.body(), raw)
        self.assertEqual(value["response_bytes_observed"], len(raw))
        self.assertEqual(value["response_bytes_charged"], probe.CAPS["individual_response_bytes"])
        self.assertTrue(self.read("attempts/a01.FINISH.json")["charge_is_conservative"])

    def test_incomplete_read_retains_exposed_partial_body_and_full_charge(self):
        class Partial(Response):
            def read(self, size):
                raise http.client.IncompleteRead(b"x" * 123)
        value = self.run_fixture([Partial(clob_page())])
        self.assert_failed(value, "incomplete_response")
        self.assertEqual(self.body(), b"x" * 123)
        self.assertEqual(value["response_bytes_observed"], 123)
        self.assertEqual(value["response_bytes_charged"], probe.CAPS["individual_response_bytes"])

    def test_opaque_read_exception_retains_prefix_and_full_charge(self):
        class Partial(Response):
            def read(self, size):
                if self.read_bytes:
                    raise OSError("unknown consumed bytes; DO_NOT_DISCLOSE")
                self.read_bytes = 3
                return b"abc"
        value = self.run_fixture([Partial(clob_page())])
        self.assert_failed(value, "response_transport_error")
        self.assertEqual(self.body(), b"abc")
        self.assertEqual(value["response_bytes_charged"], probe.CAPS["individual_response_bytes"])

    def test_deadline_in_transport_and_mid_read_charges_full_allowance(self):
        value = self.run_fixture([probe.ProbeError("runtime_limit")])
        self.assert_failed(value, "runtime_limit")
        self.assertEqual(value["response_bytes_charged"], probe.CAPS["individual_response_bytes"])
        self.assertTrue(self.read("attempts/a01.FINISH.json")["transport_dispatched"])
        class Deadline(Response):
            def read(self, size):
                if self.read_bytes:
                    raise probe.ProbeError("runtime_limit")
                self.read_bytes = 3
                return b"abc"
        self.output = self.parent / "read_deadline"
        self.assert_failed(self.run_fixture([Deadline(clob_page())]), "runtime_limit")
        self.assertEqual(self.body(), b"abc")

    def test_elapsed_deadline_still_preserves_bounded_failure_body_and_finish(self):
        late = False
        class Deadline(Response):
            def read(self, size):
                nonlocal late
                late = True
                return b"abc"
        with patch.object(probe.time, "monotonic", side_effect=lambda: 2000 if late else 0):
            value = self.run_fixture([Deadline(clob_page())])
        self.assert_failed(value, "runtime_limit")
        self.assertEqual(self.body(), b"abc")
        self.assertEqual(self.read("attempts/a01.FINISH.json")["error_code"], "runtime_limit")
        self.assertEqual(value["response_bytes_charged"], probe.CAPS["individual_response_bytes"])

    def test_http_status_headers_redirect_and_transport_contract(self):
        cases = [(Response(clob_page(), status=401), "http_status_not_200"),
                 (Response(clob_page(), status="200"), "invalid_http_status"),
                 (Response(clob_page(), headers={"Content-Type": "text/html"}), "unexpected_content_type"),
                 (Response(clob_page(), headers={"Content-Encoding": "gzip"}), "unexpected_content_encoding"),
                 (Response(clob_page(), url="https://evil.invalid/?secret=x"), "redirect_or_response_url_changed"),
                 (Response(clob_page(), headers={"Content-Length": "-1"}), "invalid_content_length"),
                 (Response(clob_page(), headers={"Content-Length": "01"}), "invalid_content_length"),
                 (Response(clob_page(), headers={"Content-Length": str(len(clob_page()) + 1)}), "truncated_response"),
                 (Response(clob_page(), headers={"Content-Length": "1"}), "content_length_differs")]
        for index, (response, code) in enumerate(cases):
            self.output = self.parent / f"http_{index}"
            self.assert_failed(self.run_fixture([response]), code)

    def test_individual_body_cap_never_overreads_or_truncates_to_success(self):
        response = Response(b"x" * (probe.CAPS["individual_response_bytes"] + 1))
        self.assert_failed(self.run_fixture([response]), "response_byte_limit")
        self.assertEqual(response.read_bytes, 0)
        self.output = self.parent / "no_length"
        response = Response(b"x" * (probe.CAPS["individual_response_bytes"] + 1))
        del response.headers["Content-Length"]
        self.assert_failed(self.run_fixture([response]), "response_byte_limit")
        self.assertEqual(response.read_bytes, probe.CAPS["individual_response_bytes"])
        self.assertEqual(len(self.body()), probe.CAPS["individual_response_bytes"])

    def test_fresh_directory_and_source_resource_admission_before_network(self):
        self.output.mkdir()
        keep = self.output / "keep"; keep.write_bytes(b"original")
        with self.assertRaisesRegex(probe.ProbeError, "destination_exists"):
            self.run_fixture()
        self.assertEqual(keep.read_bytes(), b"original")
        self.assertFalse(self.calls)
        self.output = self.parent / "new"
        with self.assertRaisesRegex(probe.ProbeError, "source_not_clean"):
            self.run_fixture(source_effect=probe.ProbeError("source_not_clean"))
        self.assertFalse(self.output.exists())
        with patch.object(probe, "source_snapshot", return_value=SOURCE), patch.object(probe, "available_ram", return_value=1):
            with self.assertRaisesRegex(probe.ProbeError, "available_ram_below_floor"):
                probe.run_probe(self.output, HEAD, transport=self.transport)
        self.assertFalse(self.output.exists())

    def test_artifact_runtime_and_floor_caps_are_explicit(self):
        self.output.mkdir()
        writer = probe.ArtifactWriter(self.output, time.monotonic())
        with patch.object(probe.shutil, "disk_usage", return_value=FREE):
            with self.assertRaisesRegex(probe.ProbeError, "artifact_byte_limit"):
                writer.check(probe.CAPS["all_artifact_bytes"])
            writer.started -= probe.CAPS["runtime_seconds"] + 1
            with self.assertRaisesRegex(probe.ProbeError, "runtime_limit"):
                writer.check()
        self.output = self.parent / "floor"
        with patch.object(probe, "source_snapshot", return_value=SOURCE), \
                patch.object(probe, "available_ram", return_value=10**12), \
                patch.object(probe.shutil, "disk_usage", return_value=shutil._ntuple_diskusage(10**12, 0, 1)):
            with self.assertRaisesRegex(probe.ProbeError, "disk_floor"):
                probe.run_probe(self.output, HEAD, transport=self.transport)
        self.assertFalse(self.calls)

    def test_same_size_raw_and_control_mutation_block_complete(self):
        for index, relative in enumerate(("responses/a01.body.gz", "START.json", "attempts/a01.FINISH.json", "rows/a01.json")):
            self.output = self.parent / f"mutation_{index}"
            def mutate(spec, timeout):
                response = self.transport(spec, timeout)
                if len(self.calls) == 2:
                    path = self.output / relative
                    raw = bytearray(path.read_bytes()); raw[-1] ^= 1
                    path.write_bytes(raw)
                return response
            self.calls = []
            self.assert_failed(self.run_fixture(terminal_responses(), transport=mutate), "artifact_reopen_differs")

    def test_source_mutation_before_final_marker_blocks_completion(self):
        other = SOURCE | {"head": "3" * 40}
        count = 0
        def source(_):
            nonlocal count
            count += 1
            return other if count == 13 else SOURCE
        value = self.run_fixture(terminal_responses(), source_effect=source)
        self.assert_failed(value, "source_changed")
        self.assertTrue((self.output / "manifest.json").exists())

    def test_source_is_reopened_again_after_pending_marker_is_staged(self):
        other = SOURCE | {"head": "3" * 40}
        def source(_):
            return other if (self.output / ".COMPLETE.pending").exists() else SOURCE
        value = self.run_fixture(terminal_responses(), source_effect=source)
        self.assert_failed(value, "source_changed")
        self.assertTrue((self.output / ".COMPLETE.pending").exists())

    def test_marker_fsync_failure_leaves_pending_not_complete(self):
        original = probe.os.fsync
        def fault(fd):
            pending = self.output / ".COMPLETE.pending"
            if pending.exists() and os.fstat(fd).st_ino == pending.stat().st_ino:
                raise OSError("marker fsync fault")
            return original(fd)
        with patch.object(probe.os, "fsync", side_effect=fault):
            value = self.run_fixture(terminal_responses())
        self.assert_failed(value, "internal_runtime_error")
        self.assertTrue((self.output / ".COMPLETE.pending").exists())

    def test_marker_partial_write_is_not_published(self):
        original = Path.open
        class PartialWrite:
            def __init__(self, stream): self.stream = stream
            def __enter__(self): self.stream.__enter__(); return self
            def __exit__(self, *args): return self.stream.__exit__(*args)
            def write(self, raw): return self.stream.write(raw[:len(raw)//2])
            def __getattr__(self, name): return getattr(self.stream, name)
        def open_path(path, *args, **kwargs):
            stream = original(path, *args, **kwargs)
            return PartialWrite(stream) if path.name == ".COMPLETE.pending" and args == ("xb",) else stream
        with patch.object(Path, "open", open_path):
            value = self.run_fixture(terminal_responses())
        self.assert_failed(value, "artifact_short_write")
        self.assertGreater((self.output / ".COMPLETE.pending").stat().st_size, 0)

    def test_marker_flush_and_close_failures_never_publish(self):
        original = Path.open
        for index, phase in enumerate(("flush", "close")):
            self.output = self.parent / f"marker_{phase}"
            class FaultyMarker:
                def __init__(self, stream): self.stream = stream
                def __enter__(self): self.stream.__enter__(); return self
                def __exit__(self, *args):
                    result = self.stream.__exit__(*args)
                    if phase == "close": raise OSError("marker close fault")
                    return result
                def flush(self):
                    if phase == "flush": raise OSError("marker flush fault")
                    return self.stream.flush()
                def __getattr__(self, name): return getattr(self.stream, name)
            def open_path(path, *args, **kwargs):
                stream = original(path, *args, **kwargs)
                return FaultyMarker(stream) if path.name == ".COMPLETE.pending" and args == ("xb",) else stream
            with patch.object(Path, "open", open_path):
                value = self.run_fixture(terminal_responses())
            self.assert_failed(value, "internal_runtime_error")

    def test_same_size_pending_marker_mutation_during_directory_fsync_blocks_complete(self):
        original = probe.os.fsync
        changed = False
        def fault(fd):
            nonlocal changed
            if not changed and stat.S_ISDIR(os.fstat(fd).st_mode):
                path = self.output / ".COMPLETE.pending"
                raw = bytearray(path.read_bytes()); raw[-1] ^= 1
                path.write_bytes(raw)
                changed = True
            return original(fd)
        with patch.object(probe.os, "fsync", side_effect=fault):
            value = self.run_fixture(terminal_responses())
        self.assertTrue(changed)
        self.assert_failed(value, "artifact_reopen_differs")

    def test_directory_fsync_failure_and_marker_rename_failure_never_publish(self):
        original = probe.os.fsync
        def fault(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError("directory fsync fault")
            return original(fd)
        with patch.object(probe.os, "fsync", side_effect=fault):
            self.assert_failed(self.run_fixture(terminal_responses()), "internal_runtime_error")
        self.output = self.parent / "rename"
        with patch.object(probe, "atomic_completion", side_effect=probe.ProbeError("atomic_completion_refused")):
            self.assert_failed(self.run_fixture(terminal_responses()), "atomic_completion_refused")

    def test_atomic_marker_never_replaces_competing_destination(self):
        pending, destination = self.parent / "pending", self.parent / "COMPLETE.json"
        pending.write_bytes(b"new"); destination.write_bytes(b"original")
        with self.assertRaisesRegex(probe.ProbeError, "atomic_completion_refused"):
            probe.atomic_completion(pending, destination)
        self.assertEqual(destination.read_bytes(), b"original")
        self.assertEqual(pending.read_bytes(), b"new")

    def test_publication_is_last_artifact_io(self):
        original_open, original_publish = Path.open, probe.atomic_completion
        published = False
        def publish(*args):
            nonlocal published
            result = original_publish(*args)
            published = True
            return result
        def open_path(path, *args, **kwargs):
            if published:
                raise AssertionError("artifact I/O after publication")
            return original_open(path, *args, **kwargs)
        with patch.object(probe, "atomic_completion", side_effect=publish), patch.object(Path, "open", open_path):
            value = self.run_fixture(terminal_responses())
        self.assertTrue(published)
        self.assertEqual(value["status"], "probe_complete_terminal_cursors_observed")

    def test_guard_precedes_source_destination_limits_and_network(self):
        with patch.object(probe, "require_production_host", side_effect=RuntimeError("guard")), \
                patch.object(probe, "source_snapshot") as source, patch.object(probe, "process_limits") as limits, \
                patch.object(probe, "public_transport") as network, patch.object(Path, "mkdir") as mkdir:
            with self.assertRaisesRegex(RuntimeError, "guard"):
                probe.main(["--run-dir", "/mnt/data/runs/probe", "--expected-head", HEAD])
        source.assert_not_called(); limits.assert_not_called(); network.assert_not_called(); mkdir.assert_not_called()

    def test_help_has_no_endpoints_auth_caps_or_resume_overrides(self):
        result = subprocess.run([sys.executable, probe.__file__, "--help"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        for flag in ("--endpoint", "--max-", "--api-key", "--resume", "--limit", "--version"):
            self.assertNotIn(flag, result.stdout)

    def test_committed_source_blob_comparison_detects_assume_unchanged_mutation(self):
        root = self.parent / "source"; root.mkdir()
        for name in probe.SOURCE_FILES:
            path = root / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_text("fixture\n")
        subprocess.run(["git", "init", "--quiet", str(root)], check=True)
        subprocess.run(["git", "-C", str(root), "add", *probe.SOURCE_FILES], check=True)
        subprocess.run(["git", "-C", str(root), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                        "commit", "--quiet", "-m", "Fixture source"], check=True)
        head = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
        with patch.object(probe, "REPO", root):
            self.assertEqual(set(probe.source_snapshot(head)["files"]), set(probe.SOURCE_FILES))
            subprocess.run(["git", "-C", str(root), "update-index", "--assume-unchanged", "production_guard.py"], check=True)
            (root / "production_guard.py").write_text("changed\n")
            with self.assertRaisesRegex(probe.ProbeError, "source_bytes_differ_from_commit"):
                probe.source_snapshot(head)

    def test_address_space_and_alarm_are_fixed_effective_and_restored(self):
        with patch.object(probe.resource, "getrlimit", side_effect=[(-1, -1), (probe.CAPS["address_space_bytes"], -1)]), \
                patch.object(probe.resource, "setrlimit") as limits, \
                patch.object(probe.signal, "getsignal", return_value=probe.signal.SIG_DFL), \
                patch.object(probe.signal, "signal") as handlers, patch.object(probe.signal, "alarm") as alarm:
            with probe.process_limits(): pass
        self.assertEqual(limits.call_args_list[0].args[1], (8 * 1024**3, -1))
        self.assertEqual(limits.call_args_list[-1].args[1], (-1, -1))
        self.assertEqual(alarm.call_args_list[0].args, (1200,))
        self.assertEqual(alarm.call_args_list[-1].args, (0,))
        self.assertEqual(handlers.call_args_list[-1].args[1], probe.signal.SIG_DFL)

    def test_public_transport_disables_proxies_and_redirects(self):
        spec = probe.request_spec(probe.STREAMS[0], probe.INITIAL_CURSOR)
        with patch.object(probe.request, "build_opener") as build:
            probe.public_transport(spec, 30)
        self.assertEqual(build.call_args.args[0].proxies, {})
        self.assertIsInstance(build.call_args.args[1], probe.NoRedirect)
        req = build.return_value.open.call_args.args[0]
        self.assertNotIn("Authorization", req.headers)
        self.assertNotIn("Cookie", req.headers)
        self.assertIsNone(probe.NoRedirect().redirect_request(None, None, 302, "", {}, "https://evil.invalid"))


if __name__ == "__main__":
    unittest.main()
