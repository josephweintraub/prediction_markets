"""Seven-word synthetic wire fixtures and in-memory collector roundtrips only."""
from __future__ import annotations

import copy
import unittest

from pipeline import decode_polymarket_v3_fill_v1 as decoder
from pipeline import normalize_polymarket_v3_archive_v1 as archive


def word(number):
    return f"{number:064x}"


class DecoderTests(unittest.TestCase):
    def setUp(self):
        self.admission = {"schema_version": archive.SCHEMA, "status": "root_accepted_exchange_builds_and_era",
            "chain_id": 137, "exchange_address": archive.EXCHANGE,
            "build_receipt": {"path": "/mnt/data/runs/fixture_build/RESULT.json", "bytes": 1234, "sha256": "a"*64},
            "builds": copy.deepcopy(archive.BUILDS), "upgrade": copy.deepcopy(archive.UPGRADE)}
        self.token = (1 << 248) | 0x123400
        self.values = (0, self.token, 250000, 1000000, 123, int("66"*32, 16), int("77"*32, 16))
        self.row = {"address": archive.EXCHANGE, "blockHash": "0x"+"22"*32, "blockNumber": hex(88978537),
            "data": "0x"+"".join(word(number) for number in self.values), "logIndex": "0xa", "removed": False,
            "topics": [decoder.TOPIC, "0x"+"33"*32, "0x"+"0"*24+"44"*20, "0x"+"0"*24+"55"*20],
            "transactionHash": "0x"+"11"*32, "transactionIndex": "0x1", "blockTimestamp": None,
            "block_number": 88978537, "log_index": 10, "transaction_index": 1,
            "exchange_address": archive.EXCHANGE, "window": "b000088978500-000088978599",
            "source_request_id": 2**32, "provider_block_timestamp_present": False}

    def decode(self, row=None, admission=None):
        return decoder.decode_native_row(self.row if row is None else row, self.admission if admission is None else admission)

    def reject(self, code, row):
        with self.assertRaisesRegex(decoder.DecodeError, "^"+code+"$"):
            self.decode(row)

    def data(self, values):
        return "0x"+"".join(word(number) for number in values)

    def test_old_and_new_wire_declarations_match_exact_signature_and_collector_topic(self):
        # Literal source/ABI evidence: old Exchange.sol145..156 and new148..159.
        old = (("orderHash", "bytes32", True), ("maker", "address", True), ("taker", "address", True),
            ("side", "uint8", False), ("tokenId", "uint256", False), ("makerAmountFilled", "uint256", False),
            ("takerAmountFilled", "uint256", False), ("fee", "uint256", False),
            ("builder", "bytes32", False), ("metadata", "bytes32", False))
        new = (("orderHash", "bytes32", True), ("maker", "address", True), ("taker", "address", True),
            ("side", "uint8", False), ("tokenId", "uint256", False), ("makerAmountFilled", "uint256", False),
            ("takerAmountFilled", "uint256", False), ("fee", "uint256", False),
            ("builder", "bytes32", False), ("metadata", "bytes32", False))
        self.assertEqual(decoder.WIRE_FIELDS, old)
        self.assertEqual(old, new)
        self.assertEqual(decoder.SIGNATURE, "OrderFilled("+",".join(field[1] for field in old)+")")
        from scripts import probe_polymarket_increment_capacity as native
        self.assertEqual(decoder.TOPIC, native.MODERN_TOPIC)
        self.assertEqual(decoder.SOURCE_EVIDENCE["old"]["source_package_sha256"], "2fa08be23b657b7071abbf89d0e482146a93b521e97142d29320094f174241c0")
        self.assertEqual(decoder.SOURCE_EVIDENCE["new"]["source_package_sha256"], "47e25bfebfe8082f8edf183e401c28307c8ef9eadf748c16494d2dbf1fdc5bf9")

    def test_topic_keccak_matches_full_signature_when_fixture_library_available(self):
        try:
            from Crypto.Hash import keccak
        except ImportError:
            self.skipTest("Optional fixture Keccak library unavailable; fixed source/collector topic still tested")
        value = keccak.new(digest_bits=256)
        value.update(decoder.SIGNATURE.encode())
        self.assertEqual("0x"+value.hexdigest(), decoder.TOPIC)

    def test_hex_case_preserves_raw_lexemes_while_decoded_bytes_are_canonical(self):
        row = {**self.row, "address": "0x"+self.row["address"][2:].upper(),
            "blockHash": "0x"+"AB"*32, "transactionHash": "0x"+"CD"*32,
            "data": "0x"+self.row["data"][2:].upper(),
            "topics": ["0x"+topic[2:].upper() for topic in self.row["topics"]]}
        result = self.decode(row)
        self.assertEqual(result["provenance"]["raw_native_row"], row)
        self.assertEqual(result["provenance"]["raw_data_words"][1], "0x"+word(self.token).upper())
        self.assertEqual(result["event"]["transaction_hash"], "0x"+"cd"*32)
        self.assertEqual(result["event"]["block_hash"], "0x"+"ab"*32)

    def test_all_seven_words_tags_hashes_and_collector_fields_retained_losslessly(self):
        result = self.decode(); event, provenance = result["event"], result["provenance"]
        self.assertEqual(tuple(int(value,16) for value in provenance["raw_data_words"]), self.values)
        self.assertEqual(event["builder"], "0x"+"66"*32)
        self.assertEqual(event["metadata"], "0x"+"77"*32)
        self.assertEqual(event["fee"], 123)
        self.assertEqual(event["order_hash"], self.row["topics"][1])
        self.assertEqual(provenance["raw_native_row"], self.row)
        self.assertEqual(set(event), archive.EVENT_FIELDS)
        self.assertEqual(provenance["native_identity"], (archive.EXCHANGE, self.row["transactionHash"], 10))
        self.row["topics"][1] = "changed after return"
        self.assertEqual(provenance["raw_native_row"]["topics"][1], "0x"+"33"*32)
        self.assertTrue(result["decoding_only"])
        self.assertTrue(all(result[field] is False for field in archive.SCOPE))

    def test_uint256_not_float_uint64_or_decimal128_truncated(self):
        values = (1, archive.UINT256, archive.UINT256, 2**255+7, archive.UINT256, archive.UINT256, 1)
        event = self.decode({**self.row, "data": self.data(values)})["event"]
        for field,index in (("side",0),("token_id",1),("maker_amount_filled",2),("taker_amount_filled",3),("fee",4)):
            self.assertIs(type(event[field]),int)
            self.assertEqual(event[field],values[index])
        self.assertEqual(event["builder"], "0x"+"f"*64)
        self.assertEqual(event["metadata"], "0x"+word(1))

    def test_actual_collector_parquet_row_representation_roundtrips_in_memory(self):
        from scripts import collect_polymarket_native_increment as native
        import pyarrow as pa
        import pyarrow.parquet as pq
        raw = native.parquet_payload([self.row])
        table = pq.read_table(pa.BufferReader(raw))
        self.assertEqual(set(table.column_names), decoder.ROW_FIELDS)
        self.assertEqual(str(table.schema.field("source_request_id").type), "uint64")
        reopened = table.to_pylist()[0]
        self.assertEqual(reopened, self.row)
        self.assertEqual(self.decode(reopened)["provenance"]["raw_native_row"]["source_request_id"],2**32)

    def test_provider_timestamp_is_opaque_provenance_never_an_execution_fallback(self):
        row = {**self.row, "blockTimestamp": "provider-text-not-a-clock", "provider_block_timestamp_present": True}
        result = self.decode(row)
        self.assertIsNone(result["event"]["block_timestamp"])
        self.assertEqual(result["provenance"]["raw_native_row"]["blockTimestamp"],row["blockTimestamp"])
        self.assertFalse(result["provenance"]["provider_timestamp_used_as_execution_clock"])
        self.reject("provider_timestamp_presence_differs", {**self.row, "blockTimestamp": "0x123"})
        self.reject("provider_timestamp_presence_differs", {**self.row, "provider_block_timestamp_present": True})
        self.reject("provider_timestamp_presence_schema", {**self.row, "provider_block_timestamp_present": 1})

    def test_buy_sell_decode_integrates_all_archive_roles(self):
        token = {"token_id": str(self.token), "complement_token_id": str(self.token+1), "market_id": "fixture",
                 "outcome": "Red", "winning_outcome": "Red", "event_slug": "event", "resolved": True}
        other = {**token, "token_id": str(self.token+1), "complement_token_id": str(self.token), "outcome": "Blue"}
        for side in (0,1):
            row = {**self.row, "data": self.data((side,self.token,250000 if side==0 else 1000000,
                                                1000000 if side==0 else 250000,123,self.values[5],self.values[6]))}
            event = self.decode(row)["event"]
            normalized = archive.normalize_event(event,self.admission,[token],[other],
                [{"block_number":event["block_number"],"block_hash":event["block_hash"],"timestamp":1782172801}])
            self.assertEqual(normalized["reason"],"eligible")
            maker,taker = normalized["rows"]
            self.assertEqual((maker["side"],taker["side"]),("BUY","SELL") if side==0 else ("SELL","BUY"))
            self.assertEqual((maker["price"],taker["price"]),(0.25,0.25))
            self.assertEqual(archive.primary_taker_rows(normalized)[0]["proxyWallet"],"0x"+"55"*20)

    def test_new_era_requires_exact_accepted_boundary_not_word_count_guess(self):
        row = {**self.row,"blockNumber":hex(93452332),"block_number":93452332,
               "blockHash":archive.UPGRADE["block_hash"],"transactionHash":archive.UPGRADE["transaction_hash"],
               "transactionIndex":"0x3e","transaction_index":62,"logIndex":hex(753),"log_index":753}
        self.assertEqual(self.decode(row)["provenance"]["source_era"],"new")
        self.assertEqual(self.decode({**row,"logIndex":hex(751),"log_index":751})["provenance"]["source_era"],"old")
        self.reject("impossible_upgrade_locator",{**row,"logIndex":hex(752),"log_index":752})
        changed=copy.deepcopy(self.admission);changed["builds"]["new"]["runtime_sha256"]="0"*64
        with self.assertRaises(archive.AdmissionError):self.decode(admission=changed)

    def test_wrong_topic_count_signature_padding_or_wire_length_rejected(self):
        self.reject("order_filled_topic_schema",{**self.row,"topics":self.row["topics"][:3]})
        self.reject("unsupported_order_filled_wire_topic",{**self.row,"topics":["0x"+"00"*32,*self.row["topics"][1:]]})
        for index in (2,3):
            topics=list(self.row["topics"]);topics[index]="0x01"+topics[index][4:]
            self.reject("noncanonical_indexed_address_padding",{**self.row,"topics":topics})
        for data in (self.data(self.values[:5]),self.data((*self.values,0)),self.row["data"]+"00",self.row["data"][:-1],"0x"+"z"*448):
            self.reject("order_filled_seven_word_schema",{**self.row,"data":data})
        for value in (2,256,2**255):
            self.reject("noncanonical_side_enum_or_padding",{**self.row,"data":self.data((value,*self.values[1:]))})

    def test_wrong_emitter_removed_boolean_or_numeric_shape_rejected(self):
        self.reject("unsupported_emitting_exchange",{**self.row,"address":"0x"+"99"*20})
        self.reject("collector_exchange_address_differs",{**self.row,"exchange_address":"0x"+"99"*20})
        for value in (True,None,0):self.reject("removed_native_log",{**self.row,"removed":value})
        for field in ("block_number","log_index","transaction_index","source_request_id"):
            for value in (True,1.0,-1,2**64):self.reject("collector_uint64_schema",{**self.row,field:value})
        for field in ("blockNumber","logIndex","transactionIndex"):
            for value in (True,1,"0x01","0X1","0xA"):
                self.reject("rpc_quantity_schema",{**self.row,field:value})

    def test_raw_cached_identity_fields_and_unknown_columns_never_silently_disagree(self):
        for field in ("block_number","log_index","transaction_index"):
            self.reject("raw_cached_native_locator_differs",{**self.row,field:self.row[field]+1})
        for field in ("blockHash","transactionHash"):
            self.reject("native_hash_schema",{**self.row,field:"0x1234"})
        self.reject("native_parquet_row_schema",{**self.row,"unknown_version":"future"})
        self.reject("native_parquet_row_schema",{key:value for key,value in self.row.items() if key!="window"})
        self.reject("collector_window_schema",{**self.row,"window":""})

    def test_repeated_order_hashes_at_different_logs_remain_separate(self):
        first = self.decode()
        second = self.decode({**self.row,"logIndex":"0xb","log_index":11})
        self.assertEqual(first["event"]["order_hash"],second["event"]["order_hash"])
        self.assertNotEqual(first["provenance"]["native_identity"],second["provenance"]["native_identity"])
        self.assertEqual(archive.native_relation(first["event"],second["event"]),"different_native_identity")


if __name__ == "__main__":
    unittest.main()
