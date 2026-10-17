import hashlib
import unittest
from datetime import date

from analysis.kaushik_kalshi_replication import source_contract as s


class SourceContractTests(unittest.TestCase):
    def trade(self, body):
        raw = ("file,bytes,sha256\n" + body).encode()
        return s.parse_trade_manifest(raw, expected_sha256=hashlib.sha256(raw).hexdigest())

    def test_exact_trade_paths_and_explicit_empty_day(self):
        digest = "a" * 64
        entries = self.trade(f"trades_all/dt=2021-06-30/part-000.parquet,123,{digest}\n"
                             f"bid_ask_1m/month=2021-06/part-000.parquet,20,{digest}\n"
                             f"trades_all/dt=2021-07-02/part-000.parquet,456,{digest}\n")
        result = s.validate_trade_days(entries, first_day=date(2021, 6, 30), last_day=date(2021, 7, 2),
                                       documented_empty_days=(date(2021, 7, 1),))
        self.assertEqual((result["files"], result["bytes"], result["maximum_shard_bytes"]), (2, 579, 456))
        with self.assertRaisesRegex(ValueError, "coverage"):
            s.validate_trade_days(entries, first_day=date(2021, 6, 30), last_day=date(2021, 7, 2), documented_empty_days=())

    def test_hash_header_duplicate_and_unadmitted_paths_block(self):
        with self.assertRaisesRegex(ValueError, "SHA"):
            s.parse_trade_manifest(b"file,bytes,sha256\n")
        for body in (f"trades_all/../x.parquet,1,{'a'*64}\n", f"quotes/new.parquet,1,{'a'*64}\n",
                     (f"trades_all/dt=2021-06-30/part-000.parquet,1,{'a'*64}\n") * 2):
            with self.subTest(body=body), self.assertRaises(ValueError):
                self.trade(body)
        raw = b"bytes,file,sha256\n1,x,y\n"
        with self.assertRaisesRegex(ValueError, "header"):
            s.parse_trade_manifest(raw, expected_sha256=hashlib.sha256(raw).hexdigest())

    def test_metadata_bijective_mapping_and_rows(self):
        raw = (f"path,bytes,rows,sha256\n{s.METADATA_PREFIX}f2_0.parquet,200,17,{'b'*64}\n").encode()
        entries, counts = s.parse_metadata_manifest(raw, expected_sha256=hashlib.sha256(raw).hexdigest())
        self.assertEqual((entries[0].path, counts[entries[0].path]), ("markets_metadata/f2_0.parquet", 17))
        for path in (s.METADATA_PREFIX + "../f2_0.parquet", "other/f2_0.parquet", s.METADATA_PREFIX + "new.parquet"):
            raw = (f"path,bytes,rows,sha256\n{path},200,17,{'b'*64}\n").encode()
            with self.subTest(path=path), self.assertRaises(ValueError):
                s.parse_metadata_manifest(raw, expected_sha256=hashlib.sha256(raw).hexdigest())


if __name__ == "__main__":
    unittest.main()
