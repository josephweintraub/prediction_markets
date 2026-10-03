from __future__ import annotations

from pathlib import Path
from collections import namedtuple
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.diagnostics.build_profit_taking_ledger import (
    HISTORY_STATUS, SOURCE_COLUMNS, build_ledger, parse_args, verify_native_readiness,
)
from analysis.diagnostics.profit_taking_actions import EXCHANGE_ADDRESSES, V2_EXCHANGE_ADDRESSES
from analysis.sports_game_dynamics.artifacts import artifact_fingerprint


EXCHANGE = sorted(EXCHANGE_ADDRESSES)[0]


def own_row(
    side: str, token: str, block: int, cash: int, quantity: int = 1_000_000,
    *, wallet: str = "wallet", market: str = "market", tx: str | None = None,
    log: int = 1, fee: int = 0, aggregate: bool = False, refund: int = 0,
) -> dict:
    tx = tx or f"tx-{block}"
    maker_amount = cash if side == "BUY" else quantity
    taker_amount = quantity if side == "BUY" else cash
    return {
        "order_hash": f"order-{block}-{log}", "maker": wallet,
        "taker": EXCHANGE if aggregate else "other",
        "maker_asset_id": "0" if side == "BUY" else token,
        "taker_asset_id": token if side == "BUY" else "0",
        "maker_amount_filled": maker_amount, "taker_amount_filled": taker_amount,
        "fee": fee, "block_number": block, "transaction_hash": tx,
        "log_index": log, "exchange_address": EXCHANGE,
        "execution_id": f"{EXCHANGE}:{tx}:{log}", "market_id": market,
        "source_role": "active_aggregate" if aggregate else "passive",
        "source_status": "verified_own_action", "source_contract_version": "legacy_reserved_making_v1",
        "fee_rule": "received_asset", "aggregate_reconciled": aggregate,
        "original_maker_amount_filled": maker_amount+refund,
        "original_taker_amount_filled": taker_amount, "refund_making_micro": refund,
    }


def fixtures(tmp_path: Path, rows: list[dict], tokens: list[dict] | None = None):
    own, spine = tmp_path/"own_actions.parquet", tmp_path/"tokens.parquet"
    pq.write_table(pa.Table.from_pylist(rows), own)
    pq.write_table(pa.Table.from_pylist(tokens or [
        {"market_id": "market", "token_id": "1", "complement_token_id": "2"},
        {"market_id": "market", "token_id": "2", "complement_token_id": "1"},
    ]), spine)
    source_manifest = tmp_path/"manifest.json"
    source_manifest.write_text(json.dumps({
        "status": "complete", "counts": {"accepted_own_actions": len(rows),
        "rejected_relevant_batches": 0, "orphan_scoped_logs": 0},
        "outputs": {"own_actions.parquet": artifact_fingerprint(own)},
    }))
    args = parse_args(["--own-actions", str(own), "--market-tokens", str(spine),
                       "--source-manifest", str(source_manifest),
                       "--run-dir", str(tmp_path/"run"), "--threads", "1",
                       "--memory-limit", "1GB", "--batch-size", "2"])
    return args


def read_rows(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()


def test_runner_preserves_own_fill_grain_fifo_and_exact_component_fractions(tmp_path: Path) -> None:
    rows = [
        own_row("BUY", "1", 1, 400_000, quantity=2_000_000),
        own_row("BUY", "2", 2, 100_000),
        own_row("SELL", "1", 3, 1_800_000, quantity=2_000_000),
        own_row("SELL", "1", 4, 900_000),
    ]
    args = fixtures(tmp_path, list(reversed(rows)))
    manifest = build_ledger(args)
    assert manifest["completion_status"] == "complete"
    assert manifest["counts"]["input_actions"] == manifest["counts"]["output_actions"] == 4
    saved = read_rows(tmp_path/"run"/"action_tags.parquet")
    assert len(saved) == 4
    assert [row["block_number"] for row in saved] == [1, 2, 3, 4]
    assert saved[1]["hedge_profitable_quantity_micro"] == 1_000_000
    assert saved[1]["hedge_fraction"] == 1
    assert saved[1]["matched_profit_usdc"] == pytest.approx(0.7)
    assert saved[2]["gross_profitable_disposal_quantity_micro"] == 2_000_000
    assert saved[2]["primary_exit_quantity_micro"] == 1_000_000
    assert saved[2]["primary_exit_fraction_numerator"] == 1
    assert saved[2]["primary_exit_fraction_denominator"] == 2
    assert saved[2]["minimum_matched_holding_blocks"] == 2
    assert saved[3]["unmatched_disposal_quantity_micro"] == 1_000_000
    assert saved[3]["unmatched_disposal_fraction"] == 1
    assert all(row["history_status"] == HISTORY_STATUS for row in saved)
    with pytest.raises(FileExistsError):
        build_ledger(args)


def test_runner_fee_adjusted_hedge_fraction_is_net_over_net(tmp_path: Path) -> None:
    args = fixtures(tmp_path, [
        own_row("BUY", "1", 1, 1, quantity=3),
        own_row("BUY", "2", 2, 2, quantity=8, fee=1),
    ])
    build_ledger(args)
    hedge = read_rows(tmp_path/"run"/"action_tags.parquet")[1]
    assert hedge["gross_quantity_micro"] == 8
    assert hedge["net_acquired_quantity_micro"] == 7
    assert hedge["hedge_fraction_numerator"] == 3
    assert hedge["hedge_fraction_denominator"] == 7
    assert hedge["matched_profit_usdc"] == pytest.approx(8/7/1_000_000)
    assert hedge["unmatched_disposal_quantity_micro"] == 0


def test_runner_dispatches_verified_v2_cash_fee_and_preserves_gross_price(tmp_path: Path) -> None:
    exchange = sorted(V2_EXCHANGE_ADDRESSES)[0]
    rows = [own_row("BUY", "1", 1, 8, quantity=10, fee=1),
            own_row("SELL", "1", 2, 9, quantity=10, fee=1)]
    for row in rows:
        row.update(exchange_address=exchange, source_contract_version="ctf_exchange_v2_v1",
                   fee_rule="collateral_extra_buy",
                   execution_id=f"{exchange}:{row['transaction_hash']}:{row['log_index']}")
    args = fixtures(tmp_path, rows)
    build_ledger(args)
    saved = read_rows(tmp_path/"run"/"action_tags.parquet")
    assert saved[0]["gross_cash_micro"] == 8
    assert saved[0]["net_acquired_quantity_micro"] == 10
    assert saved[0]["acquisition_cash_micro"] == 9
    assert saved[1]["net_sale_cash_micro"] == 8
    assert saved[1]["matched_profit_usdc"] == pytest.approx(-1/1_000_000)
    assert saved[1]["primary_exit_fraction"] == 0


def test_runner_rejects_v2_rows_mislabeled_as_legacy_even_at_zero_fee(tmp_path: Path) -> None:
    row = own_row("BUY", "1", 1, 400_000)
    exchange = sorted(V2_EXCHANGE_ADDRESSES)[0]
    row.update(exchange_address=exchange, execution_id=f"{exchange}:tx-1:1")
    args = fixtures(tmp_path, [row])
    with pytest.raises(ValueError, match="Source version/fee rule"):
        build_ledger(args)


def test_runner_does_not_label_fresh_buys_unknown_disposals(tmp_path: Path) -> None:
    args = fixtures(tmp_path, [own_row("BUY", "2", 1, 100_000)])
    build_ledger(args)
    saved = read_rows(tmp_path/"run"/"action_tags.parquet")[0]
    assert saved["hedge_fraction"] == 0
    assert saved["unmatched_quantity_micro"] == 1_000_000
    assert saved["unmatched_disposal_quantity_micro"] == 0
    assert saved["unmatched_disposal_fraction"] == 0


def test_same_transaction_history_is_updated_without_primary_tag(tmp_path: Path) -> None:
    args = fixtures(tmp_path, [
        own_row("BUY", "1", 1, 400_000, tx="same", log=1),
        own_row("SELL", "1", 1, 900_000, tx="same", log=2),
        own_row("SELL", "1", 2, 900_000),
    ])
    build_ledger(args)
    saved = read_rows(tmp_path/"run"/"action_tags.parquet")
    assert saved[1]["matched_quantity_micro"] == 1_000_000
    assert saved[1]["prior_matched_quantity_micro"] == 0
    assert saved[1]["primary_exit_fraction"] == 0
    assert saved[2]["unmatched_disposal_fraction"] == 1


def test_runner_corrected_active_aggregate_is_one_original_action(tmp_path: Path) -> None:
    args = fixtures(tmp_path, [
        own_row("BUY", "1", 1, 400_000, aggregate=True, refund=100_000),
        own_row("SELL", "1", 2, 900_000),
    ])
    result = build_ledger(args)
    saved = read_rows(tmp_path/"run"/"action_tags.parquet")
    assert result["counts"]["output_actions"] == 2
    assert saved[0]["source_role"] == "active_aggregate"
    assert saved[0]["gross_cash_micro"] == 400_000
    assert saved[1]["primary_exit_fraction"] == 1


def test_runner_resets_book_per_market_and_keeps_boundary_history(tmp_path: Path) -> None:
    tokens = [
        {"market_id": market, "token_id": token, "complement_token_id": other}
        for market, token, other in (("a", "1", "2"), ("a", "2", "1"),
                                     ("b", "3", "4"), ("b", "4", "3"))
    ]
    args = fixtures(tmp_path, [
        own_row("BUY", "1", 3, 0, market="a", wallet="bot"),
        own_row("SELL", "1", 4, 900_000, market="a", wallet="bot"),
        own_row("SELL", "3", 1, 900_000, market="b", wallet="bot"),
        own_row("BUY", "4", 2, 1_000_000, market="b", wallet="bot"),
    ], tokens)
    build_ledger(args)
    saved = read_rows(tmp_path/"run"/"action_tags.parquet")
    assert len(saved) == 4
    assert saved[1]["primary_exit_fraction"] == 1
    assert saved[2]["unmatched_disposal_fraction"] == 1
    assert saved[3]["net_acquired_quantity_micro"] == 1_000_000


@pytest.mark.parametrize("failure", [
    "fee_rule", "source_version", "source_status", "role", "unreconciled", "id",
    "duplicate", "evm_order", "refund", "taking", "integer_units", "wrong_market",
    "missing_token", "asymmetric_token", "third_token", "wrong_aggregate_exchange",
])
def test_runner_input_gates_fail_closed_and_do_not_publish(tmp_path: Path, failure: str) -> None:
    row = own_row("BUY", "1", 1, 400_000)
    rows, tokens = [row], None
    if failure == "fee_rule": row["fee_rule"] = "unknown"
    if failure == "source_version": row["source_contract_version"] = "unverified"
    if failure == "source_status": row["source_status"] = "missing_legs"
    if failure == "role": row["source_role"] = "counterparty_inferred"
    if failure == "unreconciled": row["aggregate_reconciled"] = True
    if failure == "id": row["execution_id"] = "fabricated-id"
    if failure == "duplicate": rows.append(dict(row))
    if failure == "evm_order":
        extra = own_row("BUY", "1", 1, 400_000, tx="another")
        rows.append(extra)
    if failure == "refund": row["refund_making_micro"] = 1
    if failure == "taking": row["original_taker_amount_filled"] += 1
    if failure == "integer_units": row["fee"] = 0.0
    if failure == "wrong_market": row["market_id"] = "another-market"
    if failure in ("missing_token", "asymmetric_token", "third_token"):
        tokens = [
            {"market_id": "market", "token_id": "1", "complement_token_id": "2"},
            {"market_id": "market", "token_id": "2", "complement_token_id": "1"},
        ]
        if failure == "missing_token": tokens.pop()
        if failure == "asymmetric_token": tokens[1]["complement_token_id"] = "3"
        if failure == "third_token": tokens.append({"market_id":"market", "token_id":"3", "complement_token_id":"1"})
    if failure == "wrong_aggregate_exchange":
        row = own_row("BUY", "1", 1, 400_000, aggregate=True)
        row["taker"] = next(address for address in EXCHANGE_ADDRESSES if address != EXCHANGE)
        rows = [row]
    args = fixtures(tmp_path, rows, tokens)
    with pytest.raises(ValueError):
        build_ledger(args)
    assert not (tmp_path/"run").exists()
    assert not list(tmp_path.glob(".run.staging-*"))


def test_metadata_outcomes_are_not_inputs_to_exit_classification(tmp_path: Path) -> None:
    rows = [own_row("BUY", "1", 1, 400_000), own_row("SELL", "1", 2, 900_000)]
    for row in rows:
        row.update(won=False, winning_outcome="adversarial-future-label")
    args = fixtures(tmp_path, rows)
    build_ledger(args)
    assert read_rows(tmp_path/"run"/"action_tags.parquet")[1]["primary_exit_fraction"] == 1
    assert "won" not in SOURCE_COLUMNS


def test_low_disk_space_fails_before_stage_publication(tmp_path: Path, monkeypatch) -> None:
    args = fixtures(tmp_path, [own_row("BUY", "1", 1, 400_000)])
    usage = namedtuple("usage", "total used free")
    monkeypatch.setattr("analysis.diagnostics.build_profit_taking_ledger.shutil.disk_usage",
                        lambda _: usage(20*1024**3, 19*1024**3, 1024**3))
    with pytest.raises(ValueError, match="Insufficient disk reserve"):
        build_ledger(args)
    assert not (tmp_path/"run").exists()
    assert not list(tmp_path.glob(".run.staging-*"))


@pytest.mark.parametrize("failure", [
    "blocked", "rejected_batch", "scoped_orphan", "count", "hash", "path", "missing_count",
])
def test_parent_source_stage_cannot_be_bypassed_with_accepted_subset(tmp_path: Path, failure: str) -> None:
    args = fixtures(tmp_path, [own_row("BUY", "1", 1, 400_000)])
    manifest = json.loads(args.source_manifest.read_text())
    if failure == "blocked": manifest["status"] = "blocked_source_reconciliation"
    if failure == "rejected_batch": manifest["counts"]["rejected_relevant_batches"] = 1
    if failure == "scoped_orphan": manifest["counts"]["orphan_scoped_logs"] = 1
    if failure == "count": manifest["counts"]["accepted_own_actions"] = 2
    if failure == "hash": manifest["outputs"]["own_actions.parquet"]["sha256"] = "0"*64
    if failure == "missing_count": del manifest["counts"]["rejected_relevant_batches"]
    args.source_manifest.write_text(json.dumps(manifest))
    if failure == "path":
        other = tmp_path/"other-stage"; other.mkdir()
        copied = other/"manifest.json"; copied.write_text(json.dumps(manifest))
        args.source_manifest = copied
    with pytest.raises(ValueError):
        build_ledger(args)
    assert not (tmp_path/"run").exists()
    assert not list(tmp_path.glob(".run.staging-*"))


def ledger_cli_arguments() -> list[str]:
    return ["--own-actions", "/mnt/data/source/own_actions.parquet",
            "--market-tokens", "/mnt/data/tokens.parquet",
            "--source-manifest", "/mnt/data/source/manifest.json",
            "--source-audit-manifest", "/mnt/data/source-audit/manifest.json",
            "--run-dir", "/mnt/data/runs/new-ledger"]


def test_ledger_cli_requires_shared_production_guard_before_build(monkeypatch) -> None:
    import analysis.diagnostics.build_profit_taking_ledger as runner
    def refuse() -> None:
        raise RuntimeError("canonical EC2 environment")
    monkeypatch.setattr(runner, "require_production_host", refuse)
    monkeypatch.setattr(runner, "build_ledger", lambda args: pytest.fail("Build must not run"))
    with pytest.raises(RuntimeError, match="canonical EC2 environment"):
        runner.main(ledger_cli_arguments())


def test_ledger_cli_rejects_noncanonical_script_even_after_shared_guard(monkeypatch) -> None:
    import analysis.diagnostics.build_profit_taking_ledger as runner
    monkeypatch.setattr(runner, "require_production_host", lambda: None)
    monkeypatch.setattr(runner, "__file__", "/tmp/noncanonical/build_profit_taking_ledger.py")
    monkeypatch.setattr(runner, "build_ledger", lambda args: pytest.fail("Build must not run"))
    with pytest.raises(RuntimeError, match="canonical EC2 checkout"):
        runner.main(ledger_cli_arguments())


def test_ledger_cli_canonical_host_calls_build(monkeypatch, capsys) -> None:
    import analysis.diagnostics.build_profit_taking_ledger as runner
    monkeypatch.setattr(runner, "require_production_host", lambda: None)
    monkeypatch.setattr(runner, "__file__", "/home/ubuntu/prediction_markets/analysis/diagnostics/build_profit_taking_ledger.py")
    # Linux /home has no macOS /System/Volumes/Data symlink normalization.
    actual_resolve=runner.Path.resolve
    monkeypatch.setattr(runner.Path, "resolve", lambda self: self if str(self)==runner.__file__ else actual_resolve(self))
    monkeypatch.setattr(runner, "build_ledger", lambda args: {"counts": {"output_actions": 2}})
    monkeypatch.setattr(runner, "verify_native_readiness", lambda *args: {"status":"verified_no_observed_merge"})
    runner.main(ledger_cli_arguments())
    assert json.loads(capsys.readouterr().out) == {"output_actions": 2}


def native_stage_fixtures(args, *, merge: bool):
    audit=args.source_manifest.parent/'audit'; audit.mkdir()
    summary={"status":"pending_native_merge_receipts" if merge else "complete_no_merge_native_gate_not_applicable",
             "source_status":"complete","counts":json.loads(args.source_manifest.read_text())['counts'],
             "merge_observed_addresses":[EXCHANGE] if merge else [],
             "merge_selected_transactions":1 if merge else 0,
             "accepted_match_support":[{"exchange_address":EXCHANGE,"kind":"MERGE" if merge else "NORMAL","legs":1}]}
    (audit/'summary.json').write_text(json.dumps(summary))
    if merge:
        pq.write_table(pa.Table.from_pylist([{"transaction_hash":"tx-merge","exchange_address":EXCHANGE,
            "status":"accepted","merge_legs":1}]),audit/'batch_audit.parquet')
        pq.write_table(pa.table({"raw_marker":[1]}),audit/'raw_pilot.parquet')
    from analysis.sports_game_dynamics.artifacts import fingerprint
    manifest={"status":summary['status'],"inputs":{"source_manifest":fingerprint(args.source_manifest)},
              "outputs":{p.name:artifact_fingerprint(p) for p in audit.iterdir()}}
    args.source_audit_manifest=audit/'manifest.json'
    args.source_audit_manifest.write_text(json.dumps(manifest))
    if merge:
        receipts=args.source_manifest.parent/'receipts'; receipts.mkdir()
        native={"requested_transactions":1,"statuses":{"verified":1},
                "evidence":[{"transaction_hash":"tx-merge","status":"verified"}],
                "merge_native_gate_status":"verified","required_merge_exchange_addresses":[EXCHANGE],
                "verified_merge_exchange_addresses":[EXCHANGE]}
        (receipts/'summary.json').write_text(json.dumps(native))
        (receipts/'native_receipts.json').write_text(json.dumps({"tx-merge":{}}))
        receipt_manifest={"status":"complete","inputs":{"pilot_manifest":fingerprint(args.source_audit_manifest)},
                          "outputs":{p.name:artifact_fingerprint(p) for p in receipts.iterdir()}}
        args.merge_receipt_manifest=receipts/'manifest.json'
        args.merge_receipt_manifest.write_text(json.dumps(receipt_manifest))
    return args


@pytest.mark.parametrize('merge',[False,True])
def test_ledger_saves_verified_native_gate_and_input_lineage(tmp_path,merge):
    args=native_stage_fixtures(fixtures(tmp_path,[own_row('BUY','1',1,400_000)]),merge=merge)
    manifest=build_ledger(args)
    assert manifest['native_source_gates']['status']==('verified_native_merge' if merge else 'verified_no_observed_merge')
    assert 'source_audit_manifest' in manifest['inputs']
    assert ('merge_receipt_manifest' in manifest['inputs'])==merge


@pytest.mark.parametrize('failure',[
    'blocked_source','audit_hash','source_lineage','missing_receipts','receipt_lineage','receipt_hash',
    'failed_receipt','missing_address','different_transaction','raw_pilot_hash','native_receipt_artifact',
    'audit_contradiction',
])
def test_native_readiness_cannot_be_bypassed_with_foreign_or_incomplete_evidence(tmp_path,failure):
    from analysis.sports_game_dynamics.artifacts import fingerprint
    args=native_stage_fixtures(fixtures(tmp_path,[own_row('BUY','1',1,400_000)]),merge=True)
    audit=json.loads(args.source_audit_manifest.read_text())
    receipts=json.loads(args.merge_receipt_manifest.read_text())
    native_path=args.merge_receipt_manifest.parent/'summary.json'
    native=json.loads(native_path.read_text())
    if failure=='blocked_source':
        source=json.loads(args.source_manifest.read_text()); source['status']='blocked_source_reconciliation'
        args.source_manifest.write_text(json.dumps(source))
    if failure=='audit_hash': audit['outputs']['summary.json']['sha256']='0'*64
    if failure=='source_lineage': audit['inputs']['source_manifest']['sha256']='0'*64
    if failure=='missing_receipts': args.merge_receipt_manifest=None
    if failure=='receipt_lineage': receipts['inputs']['pilot_manifest']['sha256']='0'*64
    if failure=='receipt_hash': receipts['outputs']['summary.json']['sha256']='0'*64
    if failure=='failed_receipt': native['evidence'][0]['status']='unavailable'
    if failure=='missing_address': native['verified_merge_exchange_addresses']=[]
    if failure=='different_transaction': native['evidence'][0]['transaction_hash']='other-tx'
    if failure=='raw_pilot_hash': audit['outputs']['raw_pilot.parquet']['sha256']='0'*64
    if failure=='native_receipt_artifact': receipts['outputs']['native_receipts.json']['sha256']='0'*64
    if failure=='audit_contradiction':
        path=args.source_audit_manifest.parent/'summary.json'
        summary=json.loads(path.read_text()); summary['merge_observed_addresses']=[]
        path.write_text(json.dumps(summary)); audit['outputs']['summary.json']=artifact_fingerprint(path)
    args.source_audit_manifest.write_text(json.dumps(audit))
    if args.merge_receipt_manifest is not None:
        if failure not in ('receipt_lineage','blocked_source','source_lineage','audit_hash'):
            receipts['inputs']['pilot_manifest']=fingerprint(args.source_audit_manifest)
        if failure in ('failed_receipt','missing_address','different_transaction'):
            native_path.write_text(json.dumps(native))
            receipts['outputs']['summary.json']=artifact_fingerprint(native_path)
        args.merge_receipt_manifest.write_text(json.dumps(receipts))
    with pytest.raises(ValueError):
        build_ledger(args)
    assert not (tmp_path/'run').exists()


def test_production_cli_requires_saved_source_audit_before_build(monkeypatch):
    import analysis.diagnostics.build_profit_taking_ledger as runner
    monkeypatch.setattr(runner,'require_production_host',lambda:None)
    monkeypatch.setattr(runner,'__file__','/home/ubuntu/prediction_markets/analysis/diagnostics/build_profit_taking_ledger.py')
    actual_resolve=runner.Path.resolve
    monkeypatch.setattr(runner.Path,'resolve',lambda self:self if str(self)==runner.__file__ else actual_resolve(self))
    monkeypatch.setattr(runner,'build_ledger',lambda args:pytest.fail('No build before required audit'))
    arguments=ledger_cli_arguments()
    index=arguments.index('--source-audit-manifest'); del arguments[index:index+2]
    with pytest.raises(ValueError,match='requires --source-audit-manifest'):
        runner.main(arguments)
