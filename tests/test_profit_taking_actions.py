from __future__ import annotations

from dataclasses import replace
from collections import deque
from fractions import Fraction

import pytest

from analysis.diagnostics.profit_taking_actions import (
    EXCHANGE_ADDRESSES, LEGACY_EXCHANGE_ADDRESSES, V2_EXCHANGE_ADDRESSES,
    FIFOConsumption, LotAllocation, ObservedFIFO, OwnAction,
    apply_action, decode_own_action, deduplicate_raw_fills, exposure_buy,
    reconcile_match_batch, reconcile_reserved_match_batch, tag_complement_hedge, tag_favorite_sale,
)


EXCHANGE = sorted(EXCHANGE_ADDRESSES)[0]
COMPLEMENTS = {"1": "2", "2": "1"}


def raw(
    side: str = "BUY", token: str = "1", quantity: int = 1_000_000,
    cash: int = 400_000, *, wallet: str = "wallet", counterparty: str = "other",
    block: int = 1, log: int = 1, tx: str | None = None, fee: int = 0,
    order_hash: str | None = None,
) -> dict:
    return {
        "order_hash": order_hash or f"order-{block}-{log}", "maker": wallet,
        "taker": counterparty, "maker_asset_id": "0" if side == "BUY" else token,
        "taker_asset_id": token if side == "BUY" else "0",
        "maker_amount_filled": cash if side == "BUY" else quantity,
        "taker_amount_filled": quantity if side == "BUY" else cash,
        "fee": fee, "block_number": block,
        "transaction_hash": tx or f"tx-{block}", "log_index": log,
        "exchange_address": EXCHANGE,
    }


def action(*args, **kwargs) -> OwnAction:
    return decode_own_action(raw(*args, **kwargs), fee_rule="received_asset")


def test_own_maker_fields_never_infer_the_counterparty() -> None:
    buy = action(wallet="WALLET", counterparty="BUYING-COMPLEMENT", fee=10_000)
    sell = action("SELL", cash=900_000, fee=20_000)
    assert buy.wallet == "wallet"
    assert buy.side == "BUY"
    assert buy.token_id == "1"
    assert buy.gross_price == Fraction(2, 5)
    assert buy.acquired_quantity_micro == 990_000
    assert sell.side == "SELL"
    assert sell.sale_cash_micro == 880_000
    assert buy.identity == ("tx-1", 1, EXCHANGE)
    assert not buy.is_taker_aggregate
    assert action(counterparty=EXCHANGE).is_taker_aggregate


@pytest.mark.parametrize("cash", [0, 1_000_000])
def test_boundary_price_actions_remain_in_history(cash: int) -> None:
    book = ObservedFIFO()
    apply_action(book, action(cash=cash), COMPLEMENTS)
    assert book.observed_remaining_micro("wallet", "1") == 1_000_000


@pytest.mark.parametrize("patch", [
    {"maker_asset_id": "0", "taker_asset_id": "0"},
    {"maker_asset_id": "1", "taker_asset_id": "2"},
    {"maker_amount_filled": 0.5}, {"maker_amount_filled": True},
    {"maker_amount_filled": -1}, {"maker_amount_filled": " 1"},
    {"taker_amount_filled": 0}, {"maker_amount_filled": 1_000_001},
    {"fee": -1}, {"fee": 1_000_001}, {"maker": ""},
    {"transaction_hash": None}, {"log_index": -1},
])
def test_invalid_original_fields_fail_closed(patch: dict) -> None:
    row = raw(); row.update(patch)
    with pytest.raises(ValueError):
        decode_own_action(row, fee_rule="received_asset")


def test_unknown_nonzero_fee_blocks_net_book_but_preserves_source_decode() -> None:
    unverified = decode_own_action(raw(fee=10_000))
    assert unverified.fee_micro == 10_000
    with pytest.raises(ValueError, match="unverified"):
        ObservedFIFO().acquire(unverified)
    assert decode_own_action(raw()).acquired_quantity_micro == 1_000_000


@pytest.mark.parametrize("exchange", sorted(V2_EXCHANGE_ADDRESSES))
def test_v2_buy_fee_is_extra_collateral_not_token_deduction(exchange: str) -> None:
    row = raw(quantity=10, cash=8, fee=1); row["exchange_address"] = exchange
    buy = decode_own_action(row, fee_rule="collateral_extra_buy")
    assert buy.acquired_quantity_micro == 10
    assert buy.acquisition_cash_micro == 9
    assert buy.gross_price == Fraction(4, 5)
    book = ObservedFIFO(); book.acquire(buy)
    sale_row = raw("SELL", quantity=10, cash=9, fee=1, block=2)
    sale_row["exchange_address"] = exchange
    sale = decode_own_action(sale_row, fee_rule="collateral_extra_buy")
    match = book.consume(sale)
    tag = tag_favorite_sale(sale, match, pretrade_net_favorite_quantity_micro=10)
    assert sale.sale_cash_micro == 8
    assert tag.allocations[0].cost_micro == 9
    assert tag.allocations[0].profit_micro == -1
    assert tag.qualifying_quantity_micro == 0


def test_v2_buy_fee_is_not_compared_to_token_quantity_in_different_units() -> None:
    row = raw(quantity=1, cash=1, fee=2)
    row["exchange_address"] = sorted(V2_EXCHANGE_ADDRESSES)[0]
    buy = decode_own_action(row, fee_rule="collateral_extra_buy")
    assert buy.acquired_quantity_micro == 1
    assert buy.acquisition_cash_micro == 3


@pytest.mark.parametrize("exchange,rule", [
    (sorted(V2_EXCHANGE_ADDRESSES)[0], "received_asset"),
    (sorted(LEGACY_EXCHANGE_ADDRESSES)[0], "collateral_extra_buy"),
    ("unverified-contract", "received_asset"),
])
def test_fee_rule_never_silently_crosses_exchange_versions(exchange: str, rule: str) -> None:
    row = raw(); row["exchange_address"] = exchange
    with pytest.raises(ValueError, match="Fee rule"):
        decode_own_action(row, fee_rule=rule)


def test_v2_reserved_batch_preserves_buy_fee_as_extra_cost() -> None:
    exchange = sorted(V2_EXCHANGE_ADDRESSES)[0]
    taker_row = raw(quantity=10, cash=9, fee=1, counterparty=exchange, log=3)
    maker_row = raw("SELL", quantity=10, cash=8, fee=1, wallet="maker", counterparty="wallet", log=1)
    for row in (taker_row, maker_row): row["exchange_address"] = exchange
    taker = decode_own_action(taker_row, fee_rule="collateral_extra_buy")
    maker = decode_own_action(maker_row, fee_rule="collateral_extra_buy")
    batch = reconcile_reserved_match_batch(taker, [maker], COMPLEMENTS,
                                           aggregate_amount_semantics="reserved_making_with_refund")
    assert batch.refund_micro == 1
    assert batch.effective_taker.gross_cash_micro == 8
    assert batch.effective_taker.acquisition_cash_micro == 9
    assert batch.effective_taker.acquired_quantity_micro == 10
    assert maker.sale_cash_micro == 7


def test_dedup_replays_by_original_identity_not_order_hash() -> None:
    first = raw(order_hash="repeated-order", log=1)
    second = raw(order_hash="repeated-order", log=2)
    assert len(deduplicate_raw_fills([second, first, dict(first)])) == 2
    conflict = dict(first); conflict["maker"] = "other-wallet"
    with pytest.raises(ValueError, match="Conflicting"):
        deduplicate_raw_fills([first, conflict])


@pytest.mark.parametrize("taker_side,maker_side,token,kind,taker_cash", [
    ("BUY", "SELL", "1", "NORMAL", 900_000),
    ("SELL", "BUY", "1", "NORMAL", 900_000),
    ("BUY", "BUY", "2", "MINT", 100_000),
    ("SELL", "SELL", "2", "MERGE", 100_000),
])
def test_batch_reconciles_normal_mint_merge(
    taker_side: str, maker_side: str, token: str, kind: str, taker_cash: int,
) -> None:
    taker = action(taker_side, cash=taker_cash, counterparty=EXCHANGE, log=3)
    maker = action(maker_side, token, cash=900_000, wallet="maker",
                   counterparty="wallet", log=1)
    legs = reconcile_match_batch(taker, [maker], COMPLEMENTS,
                                 aggregate_amount_semantics="effective_execution")
    assert len(legs) == 1
    assert legs[0].kind == kind
    assert legs[0].taker.wallet == "wallet"
    assert legs[0].taker_cash_micro == taker_cash
    assert legs[0].taker_fill_fraction == 1
    assert legs[0].taker.aggregate_reconciled


@pytest.mark.parametrize("side,source_quantity,source_cash,expected_refund,refund_asset", [
    ("BUY", 10, 12, 4, "0"),
    ("SELL", 12, 8, 2, "1"),
])
def test_reserved_making_refund_preserves_original_and_effective_amounts(
    side: str, source_quantity: int, source_cash: int, expected_refund: int, refund_asset: str,
) -> None:
    taker = action(side, quantity=source_quantity, cash=source_cash,
                   counterparty=EXCHANGE, log=3)
    maker = action("SELL" if side == "BUY" else "BUY", quantity=10, cash=8,
                   wallet="maker", counterparty="wallet", log=1)
    batch = reconcile_reserved_match_batch(taker, [maker], COMPLEMENTS,
                                           aggregate_amount_semantics="reserved_making_with_refund")
    assert batch.source_taker == taker
    assert batch.effective_taker.gross_quantity_micro == 10
    assert batch.effective_taker.gross_cash_micro == 8
    assert batch.effective_taker.gross_price == Fraction(4, 5)
    assert batch.refund_micro == expected_refund
    assert batch.refund_asset_id == refund_asset
    assert batch.legs[0].taker_fill_fraction == 1
    assert batch.effective_taker.aggregate_reconciled
    with pytest.raises(ValueError, match="Unreconciled"):
        _ = taker.gross_price
    with pytest.raises(ValueError, match="Unreconciled"):
        apply_action(ObservedFIFO(), taker, COMPLEMENTS)


@pytest.mark.parametrize("failure", ["taking", "negative_refund", "wrong_exchange", "unverified"])
def test_reserved_refund_does_not_repair_unknown_gaps(failure: str) -> None:
    taker = action("BUY", quantity=10, cash=9, counterparty=EXCHANGE, log=3)
    maker = action("SELL", quantity=10, cash=8, wallet="maker", counterparty="wallet", log=1)
    semantics = "reserved_making_with_refund"
    if failure == "taking": taker = replace(taker, gross_quantity_micro=11)
    if failure == "negative_refund": taker = replace(taker, gross_cash_micro=7)
    if failure == "wrong_exchange": taker = replace(taker, exchange_address=next(x for x in EXCHANGE_ADDRESSES if x != EXCHANGE))
    if failure == "unverified": semantics = "unknown"
    with pytest.raises(ValueError):
        reconcile_reserved_match_batch(taker, [maker], COMPLEMENTS, aggregate_amount_semantics=semantics)


def test_mixed_partial_batch_preserves_single_taker_fill() -> None:
    makers = [
        action("SELL", "1", quantity=3, cash=2, wallet="maker-a", counterparty="wallet", log=1),
        action("BUY", "2", quantity=7, cash=3, wallet="maker-b", counterparty="wallet", log=2),
    ]
    taker = action("BUY", "1", quantity=10, cash=6, counterparty=EXCHANGE, log=3)
    legs = reconcile_match_batch(taker, makers, COMPLEMENTS,
                                 aggregate_amount_semantics="effective_execution")
    assert [leg.kind for leg in legs] == ["NORMAL", "MINT"]
    assert [leg.taker_fill_fraction for leg in legs] == [Fraction(3, 10), Fraction(7, 10)]
    assert sum(leg.taker_cash_micro for leg in legs) == 6
    assert sum(leg.taker_fill_fraction for leg in legs) == 1


@pytest.mark.parametrize("failure", [
    "no_own_taker", "unverified_refund", "missing_maker", "cash_discrepancy",
    "quantity_discrepancy", "wrong_counterparty", "wrong_token", "wrong_tx",
    "wrong_exchange", "after_aggregate", "duplicate_leg", "nonbinary_map",
])
def test_batch_does_not_fabricate_inconsistent_taker_legs(failure: str) -> None:
    taker = action("BUY", cash=900_000, counterparty=EXCHANGE, log=3)
    maker = action("SELL", cash=900_000, wallet="maker", counterparty="wallet", log=1)
    semantics, tokens, makers = "effective_execution", COMPLEMENTS, [maker]
    if failure == "no_own_taker": taker = replace(taker, counterparty="not-exchange")
    if failure == "unverified_refund": semantics = "reserved_collateral_unknown_refund"
    if failure == "missing_maker": makers = []
    if failure == "cash_discrepancy": taker = replace(taker, gross_cash_micro=900_001)
    if failure == "quantity_discrepancy": taker = replace(taker, gross_quantity_micro=1_000_001)
    if failure == "wrong_counterparty": makers = [replace(maker, counterparty="another-wallet")]
    if failure == "wrong_token": makers = [replace(maker, token_id="2")]
    if failure == "wrong_tx": makers = [replace(maker, transaction_hash="another-tx")]
    if failure == "wrong_exchange": makers = [replace(maker, exchange_address="other-exchange")]
    if failure == "after_aggregate": makers = [replace(maker, log_index=4)]
    if failure == "duplicate_leg": makers = [maker, maker]
    if failure == "nonbinary_map": tokens = {"1": "2", "2": "3"}
    with pytest.raises(ValueError):
        reconcile_match_batch(taker, makers, tokens, aggregate_amount_semantics=semantics)


def test_fifo_consumes_integer_quantity_once_with_exact_fractional_costs() -> None:
    book = ObservedFIFO()
    book.acquire(action(quantity=3, cash=1, block=1))
    book.acquire(action(quantity=7, cash=4, block=2))
    sale = action("SELL", quantity=8, cash=7, block=3)
    match = book.consume(sale)
    assert [lot.quantity_micro for lot in match.allocations] == [3, 5]
    assert [lot.acquisition_cash_micro for lot in match.allocations] == [Fraction(1), Fraction(20, 7)]
    assert match.matched_quantity_micro == 8
    assert match.unmatched_quantity_micro == 0
    assert book.observed_remaining_micro("wallet", "1") == 2
    later = book.consume(action("SELL", quantity=5, cash=4, block=4))
    assert later.matched_quantity_micro == 2
    assert later.unmatched_quantity_micro == 3
    assert book.unknown_disposal_micro("wallet", "1") == 3
    assert book.observed_remaining_micro("wallet", "1") == 0
    book.validate_remaining_totals()


def test_sales_before_acquisition_stay_unknown_and_do_not_borrow_future_buys() -> None:
    book = ObservedFIFO()
    earlier = apply_action(book, action("SELL", cash=900_000), COMPLEMENTS)
    assert earlier.matched_quantity_micro == 0
    assert earlier.unmatched_quantity_micro == 1_000_000
    assert earlier.qualifying_fill_fraction == 0
    apply_action(book, action(block=2), COMPLEMENTS)
    assert book.observed_remaining_micro("wallet", "1") == 1_000_000
    book.validate_remaining_totals()


def test_cached_remaining_stock_reconciles_many_partial_acquisitions_and_disposals() -> None:
    book = ObservedFIFO()
    for block in range(1, 101):
        book.acquire(action(quantity=3, cash=1, block=block))
    assert book.observed_remaining_micro("wallet", "1") == 300
    book.validate_remaining_totals()
    for block in range(101, 150):
        book.consume(action("SELL", quantity=5, cash=4, block=block))
        book.validate_remaining_totals()
    assert book.observed_remaining_micro("wallet", "1") == 55
    book._remaining[("wallet", "1")] += 1
    with pytest.raises(ValueError, match="Cached"):
        book.validate_remaining_totals()


def test_fifo_wallet_token_isolation_and_replay_order_gates() -> None:
    book = ObservedFIFO()
    initial = action(wallet="a")
    book.acquire(initial)
    with pytest.raises(ValueError, match="replayed"):
        book.acquire(initial)
    with pytest.raises(ValueError, match="EVM order"):
        book.acquire(action(block=0))
    sale = book.consume(action("SELL", wallet="b", block=2))
    assert sale.matched_quantity_micro == 0
    sale = book.consume(action("SELL", token="2", wallet="a", block=3))
    assert sale.matched_quantity_micro == 0
    assert book.observed_remaining_micro("a", "1") == 1_000_000


def test_favorite_at_sale_not_at_purchase_and_not_at_resolution() -> None:
    book = ObservedFIFO()
    apply_action(book, action(cash=100_000), COMPLEMENTS)
    exit_tag = apply_action(book, action("SELL", cash=900_000, block=2), COMPLEMENTS)
    assert exit_tag.favorite_at_exit
    assert exit_tag.qualifying_quantity_micro == 1_000_000
    assert exit_tag.exposure_reducing_profitable_quantity_micro == 1_000_000
    assert exit_tag.allocations[0].profit_micro == 800_000
    assert exit_tag.qualifying_fill_fraction == 1


@pytest.mark.parametrize("sale_cash", [400_000, 500_000, 900_000])
def test_no_tag_for_nonfavorite_break_even_or_loss(sale_cash: int) -> None:
    book = ObservedFIFO()
    apply_action(book, action(cash=sale_cash), COMPLEMENTS)
    exit_tag = apply_action(book, action("SELL", cash=sale_cash, block=2), COMPLEMENTS)
    assert exit_tag.qualifying_fill_fraction == 0


def test_fee_adjusted_acquisition_and_sale_cost_can_remove_gross_profit() -> None:
    book = ObservedFIFO()
    apply_action(book, action(cash=800_000, fee=100_000), COMPLEMENTS)
    tag = apply_action(book, action("SELL", quantity=900_000, cash=810_000,
                                     fee=20_000, block=2), COMPLEMENTS)
    assert tag.favorite_at_exit
    assert tag.allocations[0].cost_micro == 800_000
    assert tag.allocations[0].exit_value_micro == 790_000
    assert tag.allocations[0].profit_micro == -10_000
    assert tag.qualifying_quantity_micro == 0


def test_same_transaction_purchase_updates_fifo_but_never_prior_profit_tag() -> None:
    book = ObservedFIFO()
    apply_action(book, action(tx="shared", log=1), COMPLEMENTS)
    sale = apply_action(book, action("SELL", cash=900_000, tx="shared", log=2), COMPLEMENTS)
    assert sale.matched_quantity_micro == 1_000_000
    assert sale.allocations[0].profit_micro == 500_000
    assert not sale.allocations[0].prior_transaction
    assert sale.qualifying_fill_fraction == 0
    assert book.observed_remaining_micro("wallet", "1") == 0


def test_complement_hedge_locks_profit_without_consuming_favorite_stock() -> None:
    book = ObservedFIFO()
    apply_action(book, action(cash=400_000), COMPLEMENTS)
    hedge = apply_action(book, action(token="2", cash=100_000, block=2), COMPLEMENTS)
    assert hedge.exit_kind == "complementary_buy_hedge"
    assert hedge.qualifying_quantity_micro == 1_000_000
    assert hedge.allocations[0].profit_micro == 500_000
    assert hedge.qualifying_fill_fraction == 1
    assert book.observed_remaining_micro("wallet", "1") == 1_000_000
    assert book.observed_remaining_micro("wallet", "2") == 1_000_000
    second = apply_action(book, action(token="2", cash=100_000, block=3), COMPLEMENTS)
    assert second.pretrade_net_favorite_quantity_micro == 0
    assert second.qualifying_quantity_micro == 0
    assert second.unmatched_quantity_micro == 1_000_000


def test_fully_hedged_low_price_buy_never_rescans_retained_favorite_lots() -> None:
    book=ObservedFIFO()
    apply_action(book,action(cash=400_000),COMPLEMENTS)
    apply_action(book,action(token='2',cash=100_000,block=2),COMPLEMENTS)
    original=book._lots[('wallet','1')]
    class NoIterationDeque(deque):
        def __iter__(self):
            pytest.fail('Zero-request preview must not iterate the retained FIFO prefix')
    book._lots[('wallet','1')]=NoIterationDeque(original)
    tag=apply_action(book,action(token='2',cash=100_000,block=3),COMPLEMENTS)
    assert tag.matched_quantity_micro==tag.qualifying_quantity_micro==0
    assert tag.unmatched_quantity_micro==1_000_000
    assert book.observed_remaining_micro('wallet','1')==1_000_000
    assert book.observed_remaining_micro('wallet','2')==2_000_000
    book._lots[('wallet','1')]=original
    book.validate_remaining_totals()


def test_repeated_hedge_offsets_oldest_favorite_lots_before_current_cost_matching() -> None:
    book = ObservedFIFO()
    apply_action(book, action(cash=100_000), COMPLEMENTS)
    apply_action(book, action(cash=800_000, block=2), COMPLEMENTS)
    first = apply_action(book, action(token="2", cash=200_000, block=3), COMPLEMENTS)
    second = apply_action(book, action(token="2", cash=200_000, block=4), COMPLEMENTS)
    assert first.allocations[0].cost_micro == 100_000
    assert first.qualifying_quantity_micro == 1_000_000
    assert second.allocations[0].cost_micro == 800_000
    assert second.allocations[0].profit_micro == 0
    assert second.qualifying_quantity_micro == 0
    assert book.observed_remaining_micro("wallet", "1") == 2_000_000


def test_direct_profits_and_exposure_reduction_are_separate_after_hedging() -> None:
    book = ObservedFIFO()
    apply_action(book, action(quantity=2_000_000, cash=400_000), COMPLEMENTS)
    apply_action(book, action(token="2", cash=100_000, block=2), COMPLEMENTS)
    sale = apply_action(book, action("SELL", quantity=2_000_000, cash=1_800_000, block=3), COMPLEMENTS)
    assert sale.qualifying_quantity_micro == 2_000_000
    assert sale.qualifying_fill_fraction == 1
    assert sale.pretrade_net_favorite_quantity_micro == 1_000_000
    assert sale.exposure_reducing_profitable_quantity_micro == 1_000_000
    assert sale.exposure_reducing_fill_fraction == Fraction(1, 2)
    assert book.observed_remaining_micro("wallet", "1") == 0
    assert book.observed_remaining_micro("wallet", "2") == 1_000_000


def test_partial_fee_aware_hedge_preserves_one_original_fill_fraction() -> None:
    book = ObservedFIFO()
    apply_action(book, action(quantity=3, cash=1), COMPLEMENTS)
    hedge = apply_action(book, action(token="2", quantity=8, cash=2, fee=1, block=2), COMPLEMENTS)
    assert hedge.fraction_basis_quantity_micro == 7
    assert hedge.matched_quantity_micro == 3
    assert hedge.unmatched_quantity_micro == 4
    assert hedge.qualifying_fill_fraction == Fraction(3, 7)
    assert hedge.allocations[0].profit_micro == Fraction(8, 7)


def test_same_transaction_hedge_is_unqualified_but_updates_stock() -> None:
    book = ObservedFIFO()
    apply_action(book, action(tx="shared", log=1), COMPLEMENTS)
    hedge = apply_action(book, action(token="2", cash=100_000, tx="shared", log=2), COMPLEMENTS)
    assert hedge.matched_quantity_micro == 1_000_000
    assert hedge.qualifying_quantity_micro == 0
    assert book.observed_remaining_micro("wallet", "2") == 1_000_000


def test_exposure_equivalence_is_not_literal_opposite_wallet_action() -> None:
    favorite_sale = action("SELL", cash=900_000)
    assert exposure_buy(favorite_sale, COMPLEMENTS) == ("2", Fraction(1, 10))
    assert favorite_sale.side == "SELL"
    assert exposure_buy(action(cash=900_000), COMPLEMENTS) == ("1", Fraction(9, 10))
    with pytest.raises(ValueError, match="complement"):
        exposure_buy(favorite_sale, {"1": "2"})


def test_forged_match_provenance_and_oversized_hedge_fail_closed() -> None:
    acquisition = action()
    sale = action("SELL", cash=900_000, block=2)
    forged = FIFOConsumption(1_000_000, (LotAllocation(
        replace(acquisition, wallet="someone-else"), 1_000_000, Fraction(400_000), True,
    ),), 0)
    with pytest.raises(ValueError, match="provenance"):
        tag_favorite_sale(sale, forged, pretrade_net_favorite_quantity_micro=1_000_000)
    hedge = action(token="2", cash=100_000, block=2)
    match = FIFOConsumption(1_000_000, (LotAllocation(acquisition, 1_000_000, Fraction(400_000), True),), 0)
    with pytest.raises(ValueError, match="exceeds"):
        tag_complement_hedge(hedge, match, favorite_token_id="1", pretrade_net_favorite_quantity_micro=500_000)
