"""Exact, outcome-blind primitives for observed trade-history exit matching.

This module deliberately does not scan production data or infer a counterparty's
action from a maker order. Inputs use Stage 1's original OrderFilled fields. An
exchange-facing taker aggregate is an own-order action at a different grain from
its maker legs; reconciliation is required before it may be used.

FIFO is accounting over the supplied observed acquisitions, not a holdings
reconstruction. Opening balances and nontrade movements remain unknown. Cash
allocations are Fractions in micro-USDC, and token quantities are integer
microtokens. No exit classification consults an eventual outcome.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, replace
from fractions import Fraction
from typing import Any, Iterable, Literal, Mapping


Side = Literal["BUY", "SELL"]
FeeRule = Literal["unknown", "received_asset", "collateral_extra_buy"]
MatchKind = Literal["NORMAL", "MINT", "MERGE"]
LEGACY_EXCHANGE_ADDRESSES = frozenset({
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
})
V2_EXCHANGE_ADDRESSES = frozenset({
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
})
EXCHANGE_ADDRESSES = LEGACY_EXCHANGE_ADDRESSES | V2_EXCHANGE_ADDRESSES
EXCHANGE_CONTRACTS = {
    **{address: ("legacy_reserved_making_v1", "received_asset")
       for address in LEGACY_EXCHANGE_ADDRESSES},
    **{address: ("ctf_exchange_v2_v1", "collateral_extra_buy")
       for address in V2_EXCHANGE_ADDRESSES},
}
RAW_FIELDS = (
    "order_hash", "maker", "taker", "maker_asset_id", "taker_asset_id",
    "maker_amount_filled", "taker_amount_filled", "fee", "block_number",
    "transaction_hash", "log_index", "exchange_address",
)


def _integer(value: Any, field: str, minimum: int = 0) -> int:
    """Accept exact integers or their decimal strings, never rounded floats."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f"{field} must be an exact integer")
    if isinstance(value, str) and (not value.isdecimal() or value.strip() != value):
        raise ValueError(f"{field} must be an unsigned decimal integer")
    number = int(value)
    if number < minimum:
        raise ValueError(f"{field} must be >= {minimum}")
    return number


def _identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError(f"{field} must be a nonempty identifier")
    return value.lower()


@dataclass(frozen=True)
class OwnAction:
    """A wallet's own order, never a fabricated opposite counterparty action."""

    order_hash: str
    wallet: str
    counterparty: str
    token_id: str
    side: Side
    gross_quantity_micro: int
    gross_cash_micro: int
    fee_micro: int
    fee_rule: FeeRule
    block_number: int
    transaction_hash: str
    log_index: int
    exchange_address: str
    aggregate_reconciled: bool = False

    @property
    def identity(self) -> tuple[str, int, str]:
        return self.transaction_hash, self.log_index, self.exchange_address

    @property
    def order_key(self) -> tuple[int, int]:
        # The original EVM log index is block-global, not orderHash-local.
        return self.block_number, self.log_index

    @property
    def gross_price(self) -> Fraction:
        self._require_execution_amounts()
        return Fraction(self.gross_cash_micro, self.gross_quantity_micro)

    @property
    def is_taker_aggregate(self) -> bool:
        return self.counterparty in EXCHANGE_ADDRESSES

    def _require_execution_amounts(self) -> None:
        if self.is_taker_aggregate and not self.aggregate_reconciled:
            raise ValueError("Unreconciled taker aggregate cannot enter trade-history accounting")

    def _require_fee_rule(self) -> None:
        self._require_execution_amounts()
        if self.fee_rule != "unknown" and EXCHANGE_CONTRACTS.get(self.exchange_address, (None, None))[1] != self.fee_rule:
            raise ValueError("Fee rule does not match the verified emitting exchange")
        if self.fee_micro and self.fee_rule == "unknown":
            raise ValueError("Nonzero fee has unverified asset semantics")

    @property
    def acquired_quantity_micro(self) -> int:
        self._require_fee_rule()
        if self.side != "BUY":
            raise ValueError("Only a BUY acquires outcome tokens")
        return (self.gross_quantity_micro - self.fee_micro
                if self.fee_rule == "received_asset" else self.gross_quantity_micro)

    @property
    def acquisition_cash_micro(self) -> int:
        self._require_fee_rule()
        if self.side != "BUY":
            raise ValueError("Only a BUY incurs acquisition cost")
        return (self.gross_cash_micro + self.fee_micro
                if self.fee_rule == "collateral_extra_buy" else self.gross_cash_micro)

    @property
    def sale_cash_micro(self) -> int:
        self._require_fee_rule()
        if self.side != "SELL":
            raise ValueError("Only a SELL receives collateral")
        return self.gross_cash_micro - self.fee_micro


def decode_own_action(row: Mapping[str, Any], *, fee_rule: FeeRule = "unknown") -> OwnAction:
    """Decode only the maker identity and that identity's own source order.

    `fee_rule='received_asset'` is opt-in after source verification. Zero-fee
    actions do not require assumptions about a fee asset. Boundary prices are
    retained for history. Aggregate amounts must still pass batch reconciliation.
    """
    missing = set(RAW_FIELDS) - row.keys()
    if missing:
        raise ValueError(f"Missing original OrderFilled fields: {sorted(missing)}")
    if fee_rule not in ("unknown", "received_asset", "collateral_extra_buy"):
        raise ValueError("Unrecognized fee rule")
    maker_asset = _integer(row["maker_asset_id"], "maker_asset_id")
    taker_asset = _integer(row["taker_asset_id"], "taker_asset_id")
    if (maker_asset == 0) == (taker_asset == 0):
        raise ValueError("Own order requires exactly one collateral asset")
    side: Side = "BUY" if maker_asset == 0 else "SELL"
    maker_amount = _integer(row["maker_amount_filled"], "maker_amount_filled")
    taker_amount = _integer(row["taker_amount_filled"], "taker_amount_filled")
    quantity, cash = ((taker_amount, maker_amount) if side == "BUY"
                      else (maker_amount, taker_amount))
    counterparty = _identifier(row["taker"], "taker")
    # Reserved-making aggregate cash is not yet an execution price. Only its
    # own verified refund reconciliation can establish bounded effective price.
    if quantity <= 0 or (cash > quantity and counterparty not in EXCHANGE_ADDRESSES):
        raise ValueError("Invalid binary execution quantity or gross price")
    fee = _integer(row["fee"], "fee")
    exchange = _identifier(row["exchange_address"], "exchange_address")
    if fee_rule != "unknown" and EXCHANGE_CONTRACTS.get(exchange, (None, None))[1] != fee_rule:
        raise ValueError("Fee rule does not match the verified emitting exchange")
    if (fee_rule == "received_asset" or side == "SELL") and fee_rule != "unknown" and fee > taker_amount:
        raise ValueError("Fee exceeds the own order's received asset")
    return OwnAction(
        _identifier(row["order_hash"], "order_hash"),
        _identifier(row["maker"], "maker"),
        counterparty,
        str(taker_asset if side == "BUY" else maker_asset), side, quantity, cash,
        fee, fee_rule, _integer(row["block_number"], "block_number"),
        _identifier(row["transaction_hash"], "transaction_hash"),
        _integer(row["log_index"], "log_index"),
        exchange,
    )


def deduplicate_raw_fills(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Remove exact log replays only; reject conflicting original identities."""
    by_identity: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in rows:
        action = decode_own_action(row)
        copied = {name: row[name] for name in RAW_FIELDS}
        previous = by_identity.get(action.identity)
        if previous is not None and previous != copied:
            raise ValueError(f"Conflicting original log identity: {action.identity}")
        by_identity[action.identity] = copied
    return sorted(by_identity.values(), key=lambda row: (
        _integer(row["block_number"], "block_number"),
        _integer(row["log_index"], "log_index"),
        _identifier(row["exchange_address"], "exchange_address"),
    ))


@dataclass(frozen=True)
class ReconciledLeg:
    maker: OwnAction
    taker: OwnAction
    kind: MatchKind
    taker_quantity_micro: int
    taker_cash_micro: int
    taker_fill_fraction: Fraction


def _batch_terms(
    taker: OwnAction, makers: Iterable[OwnAction], complements: Mapping[str, str],
) -> tuple[tuple[OwnAction, MatchKind, int, int], ...]:
    if not taker.is_taker_aggregate:
        raise ValueError("Taker evidence must be its own exchange-facing aggregate")
    if taker.counterparty != taker.exchange_address:
        raise ValueError("Aggregate counterparty must equal its emitting exchange")
    opposite = complements.get(taker.token_id)
    if opposite is None or opposite == taker.token_id or complements.get(opposite) != taker.token_id:
        raise ValueError("A unique symmetric binary complement map is required")
    maker_rows = tuple(makers)
    if not maker_rows:
        raise ValueError("Cannot reconstruct a taker from no maker legs")
    identities: set[tuple[str, int, str]] = set()
    terms: list[tuple[OwnAction, MatchKind, int, int]] = []
    for maker in maker_rows:
        if maker.identity in identities:
            raise ValueError("Duplicate maker leg would inflate a batch")
        identities.add(maker.identity)
        if (maker.is_taker_aggregate or maker.counterparty != taker.wallet
                or maker.transaction_hash != taker.transaction_hash
                or maker.block_number != taker.block_number
                or maker.exchange_address != taker.exchange_address
                or maker.log_index >= taker.log_index):
            raise ValueError("Maker leg does not belong to the evidenced taker batch")
        if (maker.gross_quantity_micro <= 0 or maker.gross_cash_micro < 0
                or maker.gross_cash_micro > maker.gross_quantity_micro):
            raise ValueError("Maker leg has invalid effective quantity or price")
        if maker.side != taker.side and maker.token_id == taker.token_id:
            kind: MatchKind = "NORMAL"
            cash = maker.gross_cash_micro
        elif maker.side == taker.side and maker.token_id == opposite:
            kind = "MINT" if taker.side == "BUY" else "MERGE"
            cash = maker.gross_quantity_micro - maker.gross_cash_micro
        else:
            raise ValueError("Contradictory assets or directions in matching batch")
        terms.append((maker, kind, maker.gross_quantity_micro, cash))
    return tuple(terms)


def reconcile_match_batch(
    taker: OwnAction, makers: Iterable[OwnAction],
    complements: Mapping[str, str], *, aggregate_amount_semantics: str,
) -> tuple[ReconciledLeg, ...]:
    """Check a caller-established batch without guessing event segmentation.

    Only verified effective-execution aggregate amounts are accepted. Reserved
    collateral/refund aggregates, missing legs, one-sided evidence, and integer
    discrepancies are blocked rather than repaired. The caller supplies a
    validated symmetric, same-binary-market complement map. One taker own action
    remains one fill; per-maker taker allocations sum to exactly one fill unit.
    Returned legs reference the certified effective aggregate, not the input's
    unverified reserved amounts.
    """
    if aggregate_amount_semantics != "effective_execution":
        raise ValueError("Aggregate execution/refund semantics are unverified")
    terms = _batch_terms(taker, makers, complements)
    if (taker.gross_quantity_micro <= 0 or taker.gross_cash_micro < 0
            or taker.gross_cash_micro > taker.gross_quantity_micro):
        raise ValueError("Effective taker execution price must lie in [0,1]")
    received = taker.gross_quantity_micro if taker.side == "BUY" else taker.gross_cash_micro
    if (taker.fee_rule == "received_asset" or taker.side == "SELL") and taker.fee_micro > received:
        raise ValueError("Aggregate fee exceeds its effective received asset")
    certified_taker = replace(taker, aggregate_reconciled=True)
    legs = tuple(ReconciledLeg(
        maker, certified_taker, kind, quantity, cash,
        Fraction(quantity, taker.gross_quantity_micro),
    ) for maker, kind, quantity, cash in terms)
    if (sum(leg.taker_quantity_micro for leg in legs) != taker.gross_quantity_micro
            or sum(leg.taker_cash_micro for leg in legs) != taker.gross_cash_micro):
        raise ValueError("Own taker aggregate fails exact quantity/cash conservation")
    if sum((leg.taker_fill_fraction for leg in legs), Fraction()) != 1:
        raise ValueError("Taker leg allocation failed to preserve one fill unit")
    return legs


@dataclass(frozen=True)
class ReconciledBatch:
    source_taker: OwnAction
    effective_taker: OwnAction
    refund_asset_id: str
    refund_micro: int
    legs: tuple[ReconciledLeg, ...]
    aggregate_amount_semantics: str = "verified_reserved_making_with_refund"


def reconcile_reserved_match_batch(
    taker: OwnAction, makers: Iterable[OwnAction], complements: Mapping[str, str],
    *, aggregate_amount_semantics: str,
) -> ReconciledBatch:
    """Reconcile the verified legacy reserved-making/refund event contract.

    Passive own orders establish effective active quantity and spending through
    NORMAL/MINT/MERGE conservation. Source aggregate *received-taking* must equal
    reconstructed received-taking exactly. The only allowed discrepancy is a
    nonnegative refund of its original making asset: collateral for a BUY,
    outcome tokens for a SELL. Both original and effective amounts survive.
    This option is opt-in only after verifying the deployed source semantics;
    it does not infer missing logs, unknown fees, receipt flows, or balance gaps.
    """
    if aggregate_amount_semantics != "reserved_making_with_refund":
        raise ValueError("Reserved-making refund source semantics are unverified")
    maker_rows = tuple(makers)
    terms = _batch_terms(taker, maker_rows, complements)
    effective_quantity = sum(term[2] for term in terms)
    effective_cash = sum(term[3] for term in terms)
    source_taking = taker.gross_quantity_micro if taker.side == "BUY" else taker.gross_cash_micro
    effective_taking = effective_quantity if taker.side == "BUY" else effective_cash
    if source_taking != effective_taking:
        raise ValueError("Source aggregate received-taking fails exact reconciliation")
    source_making = taker.gross_cash_micro if taker.side == "BUY" else taker.gross_quantity_micro
    effective_making = effective_cash if taker.side == "BUY" else effective_quantity
    if source_making < effective_making:
        raise ValueError("Negative implied making-asset refund")
    effective = replace(taker, gross_quantity_micro=effective_quantity,
                        gross_cash_micro=effective_cash)
    legs = reconcile_match_batch(effective, maker_rows, complements,
                                 aggregate_amount_semantics="effective_execution")
    return ReconciledBatch(taker, legs[0].taker,
                           "0" if taker.side == "BUY" else taker.token_id,
                           source_making - effective_making, legs)


@dataclass
class _Lot:
    action: OwnAction
    remaining_micro: int
    cost_per_micro: Fraction


@dataclass(frozen=True)
class LotAllocation:
    acquisition: OwnAction
    quantity_micro: int
    acquisition_cash_micro: Fraction
    strictly_prior_transaction: bool


@dataclass(frozen=True)
class FIFOConsumption:
    requested_quantity_micro: int
    allocations: tuple[LotAllocation, ...]
    unmatched_quantity_micro: int

    @property
    def matched_quantity_micro(self) -> int:
        return sum(lot.quantity_micro for lot in self.allocations)

    @property
    def prior_matched_quantity_micro(self) -> int:
        return sum(lot.quantity_micro for lot in self.allocations if lot.strictly_prior_transaction)


class ObservedFIFO:
    """Consume recorded acquisitions once; unmatched sales stay explicitly unknown.

    This is a partial-history accounting book, not an inventory claim. Unmatched
    sales do not create invented negative lots or consume future acquisitions.
    Callers feed all actions in original EVM order, independently of focal filters.
    Same-transaction acquisitions update the book but are not prior-match tags.
    """

    def __init__(self) -> None:
        self._lots: dict[tuple[str, str], deque[_Lot]] = defaultdict(deque)
        self._remaining: dict[tuple[str, str], int] = defaultdict(int)
        self._last_key: tuple[int, int] | None = None
        self._identities: set[tuple[str, int, str]] = set()
        self._unknown_disposals: dict[tuple[str, str], int] = defaultdict(int)

    def _check_order(self, action: OwnAction) -> None:
        if action.identity in self._identities:
            raise ValueError("A replayed action cannot update observed FIFO twice")
        if self._last_key is not None and action.order_key < self._last_key:
            raise ValueError("Observed FIFO requires original EVM order")
        self._last_key = action.order_key
        self._identities.add(action.identity)

    def acquire(self, action: OwnAction) -> None:
        if action.side != "BUY":
            raise ValueError("Acquire requires an own BUY")
        quantity = action.acquired_quantity_micro
        if quantity <= 0:
            raise ValueError("A BUY with no net tokens cannot create a FIFO lot")
        self._check_order(action)
        self._lots[(action.wallet, action.token_id)].append(_Lot(
            action, quantity, Fraction(action.acquisition_cash_micro, quantity),
        ))
        self._remaining[(action.wallet, action.token_id)] += quantity

    def consume(self, action: OwnAction) -> FIFOConsumption:
        if action.side != "SELL":
            raise ValueError("Consume requires an own SELL")
        action._require_fee_rule()
        self._check_order(action)
        quantity_left = action.gross_quantity_micro
        lots = self._lots[(action.wallet, action.token_id)]
        allocations: list[LotAllocation] = []
        while lots and quantity_left:
            lot = lots[0]
            quantity = min(lot.remaining_micro, quantity_left)
            allocations.append(LotAllocation(
                lot.action, quantity, lot.cost_per_micro * quantity,
                lot.action.transaction_hash != action.transaction_hash
                and lot.action.order_key < action.order_key,
            ))
            lot.remaining_micro -= quantity
            quantity_left -= quantity
            if not lot.remaining_micro:
                lots.popleft()
        self._unknown_disposals[(action.wallet, action.token_id)] += quantity_left
        self._remaining[(action.wallet, action.token_id)] -= action.gross_quantity_micro - quantity_left
        return FIFOConsumption(action.gross_quantity_micro, tuple(allocations), quantity_left)

    def observed_remaining_micro(self, wallet: str, token_id: str) -> int:
        return self._remaining[(wallet.lower(), token_id)]

    def validate_remaining_totals(self) -> None:
        """Audit cached integer stock against physical FIFO lots at market boundaries."""
        for key, total in self._remaining.items():
            if total < 0 or total != sum(lot.remaining_micro for lot in self._lots[key]):
                raise ValueError("Cached observed stock does not reconcile to FIFO lots")

    def unknown_disposal_micro(self, wallet: str, token_id: str) -> int:
        return self._unknown_disposals[(wallet.lower(), token_id)]

    def preview(
        self, action: OwnAction, token_id: str, quantity_micro: int, *, skip_micro: int = 0,
    ) -> FIFOConsumption:
        """Read remaining FIFO lots without disposing physically held tokens.

        For a complementary hedge, existing complement stock offsets the oldest
        favorite lots. `skip_micro` makes the current hedge use the unhedged FIFO
        suffix instead of repeatedly claiming the same acquisition cost. The
        actual BUY then updates physical recorded stock through `acquire`.
        """
        quantity_micro = _integer(quantity_micro, "quantity_micro")
        skip_micro = _integer(skip_micro, "skip_micro")
        if not quantity_micro:
            return FIFOConsumption(0, (), 0)
        remaining = quantity_micro
        allocations: list[LotAllocation] = []
        for lot in self._lots[(action.wallet, token_id)]:
            if skip_micro >= lot.remaining_micro:
                skip_micro -= lot.remaining_micro
                continue
            available = lot.remaining_micro - skip_micro
            skip_micro = 0
            quantity = min(available, remaining)
            if quantity:
                allocations.append(LotAllocation(
                    lot.action, quantity, lot.cost_per_micro * quantity,
                    lot.action.transaction_hash != action.transaction_hash
                    and lot.action.order_key < action.order_key,
                ))
                remaining -= quantity
            if not remaining:
                break
        return FIFOConsumption(quantity_micro, tuple(allocations), remaining)


@dataclass(frozen=True)
class ProfitAllocation:
    acquisition_identity: tuple[str, int, str]
    quantity_micro: int
    cost_micro: Fraction
    exit_value_micro: Fraction
    profit_micro: Fraction
    prior_transaction: bool
    qualifies: bool
    acquisition_block_number: int = 0


@dataclass(frozen=True)
class ExitTag:
    action_identity: tuple[str, int, str]
    exit_kind: str
    favorite_at_exit: bool
    requested_quantity_micro: int
    matched_quantity_micro: int
    unmatched_quantity_micro: int
    qualifying_quantity_micro: int
    qualifying_fill_fraction: Fraction
    allocations: tuple[ProfitAllocation, ...]
    exposure_reducing_profitable_quantity_micro: int
    exposure_reducing_fill_fraction: Fraction
    pretrade_net_favorite_quantity_micro: int
    fraction_basis_quantity_micro: int
    history_status: str = "trade_implied_only_opening_and_nontrade_movements_unknown"


def _validate_match(action: OwnAction, match: FIFOConsumption, token_id: str) -> None:
    if (match.requested_quantity_micro < 0 or match.unmatched_quantity_micro < 0
            or match.matched_quantity_micro + match.unmatched_quantity_micro
            != match.requested_quantity_micro):
        raise ValueError("FIFO quantities do not reconcile")
    for lot in match.allocations:
        if (lot.quantity_micro <= 0 or lot.acquisition.wallet != action.wallet
                or lot.acquisition.token_id != token_id or lot.acquisition.side != "BUY"
                or lot.acquisition_cash_micro < 0
                or lot.acquisition.order_key >= action.order_key
                or lot.strictly_prior_transaction
                != (lot.acquisition.transaction_hash != action.transaction_hash)):
            raise ValueError("FIFO acquisition provenance does not match the exit")


def tag_favorite_sale(
    action: OwnAction, match: FIFOConsumption, *, pretrade_net_favorite_quantity_micro: int,
) -> ExitTag:
    """A strictly prior FIFO purchase, current price > .5, and net realized gain.

    Gain is an accounting property of the observed lot, not a motive or causal
    price effect. Fractional fill weight uses gross disposed quantity as its
    denominator, so partially matched sales do not turn into multiple fills.
    """
    if action.side != "SELL" or match.requested_quantity_micro != action.gross_quantity_micro:
        raise ValueError("Sale tag requires a quantity-aligned own SELL match")
    _validate_match(action, match, action.token_id)
    net_exposure = _integer(pretrade_net_favorite_quantity_micro, "pretrade_net_favorite_quantity_micro")
    favorite = action.gross_price > Fraction(1, 2)
    unit_value = Fraction(action.sale_cash_micro, action.gross_quantity_micro)
    allocations = tuple(ProfitAllocation(
        lot.acquisition.identity, lot.quantity_micro, lot.acquisition_cash_micro,
        unit_value * lot.quantity_micro,
        unit_value * lot.quantity_micro - lot.acquisition_cash_micro,
        lot.strictly_prior_transaction,
        favorite and lot.strictly_prior_transaction
        and unit_value * lot.quantity_micro > lot.acquisition_cash_micro,
        lot.acquisition.block_number,
    ) for lot in match.allocations)
    quantity = sum(lot.quantity_micro for lot in allocations if lot.qualifies)
    exposure_quantity = min(quantity, net_exposure)
    return ExitTag(action.identity, "direct_favorite_sale", favorite,
                   match.requested_quantity_micro, match.matched_quantity_micro,
                   match.unmatched_quantity_micro, quantity,
                   Fraction(quantity, action.gross_quantity_micro), allocations,
                   exposure_quantity, Fraction(exposure_quantity, action.gross_quantity_micro),
                   net_exposure, action.gross_quantity_micro)


def tag_complement_hedge(
    action: OwnAction, match: FIFOConsumption, *, favorite_token_id: str,
    pretrade_net_favorite_quantity_micro: int,
) -> ExitTag:
    """Tag a low-price BUY that locks positive observed acquisition profit.

    A binary complementary pair pays one micro-USDC per paired microtoken.
    Fees enter each acquisition cost through its actual net acquired quantity.
    No eventual winner or later outcome is used. Matching and net-exposure caps
    are trade-implied; the pair is not treated as a physical token disposal.
    """
    if action.side != "BUY" or favorite_token_id == action.token_id:
        raise ValueError("Hedge tag requires a distinct complementary own BUY")
    _validate_match(action, match, favorite_token_id)
    net_exposure = _integer(pretrade_net_favorite_quantity_micro, "pretrade_net_favorite_quantity_micro")
    net_quantity = action.acquired_quantity_micro
    if net_quantity <= 0 or match.requested_quantity_micro > min(net_quantity, net_exposure):
        raise ValueError("Hedge match exceeds received quantity or pretrade net exposure")
    favorite = action.gross_price < Fraction(1, 2)
    unit_cost = Fraction(action.acquisition_cash_micro, net_quantity)
    allocations = tuple(ProfitAllocation(
        lot.acquisition.identity, lot.quantity_micro, lot.acquisition_cash_micro,
        lot.quantity_micro - unit_cost * lot.quantity_micro,
        lot.quantity_micro - unit_cost * lot.quantity_micro - lot.acquisition_cash_micro,
        lot.strictly_prior_transaction,
        favorite and lot.strictly_prior_transaction
        and lot.quantity_micro > (unit_cost * lot.quantity_micro + lot.acquisition_cash_micro),
        lot.acquisition.block_number,
    ) for lot in match.allocations)
    quantity = sum(lot.quantity_micro for lot in allocations if lot.qualifies)
    fraction = Fraction(quantity, net_quantity)
    return ExitTag(action.identity, "complementary_buy_hedge", favorite,
                   net_quantity, match.matched_quantity_micro,
                   net_quantity - match.matched_quantity_micro, quantity, fraction, allocations,
                   quantity, fraction, net_exposure, net_quantity)


def apply_action(
    book: ObservedFIFO, action: OwnAction, complements: Mapping[str, str],
) -> ExitTag | None:
    """Update all own actions; qualification never controls history admission.

    SELL tags distinguish all profitable FIFO disposal from profitable disposal
    capped by positive pretrade net favorite exposure. The latter is the primary
    descriptive unwinding component. Complement BUYs use the same cap without
    virtually consuming favorite stock. Neither implies unique positions closed
    across episodes, certified holdings, profit-taking intent, or causal impact.
    """
    complement = complements.get(action.token_id)
    if (complement is None or complement == action.token_id
            or complements.get(complement) != action.token_id):
        raise ValueError("Own action requires a validated binary complement map")
    same_stock = book.observed_remaining_micro(action.wallet, action.token_id)
    opposite_stock = book.observed_remaining_micro(action.wallet, complement)
    if action.side == "SELL":
        net_exposure = max(same_stock - opposite_stock, 0)
        match = book.consume(action)
        return tag_favorite_sale(action, match,
                                 pretrade_net_favorite_quantity_micro=net_exposure)
    tag = None
    if action.gross_price < Fraction(1, 2):
        net_exposure = max(opposite_stock - same_stock, 0)
        quantity = min(action.acquired_quantity_micro, net_exposure)
        match = book.preview(action, complement, quantity, skip_micro=same_stock)
        tag = tag_complement_hedge(action, match, favorite_token_id=complement,
                                   pretrade_net_favorite_quantity_micro=net_exposure)
    book.acquire(action)
    return tag


def exposure_buy(action: OwnAction, complements: Mapping[str, str]) -> tuple[str, Fraction]:
    """Represent a SELL as complementary BUY exposure, without changing raw action.

    This before-fee payoff equivalence applies to a validated binary proposition.
    It is not evidence of a literal complementary-token acquisition or of a
    counterparty's side. An EPL complement need not be the other named team.
    """
    if action.side == "BUY":
        return action.token_id, action.gross_price
    complement = complements.get(action.token_id)
    if (complement is None or complement == action.token_id
            or complements.get(complement) != action.token_id):
        raise ValueError("SELL exposure requires a unique symmetric complement")
    return complement, 1 - action.gross_price
