"""Inject Opposite Trading and Bonus Abuse fraud signals into the trades table.

Strategy shift from v1: Instead of requiring fraud-ring client pairs to
already exist in the data (which fails due to entity resolution gaps), we
take each fraud partner's existing trades and forcibly pair them up —
overwriting timestamps, instruments, and directions to create detectable
opposite-trading signals.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.config import (
    BONUS_ABUSE_DEPOSIT,
    BONUS_ABUSE_WITHDRAW_DELAY_HOURS,
    COMMISSION_RATE,
    OPPOSITE_TRADE_PROBABILITY,
    SEED,
    TRANSFORMED_DIR,
)
from pipeline.parse_patterns import load_fraud_rings


def _load_tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    trades = pd.read_csv(TRANSFORMED_DIR / "trades.csv", parse_dates=["timestamp"])
    partners = pd.read_csv(TRANSFORMED_DIR / "partners.csv")
    referrals = pd.read_csv(TRANSFORMED_DIR / "referrals.csv")
    return trades, partners, referrals


# ── Pattern 1: Opposite Trading ──────────────────────────────────────────────

def inject_opposite_trading(
    trades: pd.DataFrame,
    partners: pd.DataFrame,
    seed: int = SEED,
) -> pd.DataFrame:
    """For each fraud partner, pair up their trades and force opposite signals.

    Takes existing trades under each fraudulent partner, sorts by timestamp,
    and rewrites consecutive pairs to share the same timestamp (±2s),
    same instrument, opposite direction, and similar volume.
    """
    rng = np.random.default_rng(seed)

    trades["is_opposite_trade"] = False

    fraud_partners = partners[partners["is_fraudulent"]]["partner_id"].values
    if len(fraud_partners) == 0:
        return trades

    count_injected = 0

    for partner_id in fraud_partners:
        p_mask = trades["partner_id"] == partner_id
        p_indices = trades.index[p_mask]

        if len(p_indices) < 2:
            continue

        # Sort by timestamp to find natural neighbors
        p_sorted = trades.loc[p_indices].sort_values("timestamp")
        indices = p_sorted.index.tolist()

        # Step through in pairs
        for i in range(0, len(indices) - 1, 2):
            idx1 = indices[i]
            idx2 = indices[i + 1]

            # 80% get the opposite treatment, 20% remain as noise
            if rng.random() > OPPOSITE_TRADE_PROBABILITY:
                continue

            # Force timestamp sync: move trade 2 to within seconds of trade 1
            base_time = trades.at[idx1, "timestamp"]
            trades.at[idx2, "timestamp"] = base_time + pd.Timedelta(
                seconds=int(rng.integers(1, 60)) # Change to: 1-60 minutes (Harder to link)
            )

            # Force same instrument
            trades.at[idx2, "instrument"] = trades.at[idx1, "instrument"]

            # Force OPPOSITE direction
            dir1 = trades.at[idx1, "direction"]
            trades.at[idx1, "direction"] = "BUY"
            trades.at[idx2, "direction"] = "SELL" if dir1 == "BUY" else "BUY"
            # Ensure they are truly opposite
            if trades.at[idx1, "direction"] == trades.at[idx2, "direction"]:
                trades.at[idx2, "direction"] = "SELL"
                trades.at[idx1, "direction"] = "BUY"

            # Force similar volume (±2%)
            vol1 = trades.at[idx1, "trade_volume"]
            trades.at[idx2, "trade_volume"] = round(
                vol1 * rng.uniform(0.98, 1.02), 2
            )

            # Mark both
            trades.at[idx1, "is_opposite_trade"] = True
            trades.at[idx2, "is_opposite_trade"] = True
            trades.at[idx1, "is_fraudulent"] = True
            trades.at[idx2, "is_fraudulent"] = True
            count_injected += 2

    print(f"  Rewrote {count_injected} trades into opposite-trade pairs")
    return trades


# ── Pattern 2: Bonus Abuse ───────────────────────────────────────────────────

def inject_bonus_abuse(
    trades: pd.DataFrame,
    partners: pd.DataFrame,
    seed: int = SEED,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """For ~30% of fraud partners, inject coordinated deposit-withdraw pattern.

    Returns (updated trades, new withdrawals DataFrame).
    """
    rng = np.random.default_rng(seed + 1000)

    fraud_partners = partners[partners["is_fraudulent"]].copy()
    if fraud_partners.empty:
        return trades, pd.DataFrame(
            columns=["withdrawal_id", "timestamp", "client_id", "partner_id", "amount", "is_bonus_abuse"]
        )

    # Select ~30% for bonus abuse
    n_abuse = max(1, int(len(fraud_partners) * 0.3))
    abuse_partners = fraud_partners.sample(n=n_abuse, random_state=seed + 1000)

    # Build partner→client lookup from trades
    partner_clients = trades.groupby("partner_id")["client_id"].apply(list).to_dict()

    if "is_bonus_abuse" not in trades.columns:
        trades["is_bonus_abuse"] = False
    if "is_opposite_trade" not in trades.columns:
        trades["is_opposite_trade"] = False

    new_trades = []
    withdrawals = []
    trade_counter = len(trades)
    withdrawal_counter = 0

    for _, partner_row in abuse_partners.iterrows():
        pid = partner_row["partner_id"]
        clients = partner_clients.get(pid, [])
        if not clients:
            continue

        # Pick 10-15 clients (or all if fewer)
        unique_clients = list(set(clients))
        if len(unique_clients) < 2:
            continue
        n_clients = min(int(rng.integers(10, 16)), len(unique_clients))
        selected = list(rng.choice(unique_clients, size=n_clients, replace=False))

        # All first trades within a 1-hour coordinated window
        base_time = pd.Timestamp("2022-09-01") + pd.Timedelta(
            days=int(rng.integers(0, 30)),
            hours=int(rng.integers(6, 22)),
        )

        for cid in selected:
            offset_mins = int(rng.integers(0, 60))
            ts = base_time + pd.Timedelta(minutes=offset_mins)

            trade_counter += 1
            new_trades.append({
                "trade_id": f"T_{trade_counter:07d}",
                "timestamp": ts,
                "client_id": cid,
                "partner_id": pid,
                "instrument": "EURUSD",
                "direction": "BUY",
                "trade_volume": BONUS_ABUSE_DEPOSIT,
                "currency": "US Dollar",
                "is_fraudulent": True,
                "is_opposite_trade": False,
                "is_bonus_abuse": True,
            })

            withdrawal_counter += 1
            withdrawals.append({
                "withdrawal_id": f"W_{withdrawal_counter:05d}",
                "timestamp": ts + pd.Timedelta(hours=BONUS_ABUSE_WITHDRAW_DELAY_HOURS),
                "client_id": cid,
                "partner_id": pid,
                "amount": BONUS_ABUSE_DEPOSIT,
                "is_bonus_abuse": True,
            })

    if new_trades:
        new_df = pd.DataFrame(new_trades)
        trades = pd.concat([trades, new_df], ignore_index=True)

    withdrawals_df = pd.DataFrame(withdrawals) if withdrawals else pd.DataFrame(
        columns=["withdrawal_id", "timestamp", "client_id", "partner_id", "amount", "is_bonus_abuse"]
    )

    return trades, withdrawals_df


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_injection(seed: int = SEED) -> dict[str, pd.DataFrame]:
    """Run all fraud pattern injections. Returns updated tables."""
    print("Loading tables for fraud injection...")
    trades, partners, referrals = _load_tables()
    fraud_rings = load_fraud_rings()

    print("Injecting opposite trading patterns...")
    trades = inject_opposite_trading(trades, partners, seed=seed)
    n_opp = trades["is_opposite_trade"].sum()
    print(f"  Total opposite trade rows: {n_opp}")

    print("Injecting bonus abuse patterns...")
    trades, withdrawals = inject_bonus_abuse(trades, partners, seed=seed)
    n_bonus = trades["is_bonus_abuse"].sum()
    print(f"  {n_bonus} bonus abuse trade rows")
    print(f"  {len(withdrawals)} withdrawal rows")

    # Save updated trades
    trades.to_csv(TRANSFORMED_DIR / "trades.csv", index=False)
    print(f"  Updated trades.csv ({len(trades):,} rows)")

    withdrawals.to_csv(TRANSFORMED_DIR / "withdrawals.csv", index=False)
    print(f"  Saved withdrawals.csv ({len(withdrawals)} rows)")

    # Resync commissions with modified trade volumes
    print("Resyncing commissions with updated trade volumes...")
    commissions = pd.read_csv(TRANSFORMED_DIR / "commissions.csv")
    trade_vol_map = trades.set_index("trade_id")["trade_volume"].to_dict()
    mask = commissions["trade_id"].isin(trade_vol_map)
    commissions.loc[mask, "commission_amount"] = (
        commissions.loc[mask, "trade_id"].map(trade_vol_map) * COMMISSION_RATE
    )
    commissions.to_csv(TRANSFORMED_DIR / "commissions.csv", index=False)
    print(f"  Resynced {mask.sum()} commission records")

    return {"trades": trades, "withdrawals": withdrawals}
