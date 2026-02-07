"""Core transformation: AMLSim transactions → Deriv Affiliate Fraud schema.

Produces: partners.csv, clients.csv, trades.csv, commissions.csv, referrals.csv

Key design: "Smart Selection" ensures fraud hub accounts are force-included
as partners (20% fraud slots + 80% legit top fan-in) to avoid the cold-start
problem where all top-volume accounts are legitimate banks.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.config import (
    ACCOUNTS_FILE,
    COMMISSION_DELAY_MINUTES,
    COMMISSION_RATE,
    INSTRUMENTS,
    RAW_DATA_DIR,
    SAMPLE_TRANSACTIONS,
    SEED,
    TOP_N_PARTNERS,
    TRANS_FILE,
    TRANSFORMED_DIR,
)
from pipeline.parse_patterns import load_fraud_rings


# ── Step A: Load & Rename ─────────────────────────────────────────────────────

def load_transactions(
    sample: int | None = SAMPLE_TRANSACTIONS,
    seed: int = SEED,
) -> pd.DataFrame:
    """Load raw CSV, fix duplicate columns, subsample preserving ALL fraud rows."""
    path = RAW_DATA_DIR / TRANS_FILE
    df = pd.read_csv(path)

    # Fix duplicate "Account" columns — pandas auto-renames to Account, Account.1
    df.columns = [
        "Timestamp", "From Bank", "sender_account", "To Bank",
        "receiver_account", "Amount Received", "Receiving Currency",
        "Amount Paid", "Payment Currency", "Payment Format", "Is Laundering",
    ]

    df["Timestamp"] = pd.to_datetime(df["Timestamp"], format="%Y/%m/%d %H:%M")

    if sample is not None and len(df) > sample:
        # Keep ALL fraud rows, only subsample legitimate rows
        fraud_df = df[df["Is Laundering"] == 1]
        legit_df = df[df["Is Laundering"] == 0]
        n_legit = sample - len(fraud_df)
        if n_legit > 0 and len(legit_df) > n_legit:
            legit_df = legit_df.sample(n=n_legit, random_state=seed)
        df = pd.concat([fraud_df, legit_df]).sort_values("Timestamp").reset_index(drop=True)

    return df


def load_accounts() -> pd.DataFrame:
    """Load accounts CSV."""
    return pd.read_csv(RAW_DATA_DIR / ACCOUNTS_FILE)


# ── Step B: Smart Partner Selection ───────────────────────────────────────────

def identify_partners(
    trans_df: pd.DataFrame,
    fraud_rings: list[dict],
    top_n: int = TOP_N_PARTNERS,
) -> tuple[pd.Index, set[str]]:
    """Smart selection: 20% fraud hub slots + 80% legit top fan-in.

    Returns (partner_accounts Index, set of fraud_hub_accounts that were selected).
    """
    in_degree = trans_df.groupby("receiver_account")["sender_account"].nunique()

    # Collect all hub accounts from parsed fraud rings
    fraud_hubs = set(r["hub_account"] for r in fraud_rings)

    # A. Pick fraud hubs that actually appear as receivers in our data
    valid_fraud_hubs = [h for h in fraud_hubs if h in in_degree.index]
    fraud_candidates = in_degree[in_degree.index.isin(valid_fraud_hubs)]
    target_fraud_count = int(top_n * 0.20)  # reserve 20% slots
    selected_fraud = fraud_candidates.nlargest(min(target_fraud_count, len(fraud_candidates))).index.tolist()

    # B. Fill remaining slots with top legit partners (exclude fraud hubs)
    legit_candidates = in_degree[~in_degree.index.isin(fraud_hubs)]
    target_legit_count = top_n - len(selected_fraud)
    selected_legit = legit_candidates.nlargest(target_legit_count).index.tolist()

    # C. Combine
    combined = list(set(selected_fraud + selected_legit))
    return pd.Index(combined, name="receiver_account"), set(selected_fraud)


# ── Step C: Filter to partner-related transactions ────────────────────────────

def filter_partner_transactions(
    trans_df: pd.DataFrame,
    partner_accounts: pd.Index,
) -> pd.DataFrame:
    """Keep only rows where the receiver is a known partner."""
    return trans_df[trans_df["receiver_account"].isin(partner_accounts)].copy()


# ── Helper: build fraud lookups ───────────────────────────────────────────────

def _fraud_lookups(fraud_rings: list[dict]) -> tuple[set, dict, dict]:
    """Return (all_fraud_accounts, hub→ring_ids, account→ring_ids)."""
    all_fraud_accounts: set[str] = set()
    hub_ring_map: dict[str, list[int]] = {}
    account_ring_map: dict[str, list[int]] = {}
    for ring in fraud_rings:
        rid = ring["ring_id"]
        hub = ring["hub_account"]
        hub_ring_map.setdefault(hub, []).append(rid)
        for acc in ring["accounts"]:
            all_fraud_accounts.add(acc)
            account_ring_map.setdefault(acc, []).append(rid)
    return all_fraud_accounts, hub_ring_map, account_ring_map


# ── Step D: Partners table ────────────────────────────────────────────────────

def build_partners(
    filtered_df: pd.DataFrame,
    partner_accounts: pd.Index,
    accounts_df: pd.DataFrame,
    fraud_rings: list[dict],
) -> pd.DataFrame:
    """Build partners.csv with entity info and fraud flags."""
    all_fraud, hub_rings, acc_rings = _fraud_lookups(fraud_rings)
    ring_type_map = {r["ring_id"]: r["pattern_type"] for r in fraud_rings}

    # Aggregate per partner
    agg = filtered_df.groupby("receiver_account").agg(
        num_referred_clients=("sender_account", "nunique"),
        total_trade_volume=("Amount Paid", "sum"),
    ).reindex(partner_accounts, fill_value=0)

    agg["total_commissions_paid"] = agg["total_trade_volume"] * COMMISSION_RATE
    agg["avg_commission"] = np.where(
        agg["num_referred_clients"] > 0,
        agg["total_commissions_paid"] / agg["num_referred_clients"],
        0.0,
    )

    # Join with accounts master for entity info
    acc_lookup = accounts_df.set_index("Account Number")[
        ["Bank ID", "Bank Name", "Entity Name"]
    ]
    agg = agg.join(acc_lookup, how="left")

    # Fraud flags — a partner is fraudulent if their account is in ANY fraud ring
    agg["is_fraudulent"] = agg.index.isin(all_fraud)
    agg["fraud_ring_ids"] = [
        ",".join(map(str, acc_rings.get(acc, []))) for acc in agg.index
    ]
    agg["primary_pattern_type"] = [
        ring_type_map.get(acc_rings[acc][0], "") if acc in acc_rings else ""
        for acc in agg.index
    ]

    # Assign partner IDs
    agg = agg.reset_index().rename(columns={"receiver_account": "account_number"})
    agg.insert(0, "partner_id", [f"P_{i+1:04d}" for i in range(len(agg))])
    agg = agg.rename(columns={
        "Bank ID": "bank_id", "Bank Name": "bank_name", "Entity Name": "entity_name",
    })

    return agg


# ── Step E: Clients table ─────────────────────────────────────────────────────

def build_clients(
    filtered_df: pd.DataFrame,
    partners_df: pd.DataFrame,
    accounts_df: pd.DataFrame,
    fraud_rings: list[dict],
) -> pd.DataFrame:
    """Build clients.csv — one row per (client, partner) relationship."""
    all_fraud, _, _ = _fraud_lookups(fraud_rings)

    # Map partner account → partner_id
    partner_map = partners_df.set_index("account_number")["partner_id"].to_dict()

    # Group by (sender, receiver)
    grouped = (
        filtered_df
        .groupby(["sender_account", "receiver_account"])
        .agg(num_trades=("Amount Paid", "size"), total_volume=("Amount Paid", "sum"))
        .reset_index()
    )
    grouped["partner_id"] = grouped["receiver_account"].map(partner_map)

    # Join entity info
    acc_lookup = accounts_df.set_index("Account Number")[
        ["Bank ID", "Bank Name", "Entity Name"]
    ]
    grouped = grouped.join(acc_lookup, on="sender_account", how="left")
    grouped["is_in_fraud_ring"] = grouped["sender_account"].isin(all_fraud)

    # Assign client IDs
    grouped = grouped.rename(columns={
        "sender_account": "account_number",
        "Bank ID": "bank_id",
        "Bank Name": "bank_name",
        "Entity Name": "entity_name",
    }).drop(columns=["receiver_account"])
    grouped.insert(0, "client_id", [f"C_{i+1:06d}" for i in range(len(grouped))])

    return grouped


# ── Step F: Trades table ──────────────────────────────────────────────────────

def build_trades(
    filtered_df: pd.DataFrame,
    clients_df: pd.DataFrame,
    partners_df: pd.DataFrame,
    seed: int = SEED,
) -> pd.DataFrame:
    """Build trades.csv — one trade per filtered transaction."""
    rng = np.random.default_rng(seed)

    # Build lookup: (sender_account, partner_id) → client_id
    client_lookup = clients_df.set_index(["account_number", "partner_id"])["client_id"].to_dict()
    partner_map = partners_df.set_index("account_number")["partner_id"].to_dict()

    df = filtered_df.copy()
    df["partner_id"] = df["receiver_account"].map(partner_map)

    # Vectorized client_id lookup
    df["_key"] = list(zip(df["sender_account"], df["partner_id"]))
    df["client_id"] = df["_key"].map(client_lookup).fillna("")
    df.drop(columns=["_key"], inplace=True)

    # Instrument: seeded per client for consistency
    unique_clients = df["client_id"].unique()
    client_instrument_map = {}
    for cid in unique_clients:
        client_rng = np.random.default_rng(seed + hash(cid) % (2**31))
        n_instr = client_rng.integers(1, 4)
        client_instrument_map[cid] = list(
            client_rng.choice(INSTRUMENTS, size=n_instr, replace=False)
        )

    instruments = []
    for cid in df["client_id"]:
        pool = client_instrument_map.get(cid, INSTRUMENTS)
        instruments.append(rng.choice(pool))
    df["instrument"] = instruments

    # Direction: random BUY/SELL
    df["direction"] = rng.choice(["BUY", "SELL"], size=len(df))

    # Build output
    trades = pd.DataFrame({
        "trade_id": [f"T_{i+1:07d}" for i in range(len(df))],
        "timestamp": df["Timestamp"].values,
        "client_id": df["client_id"].values,
        "partner_id": df["partner_id"].values,
        "instrument": df["instrument"],
        "direction": df["direction"],
        "trade_volume": df["Amount Paid"].values,
        "currency": df["Payment Currency"].values,
        "is_fraudulent": df["Is Laundering"].values.astype(bool),
    })

    return trades


# ── Step G: Commissions table ─────────────────────────────────────────────────

def build_commissions(trades_df: pd.DataFrame) -> pd.DataFrame:
    """Build commissions.csv — one commission per trade."""
    return pd.DataFrame({
        "commission_id": [f"CM_{i+1:07d}" for i in range(len(trades_df))],
        "timestamp": pd.to_datetime(trades_df["timestamp"]) + pd.Timedelta(minutes=COMMISSION_DELAY_MINUTES),
        "client_id": trades_df["client_id"].values,
        "partner_id": trades_df["partner_id"].values,
        "trade_id": trades_df["trade_id"].values,
        "commission_amount": (trades_df["trade_volume"].values * COMMISSION_RATE),
        "currency": trades_df["currency"].values,
        "is_fraudulent": trades_df["is_fraudulent"].values,
    })


# ── Step H: Referrals table ──────────────────────────────────────────────────

def build_referrals(trades_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregated partner–client edges."""
    ref = (
        trades_df
        .groupby(["partner_id", "client_id"])
        .agg(
            first_trade_date=("timestamp", "min"),
            last_trade_date=("timestamp", "max"),
            num_trades=("trade_id", "size"),
            total_volume=("trade_volume", "sum"),
        )
        .reset_index()
    )
    ref["total_commissions"] = ref["total_volume"] * COMMISSION_RATE
    return ref


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_transform(
    sample: int | None = SAMPLE_TRANSACTIONS,
    top_n_partners: int = TOP_N_PARTNERS,
    seed: int = SEED,
) -> dict[str, pd.DataFrame]:
    """Run the full transformation pipeline and save CSVs."""
    TRANSFORMED_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading fraud rings...")
    fraud_rings = load_fraud_rings()

    print("Loading transactions...")
    trans_df = load_transactions(sample=sample, seed=seed)
    n_fraud_tx = trans_df["Is Laundering"].sum()
    print(f"  Loaded {len(trans_df):,} transactions ({n_fraud_tx:,} fraudulent — ALL fraud rows kept)")

    print("Loading accounts...")
    accounts_df = load_accounts()

    print("Identifying partners (smart 80/20 selection)...")
    partner_accounts, selected_fraud_hubs = identify_partners(
        trans_df, fraud_rings, top_n=top_n_partners,
    )
    print(f"  {len(partner_accounts)} partners selected ({len(selected_fraud_hubs)} fraud hubs force-included)")

    print("Filtering to partner-related transactions...")
    filtered = filter_partner_transactions(trans_df, partner_accounts)
    n_fraud_filtered = filtered["Is Laundering"].sum()
    print(f"  {len(filtered):,} partner-related transactions ({n_fraud_filtered:,} fraudulent)")

    print("Building partners table...")
    partners_df = build_partners(filtered, partner_accounts, accounts_df, fraud_rings)
    n_fraud_partners = partners_df["is_fraudulent"].sum()
    print(f"  {len(partners_df)} partners ({n_fraud_partners} fraudulent)")

    print("Building clients table...")
    clients_df = build_clients(filtered, partners_df, accounts_df, fraud_rings)
    n_fraud_clients = clients_df["is_in_fraud_ring"].sum()
    print(f"  {len(clients_df):,} client-partner relationships ({n_fraud_clients:,} in fraud rings)")

    print("Building trades table...")
    trades_df = build_trades(filtered, clients_df, partners_df, seed=seed)
    print(f"  {len(trades_df):,} trades")

    print("Building commissions table...")
    commissions_df = build_commissions(trades_df)
    print(f"  {len(commissions_df):,} commissions")

    print("Building referrals table...")
    referrals_df = build_referrals(trades_df)
    print(f"  {len(referrals_df):,} referral edges")

    # Save
    tables = {
        "partners": partners_df,
        "clients": clients_df,
        "trades": trades_df,
        "commissions": commissions_df,
        "referrals": referrals_df,
    }
    for name, df in tables.items():
        path = TRANSFORMED_DIR / f"{name}.csv"
        df.to_csv(path, index=False)
        print(f"  Saved {path} ({len(df):,} rows)")

    return tables
