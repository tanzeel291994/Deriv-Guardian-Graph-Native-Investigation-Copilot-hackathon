"""Export clean CSVs for Kumo.ai graph schema upload."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.config import KUMO_EXPORT_DIR, TRANSFORMED_DIR


def export_accounts(
    partners: pd.DataFrame,
    clients: pd.DataFrame,
) -> pd.DataFrame:
    """Union of partners and clients into a single accounts node table."""
    p = partners[["partner_id", "account_number", "bank_name", "entity_name", "is_fraudulent"]].copy()
    p = p.rename(columns={"partner_id": "account_id"})
    p["role"] = "PARTNER"

    c = clients[["client_id", "account_number", "bank_name", "entity_name", "is_in_fraud_ring"]].copy()
    c = c.rename(columns={"client_id": "account_id", "is_in_fraud_ring": "is_fraudulent"})
    c["role"] = "CLIENT"

    # Align columns
    cols = ["account_id", "role", "account_number", "bank_name", "entity_name", "is_fraudulent"]
    accounts = pd.concat([p[cols], c[cols]], ignore_index=True)
    return accounts


def export_trades(trades: pd.DataFrame) -> pd.DataFrame:
    """Trades formatted for Kumo — temporal events on client nodes."""
    cols = [
        "trade_id", "timestamp", "client_id", "instrument", "direction",
        "trade_volume", "is_opposite_trade", "is_bonus_abuse",
    ]
    # Ensure boolean columns exist
    for col in ["is_opposite_trade", "is_bonus_abuse"]:
        if col not in trades.columns:
            trades[col] = False
    out = trades[cols].copy()
    out = out.rename(columns={"client_id": "client_account_id"})
    return out


def export_commissions(commissions: pd.DataFrame) -> pd.DataFrame:
    """Commissions formatted for Kumo — temporal edges partner↔client."""
    cols = [
        "commission_id", "timestamp", "partner_id", "client_id",
        "commission_amount", "currency",
    ]
    out = commissions[cols].copy()
    out = out.rename(columns={
        "partner_id": "partner_account_id",
        "client_id": "client_account_id",
    })
    return out


def export_referrals(referrals: pd.DataFrame) -> pd.DataFrame:
    """Referrals formatted for Kumo — static edges partner↔client."""
    out = referrals[["partner_id", "client_id", "first_trade_date"]].copy()
    out = out.rename(columns={
        "partner_id": "partner_account_id",
        "client_id": "client_account_id",
        "first_trade_date": "referral_date",
    })
    return out


def run_export() -> dict[str, Path]:
    """Load transformed CSVs and produce Kumo-ready exports."""
    KUMO_EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading transformed tables for Kumo export...")
    partners = pd.read_csv(TRANSFORMED_DIR / "partners.csv")
    clients = pd.read_csv(TRANSFORMED_DIR / "clients.csv")
    trades = pd.read_csv(TRANSFORMED_DIR / "trades.csv", parse_dates=["timestamp"])
    commissions = pd.read_csv(TRANSFORMED_DIR / "commissions.csv", parse_dates=["timestamp"])
    referrals = pd.read_csv(TRANSFORMED_DIR / "referrals.csv")

    outputs: dict[str, Path] = {}

    print("Exporting accounts...")
    acc = export_accounts(partners, clients)
    path = KUMO_EXPORT_DIR / "accounts.csv"
    acc.to_csv(path, index=False)
    outputs["accounts"] = path
    print(f"  {path} ({len(acc):,} rows)")

    print("Exporting trades...")
    t = export_trades(trades)
    path = KUMO_EXPORT_DIR / "trades.csv"
    t.to_csv(path, index=False)
    outputs["trades"] = path
    print(f"  {path} ({len(t):,} rows)")

    print("Exporting commissions...")
    c = export_commissions(commissions)
    path = KUMO_EXPORT_DIR / "commissions.csv"
    c.to_csv(path, index=False)
    outputs["commissions"] = path
    print(f"  {path} ({len(c):,} rows)")

    print("Exporting referrals...")
    r = export_referrals(referrals)
    path = KUMO_EXPORT_DIR / "referrals.csv"
    r.to_csv(path, index=False)
    outputs["referrals"] = path
    print(f"  {path} ({len(r):,} rows)")

    return outputs
