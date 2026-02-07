"""CLI entrypoint for the full Deriv Guardian pipeline.

Phase 1-2: Data transformation (AMLSim → Deriv Affiliate Fraud schema)
Phase 3:   Kumo.ai predictions + GenAI investigation copilot

Usage:
    # Full pipeline (Phase 1-2 only, safe without API keys):
    python -m pipeline.run_pipeline [--sample 500000] [--top-partners 200] [--seed 42]

    # Include Phase 3 (requires KUMO_API_KEY):
    python -m pipeline.run_pipeline --with-kumo

    # Include Phase 3 copilot reports (requires AZURE_OPENAI_URL + AZURE_OPENAI_KEY):
    python -m pipeline.run_pipeline --with-kumo --with-copilot
"""

from __future__ import annotations

import argparse
import os
import time

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from pipeline.config import (
    COMMISSION_RATE,
    SAMPLE_TRANSACTIONS,
    SEED,
    TOP_N_PARTNERS,
    TRANSFORMED_DIR,
    KUMO_EXPORT_DIR,
)
from pipeline.parse_patterns import parse_patterns, save_fraud_rings
from pipeline.transform import run_transform
from pipeline.inject_patterns import run_injection
from pipeline.export_kumo import run_export


def validate(transformed_dir=TRANSFORMED_DIR) -> list[str]:
    """Run validation checks on the output tables. Returns list of issues (empty = OK)."""
    issues: list[str] = []

    partners = pd.read_csv(transformed_dir / "partners.csv")
    clients = pd.read_csv(transformed_dir / "clients.csv")
    trades = pd.read_csv(transformed_dir / "trades.csv")
    commissions = pd.read_csv(transformed_dir / "commissions.csv")
    referrals = pd.read_csv(transformed_dir / "referrals.csv")

    # 1. All partner_ids in commissions exist in partners
    comm_partners = set(commissions["partner_id"].unique())
    known_partners = set(partners["partner_id"].unique())
    orphan = comm_partners - known_partners
    if orphan:
        issues.append(f"Commission partner_ids not in partners.csv: {orphan}")

    # 2. All client_ids in trades exist in clients
    trade_clients = set(trades["client_id"].unique())
    known_clients = set(clients["client_id"].unique())
    orphan_clients = trade_clients - known_clients
    # Injected trades may reference existing client_ids, so filter empties
    orphan_clients.discard("")
    if orphan_clients:
        issues.append(f"Trade client_ids not in clients.csv: {len(orphan_clients)} orphans")

    # 3. Fraud ratio in partners
    n_fraud = partners["is_fraudulent"].sum()
    pct = n_fraud / len(partners) * 100 if len(partners) > 0 else 0
    if n_fraud == 0:
        issues.append("No fraudulent partners found")
    print(f"  Partner fraud rate: {n_fraud}/{len(partners)} ({pct:.1f}%)")

    # 4. Opposite trading pairs validation
    if "is_opposite_trade" in trades.columns:
        opp = trades[trades["is_opposite_trade"] == True]
        if len(opp) > 0:
            # Check that opposite trades come in pairs (same timestamp roughly, same instrument)
            print(f"  Opposite trades: {len(opp)}")
        else:
            print("  No opposite trades found (may be OK if no fraud ring overlap)")

    # 5. Commission amounts = trade_volume × COMMISSION_RATE
    merged = commissions.merge(trades[["trade_id", "trade_volume"]], on="trade_id", how="inner")
    if len(merged) > 0:
        expected = merged["trade_volume"] * COMMISSION_RATE
        diff = (merged["commission_amount"] - expected).abs()
        bad = diff[diff > 0.01]
        if len(bad) > 0:
            issues.append(f"Commission amount mismatch for {len(bad)} rows")

    # 6. No NaN primary keys
    for name, df, pk in [
        ("partners", partners, "partner_id"),
        ("clients", clients, "client_id"),
        ("trades", trades, "trade_id"),
        ("commissions", commissions, "commission_id"),
    ]:
        nulls = df[pk].isna().sum()
        if nulls > 0:
            issues.append(f"{name}.{pk} has {nulls} NaN values")

    return issues


def main():
    parser = argparse.ArgumentParser(description="Deriv Guardian — Full Pipeline")
    parser.add_argument("--sample", type=int, default=SAMPLE_TRANSACTIONS,
                        help="Number of transactions to sample (0 = full dataset)")
    parser.add_argument("--top-partners", type=int, default=TOP_N_PARTNERS,
                        help="Top N partners by fan-in")
    parser.add_argument("--seed", type=int, default=SEED, help="Random seed")
    parser.add_argument("--with-kumo", action="store_true",
                        help="Run Kumo.ai predictions (requires KUMO_API_KEY)")
    parser.add_argument("--with-copilot", action="store_true",
                        help="Generate LLM investigation reports (requires AZURE_OPENAI_URL + AZURE_OPENAI_KEY)")
    parser.add_argument("--copilot-top-n", type=int, default=5,
                        help="Number of top-risk partners for LLM reports")
    args = parser.parse_args()

    sample = args.sample if args.sample and args.sample > 0 else None

    total_steps = 5
    if args.with_kumo:
        total_steps += 1
    if args.with_copilot:
        total_steps += 1
    step = 0

    t0 = time.time()

    step += 1
    print("=" * 60)
    print(f"STEP {step}/{total_steps}: Parsing fraud patterns")
    print("=" * 60)
    rings = parse_patterns()
    save_fraud_rings(rings)
    print(f"  {len(rings)} fraud rings parsed\n")

    step += 1
    print("=" * 60)
    print(f"STEP {step}/{total_steps}: Core transformation")
    print("=" * 60)
    tables = run_transform(sample=sample, top_n_partners=args.top_partners, seed=args.seed)
    print()

    step += 1
    print("=" * 60)
    print(f"STEP {step}/{total_steps}: Fraud pattern injection")
    print("=" * 60)
    injected = run_injection(seed=args.seed)
    print()

    step += 1
    print("=" * 60)
    print(f"STEP {step}/{total_steps}: Kumo.ai export")
    print("=" * 60)
    exports = run_export()
    print()

    step += 1
    print("=" * 60)
    print(f"STEP {step}/{total_steps}: Validation")
    print("=" * 60)
    issues = validate()
    if issues:
        print("  ISSUES FOUND:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print("  All checks passed!")

    # ── Phase 3: Kumo Predictions ──────────────────────────────────────────
    if args.with_kumo:
        step += 1
        print()
        print("=" * 60)
        print(f"STEP {step}/{total_steps}: Kumo.ai GNN Predictions")
        print("=" * 60)
        if not os.environ.get("KUMO_API_KEY"):
            print("  ⚠ KUMO_API_KEY not set — skipping")
            print("  Get a free key at https://kumorfm.ai")
        else:
            from pipeline.kumo_predict import run_kumo_predictions
            try:
                kumo_results = run_kumo_predictions()
                print(f"  Predictions complete!")
            except Exception as e:
                print(f"  ⚠ Kumo prediction failed: {e}")

    # ── Phase 3: Copilot Reports ───────────────────────────────────────────
    if args.with_copilot:
        step += 1
        print()
        print("=" * 60)
        print(f"STEP {step}/{total_steps}: GenAI Investigation Reports")
        print("=" * 60)
        if not os.environ.get("AZURE_OPENAI_KEY"):
            print("  ⚠ AZURE_OPENAI_KEY not set — using quick summaries")
            from pipeline.copilot import generate_quick_summary
            partners = pd.read_csv(TRANSFORMED_DIR / "partners.csv")
            fraud = partners[partners["is_fraudulent"] == True].head(args.copilot_top_n)
            for _, row in fraud.iterrows():
                pid = row["partner_id"]
                try:
                    s = generate_quick_summary(pid)
                    print(f"  {pid}: {s['risk_level']} — {s['recommendation']}")
                except Exception as e:
                    print(f"  {pid}: Error — {e}")
        else:
            from pipeline.copilot import generate_batch_reports
            try:
                reports = generate_batch_reports(top_n=args.copilot_top_n)
                print(f"  Generated {len(reports)} reports")
            except Exception as e:
                print(f"  ⚠ Report generation failed: {e}")

    elapsed = time.time() - t0
    print(f"\nPipeline completed in {elapsed:.1f}s")

    # Summary
    print("\n" + "=" * 60)
    print("OUTPUT SUMMARY")
    print("=" * 60)
    for name in ["partners", "clients", "trades", "commissions", "referrals"]:
        path = TRANSFORMED_DIR / f"{name}.csv"
        if path.exists():
            df = pd.read_csv(path)
            print(f"  {name:15s} → {len(df):>10,} rows  ({path})")
    wpath = TRANSFORMED_DIR / "withdrawals.csv"
    if wpath.exists():
        df = pd.read_csv(wpath)
        print(f"  {'withdrawals':15s} → {len(df):>10,} rows  ({wpath})")
    print()
    for name in ["accounts", "trades", "commissions", "referrals"]:
        path = KUMO_EXPORT_DIR / f"{name}.csv"
        if path.exists():
            df = pd.read_csv(path)
            print(f"  kumo/{name:15s} → {len(df):>10,} rows  ({path})")

    print()
    print("=" * 60)
    print("Next Step: The Kumo Graph Build")
    print("=" * 60)
    print("""  You are now ready to upload these 4 files to Kumo.ai.

  Quick Definition Script: When you go to Kumo to "Create Graph," use these definitions to
  ensure it recognizes the schema correctly:

  1. Nodes

  Table: accounts.csv

  Primary Key: account_id

  Node Type: Account

  2. Edges (Static)

  Table: referrals.csv

  Source Node: partner_account_id (Type: Account)

  Dest Node: client_account_id (Type: Account)

  Relationship: REFERRED

  3. Events (Temporal)

  Table: trades.csv

  Link to Node: client_account_id (Type: Account)

  Timestamp: timestamp

  Event Name: TRADED

  Table: commissions.csv

  Link to Node: partner_account_id (Type: Account)

  Timestamp: timestamp

  Event Name: EARNED_COMMISSION

  Strategic Tip: When you run your predictive query in Kumo, the GNN will now be able to see
  that Partner X (Node) received a Commission (Event) from Client Y (Edge), who executed an
  Opposite Trade (Event) just 50ms after Client Z. This "Multi-Hop" pattern is exactly what
  you promised to build.

  API Integration:
    export KUMO_API_KEY="your-key"
    python -m pipeline.kumo_predict

  Copilot Reports (Azure OpenAI):
    # Set AZURE_OPENAI_URL and AZURE_OPENAI_KEY in .env
    python -m pipeline.copilot --partner P_0002
    python -m pipeline.copilot --batch 10

  Demo API Server:
    pip install fastapi uvicorn
    uvicorn pipeline.api:app --reload --port 8000
    # Open http://localhost:8000/docs for interactive API docs
""")


if __name__ == "__main__":
    main()
