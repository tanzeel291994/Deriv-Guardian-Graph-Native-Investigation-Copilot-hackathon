"""Phase 3B: GenAI Investigation Copilot — LLM-powered fraud summaries.

Takes Kumo predictions + fraud ring context and generates human-readable
investigation reports. This is the "Narrator" from the architecture.

Usage:
    python -m pipeline.copilot [--partner P_0002]

Requires:
    pip install openai
    AZURE_OPENAI_URL and AZURE_OPENAI_KEY set in .env
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from textwrap import dedent

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from pipeline.config import TRANSFORMED_DIR, KUMO_EXPORT_DIR

PREDICTIONS_DIR = Path("data/predictions")
REPORTS_DIR = Path("data/reports")


# ── System Prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = dedent("""\
    You are a senior fraud investigator at Deriv, specializing in Partner &
    Affiliate Fraud. You analyze graph-based intelligence from Kumo.ai's
    Graph Neural Network and produce concise, actionable case files.

    Your reports are used by compliance officers to make suspension/monitoring
    decisions. Be precise, cite specific numbers, and always include:
    1. Evidence summary (what the GNN detected)
    2. Financial impact estimate
    3. Clear recommendation (Suspend / Monitor / Escalate / Clear)

    Use professional but direct language. Flag urgency when warranted.
    Format your output as structured markdown.
""")


# ── Data Loading ──────────────────────────────────────────────────────────────

def load_partner_context(partner_id: str) -> dict:
    """Gather all context about a partner for the LLM."""
    # Load tables
    partners = pd.read_csv(TRANSFORMED_DIR / "partners.csv")
    clients = pd.read_csv(TRANSFORMED_DIR / "clients.csv")
    trades = pd.read_csv(TRANSFORMED_DIR / "trades.csv")
    commissions = pd.read_csv(TRANSFORMED_DIR / "commissions.csv")
    referrals = pd.read_csv(TRANSFORMED_DIR / "referrals.csv")
    fraud_rings = json.loads((TRANSFORMED_DIR / "fraud_rings.json").read_text())

    # Partner info
    partner = partners[partners["partner_id"] == partner_id]
    if partner.empty:
        raise ValueError(f"Partner {partner_id} not found")
    partner_row = partner.iloc[0].to_dict()

    # Their clients
    partner_clients = clients[clients["partner_id"] == partner_id]
    n_clients = len(partner_clients)
    n_fraud_clients = partner_clients["is_in_fraud_ring"].sum() if "is_in_fraud_ring" in partner_clients.columns else 0
    client_ids = partner_clients["client_id"].tolist()

    # Their trades
    partner_trades = trades[trades["partner_id"] == partner_id]
    n_trades = len(partner_trades)
    total_volume = partner_trades["trade_volume"].sum()

    # Opposite trading stats
    n_opposite = 0
    if "is_opposite_trade" in partner_trades.columns:
        n_opposite = int(partner_trades["is_opposite_trade"].sum())

    # Bonus abuse stats
    n_bonus = 0
    if "is_bonus_abuse" in partner_trades.columns:
        n_bonus = int(partner_trades["is_bonus_abuse"].sum())

    # Commission stats
    partner_comms = commissions[commissions["partner_id"] == partner_id]
    total_commissions = partner_comms["commission_amount"].sum()

    # Referral stats
    partner_refs = referrals[referrals["partner_id"] == partner_id]

    # Temporal analysis: trade timing gaps
    timing_analysis = {}
    if n_opposite > 0:
        opp_trades = partner_trades[partner_trades["is_opposite_trade"] == True].copy()
        opp_trades["timestamp"] = pd.to_datetime(opp_trades["timestamp"])
        opp_trades = opp_trades.sort_values("timestamp")
        if len(opp_trades) >= 2:
            time_diffs = opp_trades["timestamp"].diff().dt.total_seconds().dropna()
            timing_analysis = {
                "min_gap_seconds": float(time_diffs.min()),
                "max_gap_seconds": float(time_diffs.max()),
                "median_gap_seconds": float(time_diffs.median()),
                "mean_gap_seconds": float(time_diffs.mean()),
            }

    # Fraud ring association
    ring_ids_str = partner_row.get("fraud_ring_ids", "")
    associated_rings = []
    if ring_ids_str:
        for rid_str in str(ring_ids_str).split(","):
            rid_str = rid_str.strip()
            if rid_str:
                try:
                    rid = int(rid_str)
                    ring = next((r for r in fraud_rings if r["ring_id"] == rid), None)
                    if ring:
                        associated_rings.append({
                            "ring_id": ring["ring_id"],
                            "pattern_type": ring["pattern_type"],
                            "num_accounts": len(ring["accounts"]),
                            "num_transactions": ring["num_transactions"],
                            "temporal_span": ring.get("temporal_span", []),
                        })
                except (ValueError, StopIteration):
                    pass

    # Load Kumo predictions if available
    kumo_score = None
    try:
        preds = pd.read_csv(PREDICTIONS_DIR / "enriched_predictions.csv")
        match = preds[preds["account_id"] == partner_id]
        if not match.empty:
            score_cols = [c for c in match.columns if "prob" in c.lower() or "score" in c.lower() or "predict" in c.lower()]
            if score_cols:
                kumo_score = float(match.iloc[0][score_cols[0]])
    except FileNotFoundError:
        pass

    context = {
        "partner_id": partner_id,
        "partner_info": {
            k: (v if not isinstance(v, float) or not pd.isna(v) else None)
            for k, v in partner_row.items()
        },
        "network_stats": {
            "num_referred_clients": n_clients,
            "num_fraud_ring_clients": int(n_fraud_clients),
            "num_trades": n_trades,
            "total_trade_volume": round(float(total_volume), 2),
            "total_commissions": round(float(total_commissions), 2),
            "num_opposite_trades": n_opposite,
            "num_bonus_abuse_trades": n_bonus,
            "num_referrals": len(partner_refs),
        },
        "timing_analysis": timing_analysis,
        "associated_fraud_rings": associated_rings,
        "kumo_risk_score": kumo_score,
    }

    return context


# ── LLM Report Generation ────────────────────────────────────────────────────

def generate_investigation_report(
    partner_id: str,
    context: dict | None = None,
    model: str = "gpt-4o",
) -> str:
    """Generate a fraud investigation report using Azure OpenAI.

    Args:
        partner_id: The partner to investigate.
        context: Pre-loaded context dict (if None, loads automatically).
        model: Azure OpenAI model/deployment to use.

    Returns:
        Markdown-formatted investigation report.
    """
    from openai import AzureOpenAI

    if context is None:
        context = load_partner_context(partner_id)

    client = AzureOpenAI(
        azure_endpoint=os.environ.get("AZURE_OPENAI_URL"),
        api_key=os.environ.get("AZURE_OPENAI_KEY"),
        api_version="2024-12-01-preview",
    )

    user_prompt = _build_user_prompt(context)

    response = client.chat.completions.create(
        model="azure-gpt-4o",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.3,
        max_tokens=1500,
    )

    report = response.choices[0].message.content
    return report


def _build_user_prompt(context: dict) -> str:
    """Build the user prompt from partner context data."""
    pid = context["partner_id"]
    info = context["partner_info"]
    stats = context["network_stats"]
    timing = context["timing_analysis"]
    rings = context["associated_fraud_rings"]
    kumo_score = context.get("kumo_risk_score")

    lines = [
        f"## Investigation Request: {pid}",
        "",
        f"**Entity:** {info.get('entity_name', 'Unknown')}",
        f"**Bank:** {info.get('bank_name', 'Unknown')}",
        f"**Account:** {info.get('account_number', 'Unknown')}",
        f"**Flagged as Fraudulent:** {info.get('is_fraudulent', False)}",
        "",
    ]

    if kumo_score is not None:
        lines.append(f"**Kumo GNN Risk Score:** {kumo_score:.4f}")
        lines.append("")

    lines += [
        "### Network Statistics",
        f"- Referred clients: {stats['num_referred_clients']}",
        f"- Clients in fraud rings: {stats['num_fraud_ring_clients']}",
        f"- Total trades: {stats['num_trades']}",
        f"- Total trade volume: ${stats['total_trade_volume']:,.2f}",
        f"- Total commissions earned: ${stats['total_commissions']:,.2f}",
        f"- Opposite trade events: {stats['num_opposite_trades']}",
        f"- Bonus abuse events: {stats['num_bonus_abuse_trades']}",
        "",
    ]

    if timing:
        lines += [
            "### Temporal Analysis (Opposite Trades)",
            f"- Min time gap between paired trades: {timing['min_gap_seconds']:.1f}s",
            f"- Median time gap: {timing['median_gap_seconds']:.1f}s",
            f"- Mean time gap: {timing['mean_gap_seconds']:.1f}s",
            "",
        ]

    if rings:
        lines.append("### Associated Fraud Rings")
        for ring in rings:
            lines.append(
                f"- Ring #{ring['ring_id']} ({ring['pattern_type']}): "
                f"{ring['num_accounts']} accounts, "
                f"{ring['num_transactions']} transactions"
            )
        lines.append("")

    lines += [
        "### Task",
        "Based on this graph intelligence, produce a structured investigation",
        "case file with:",
        "1. **Risk Assessment** (Critical / High / Medium / Low)",
        "2. **Evidence Summary** — 3 bullet points of the strongest signals",
        "3. **Financial Impact** — estimated fraudulent commission losses",
        "4. **Fraud Pattern** — describe the detected scheme in plain English",
        "5. **Recommendation** — Suspend / Monitor / Escalate / Clear",
        "6. **Suggested Actions** — immediate next steps for the compliance team",
    ]

    return "\n".join(lines)


# ── Batch Report Generation ──────────────────────────────────────────────────

def generate_batch_reports(
    top_n: int = 10,
    model: str = "gpt-4o",
) -> list[dict]:
    """Generate reports for top-N riskiest partners."""
    partners = pd.read_csv(TRANSFORMED_DIR / "partners.csv")

    # Prioritize: fraud partners first, then by total_trade_volume
    fraud_partners = partners[partners["is_fraudulent"] == True].sort_values(
        "total_trade_volume", ascending=False
    )
    top = fraud_partners.head(top_n)

    if len(top) < top_n:
        # Fill with highest-volume non-fraud partners
        non_fraud = partners[partners["is_fraudulent"] == False].sort_values(
            "total_trade_volume", ascending=False
        )
        top = pd.concat([top, non_fraud.head(top_n - len(top))])

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    reports = []

    for _, row in top.iterrows():
        pid = row["partner_id"]
        print(f"\n  Generating report for {pid}...")

        try:
            context = load_partner_context(pid)
            report = generate_investigation_report(pid, context=context, model=model)

            result = {
                "partner_id": pid,
                "entity_name": row.get("entity_name", ""),
                "is_fraudulent": bool(row.get("is_fraudulent", False)),
                "report": report,
                "context": context,
            }
            reports.append(result)

            # Save individual report
            path = REPORTS_DIR / f"{pid}_report.md"
            path.write_text(report)
            print(f"    Saved {path}")

        except Exception as e:
            print(f"    ERROR: {e}")
            reports.append({
                "partner_id": pid,
                "error": str(e),
            })

    # Save all reports as JSON
    summary_path = REPORTS_DIR / "all_reports.json"
    with open(summary_path, "w") as f:
        json.dump(reports, f, indent=2, default=str)
    print(f"\n  Saved summary: {summary_path}")

    return reports


# ── Quick Summary (no LLM needed) ────────────────────────────────────────────

def generate_quick_summary(partner_id: str) -> dict:
    """Generate a rule-based summary without calling the LLM.

    Useful for the demo API when you want instant responses.
    """
    context = load_partner_context(partner_id)
    stats = context["network_stats"]
    timing = context["timing_analysis"]
    rings = context["associated_fraud_rings"]
    info = context["partner_info"]
    kumo_score = context.get("kumo_risk_score")

    # Determine risk level
    risk_signals = 0
    if stats["num_opposite_trades"] > 0:
        risk_signals += 2
    if stats["num_bonus_abuse_trades"] > 0:
        risk_signals += 1
    if stats["num_fraud_ring_clients"] > 0:
        risk_signals += 2
    if len(rings) > 0:
        risk_signals += 2
    if timing and timing.get("min_gap_seconds", 999) < 10:
        risk_signals += 1

    if risk_signals >= 5:
        risk_level = "CRITICAL"
    elif risk_signals >= 3:
        risk_level = "HIGH"
    elif risk_signals >= 1:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    # Build evidence bullets
    evidence = []
    if stats["num_opposite_trades"] > 0:
        pct = stats["num_opposite_trades"] / max(stats["num_trades"], 1) * 100
        evidence.append(
            f"{stats['num_opposite_trades']} opposite/mirrored trades detected "
            f"({pct:.0f}% of all trades under this partner)"
        )
    if timing:
        evidence.append(
            f"Paired trades executed within {timing['min_gap_seconds']:.0f}–"
            f"{timing['max_gap_seconds']:.0f}s of each other"
        )
    if stats["num_fraud_ring_clients"] > 0:
        evidence.append(
            f"{stats['num_fraud_ring_clients']} of {stats['num_referred_clients']} "
            f"referred clients are in known fraud rings"
        )
    if stats["num_bonus_abuse_trades"] > 0:
        evidence.append(
            f"{stats['num_bonus_abuse_trades']} trades flagged as bonus abuse "
            f"(coordinated deposit–withdrawal pattern)"
        )
    if len(rings) > 0:
        ring_types = [r["pattern_type"] for r in rings]
        evidence.append(
            f"Associated with {len(rings)} fraud ring(s): {', '.join(ring_types)}"
        )
    if not evidence:
        evidence.append("No specific fraud signals detected in current data")

    # Financial impact
    estimated_loss = stats["total_commissions"]
    if stats["num_opposite_trades"] > 0:
        # Opposite trades are the main fraud — estimate loss from those
        opp_ratio = stats["num_opposite_trades"] / max(stats["num_trades"], 1)
        estimated_loss = stats["total_commissions"] * opp_ratio

    # Recommendation
    if risk_level == "CRITICAL":
        recommendation = "IMMEDIATE SUSPENSION"
        actions = [
            f"Suspend partner {partner_id} and freeze all referral commissions",
            f"Freeze {stats['num_fraud_ring_clients']} client accounts linked to fraud rings",
            "Escalate to regulatory compliance team",
            "Preserve all trade logs for forensic analysis",
        ]
    elif risk_level == "HIGH":
        recommendation = "SUSPENSION RECOMMENDED"
        actions = [
            f"Suspend partner {partner_id} pending full investigation",
            "Review all client accounts for coordinated behavior",
            "Flag for enhanced due diligence review",
        ]
    elif risk_level == "MEDIUM":
        recommendation = "ENHANCED MONITORING"
        actions = [
            "Place under enhanced transaction monitoring",
            "Review commission payout patterns weekly",
            "Flag any new opposite trading for immediate review",
        ]
    else:
        recommendation = "CLEAR — NO ACTION REQUIRED"
        actions = ["Continue standard monitoring"]

    return {
        "partner_id": partner_id,
        "entity_name": info.get("entity_name", "Unknown"),
        "risk_level": risk_level,
        "kumo_risk_score": kumo_score,
        "evidence": evidence,
        "financial_impact": {
            "total_commissions": round(stats["total_commissions"], 2),
            "estimated_fraudulent_loss": round(estimated_loss, 2),
            "total_trade_volume": round(stats["total_trade_volume"], 2),
        },
        "recommendation": recommendation,
        "suggested_actions": actions,
        "network_stats": stats,
        "timing_analysis": timing,
        "associated_fraud_rings": rings,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Deriv Guardian — Investigation Copilot")
    parser.add_argument("--partner", type=str, help="Partner ID to investigate (e.g. P_0002)")
    parser.add_argument("--batch", type=int, default=0, help="Generate batch reports for top N partners")
    parser.add_argument("--quick", action="store_true", help="Quick rule-based summary (no LLM)")
    parser.add_argument("--model", type=str, default="gpt-4o", help="Azure OpenAI model")
    args = parser.parse_args()

    if args.partner:
        if args.quick:
            print(f"Quick summary for {args.partner}:")
            summary = generate_quick_summary(args.partner)
            print(json.dumps(summary, indent=2))
        else:
            print(f"Generating LLM investigation report for {args.partner}...")
            report = generate_investigation_report(args.partner, model=args.model)
            print("\n" + report)

            # Save
            REPORTS_DIR.mkdir(parents=True, exist_ok=True)
            path = REPORTS_DIR / f"{args.partner}_report.md"
            path.write_text(report)
            print(f"\nSaved to {path}")

    elif args.batch > 0:
        print(f"Generating batch reports for top {args.batch} partners...")
        generate_batch_reports(top_n=args.batch, model=args.model)

    else:
        # Default: show all fraud partners' quick summaries
        print("Fraud Partner Summaries (Quick Mode)")
        print("=" * 60)
        partners = pd.read_csv(TRANSFORMED_DIR / "partners.csv")
        fraud = partners[partners["is_fraudulent"] == True].sort_values(
            "total_trade_volume", ascending=False
        )
        for _, row in fraud.head(10).iterrows():
            pid = row["partner_id"]
            try:
                summary = generate_quick_summary(pid)
                print(f"\n{'─' * 60}")
                print(f"Partner: {pid} | {summary['entity_name']}")
                print(f"Risk: {summary['risk_level']} | Recommendation: {summary['recommendation']}")
                print(f"Evidence:")
                for e in summary["evidence"]:
                    print(f"  • {e}")
                print(f"Est. Fraudulent Loss: ${summary['financial_impact']['estimated_fraudulent_loss']:,.2f}")
            except Exception as e:
                print(f"\n{pid}: ERROR — {e}")


if __name__ == "__main__":
    main()

