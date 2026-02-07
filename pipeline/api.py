"""Phase 3C: FastAPI backend for the Deriv Guardian demo.

Endpoints:
    GET  /api/partners           — list all partners with risk scores
    GET  /api/partners/{id}      — partner detail + network stats
    GET  /api/partners/{id}/report — LLM investigation report
    GET  /api/partners/{id}/graph  — network graph data (nodes + edges)
    GET  /api/fraud-rings        — all detected fraud rings
    GET  /api/timeline           — temporal trade data for timeline viz
    GET  /api/stats              — dashboard summary stats

Usage:
    uvicorn pipeline.api:app --reload --port 8000

Requires:
    pip install fastapi uvicorn
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from pipeline.config import TRANSFORMED_DIR, KUMO_EXPORT_DIR
from pipeline.copilot import generate_quick_summary, generate_investigation_report, load_partner_context

app = FastAPI(
    title="Deriv Guardian API",
    description="Graph-Native Investigation Copilot for Partner & Affiliate Fraud",
    version="1.0.0",
)

# CORS — allow the React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Cached Data Loading ──────────────────────────────────────────────────────

_cache: dict = {}


def _load(name: str) -> pd.DataFrame:
    if name not in _cache:
        path = TRANSFORMED_DIR / f"{name}.csv"
        _cache[name] = pd.read_csv(path)
    return _cache[name]


def _load_kumo(name: str) -> pd.DataFrame:
    key = f"kumo_{name}"
    if key not in _cache:
        path = KUMO_EXPORT_DIR / f"{name}.csv"
        _cache[key] = pd.read_csv(path)
    return _cache[key]


def _load_fraud_rings() -> list[dict]:
    if "fraud_rings" not in _cache:
        path = TRANSFORMED_DIR / "fraud_rings.json"
        _cache["fraud_rings"] = json.loads(path.read_text())
    return _cache["fraud_rings"]


# ── Helper: serialize DataFrames safely ──────────────────────────────────────

def _df_to_records(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to list of dicts, handling NaN → None."""
    return json.loads(df.to_json(orient="records", date_format="iso"))


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/api/stats")
def get_dashboard_stats():
    """Dashboard summary statistics."""
    partners = _load("partners")
    clients = _load("clients")
    trades = _load("trades")
    commissions = _load("commissions")

    n_fraud_partners = int(partners["is_fraudulent"].sum())
    n_fraud_clients = int(clients["is_in_fraud_ring"].sum()) if "is_in_fraud_ring" in clients.columns else 0
    n_opposite = int(trades["is_opposite_trade"].sum()) if "is_opposite_trade" in trades.columns else 0
    n_bonus = int(trades["is_bonus_abuse"].sum()) if "is_bonus_abuse" in trades.columns else 0

    total_commissions = float(commissions["commission_amount"].sum())
    fraud_trades = trades[trades["is_fraudulent"] == True] if "is_fraudulent" in trades.columns else pd.DataFrame()
    fraud_volume = float(fraud_trades["trade_volume"].sum()) if not fraud_trades.empty else 0.0

    rings = _load_fraud_rings()

    return {
        "total_partners": len(partners),
        "fraud_partners": n_fraud_partners,
        "fraud_partner_pct": round(n_fraud_partners / max(len(partners), 1) * 100, 1),
        "total_clients": len(clients),
        "fraud_clients": n_fraud_clients,
        "total_trades": len(trades),
        "opposite_trades": n_opposite,
        "bonus_abuse_trades": n_bonus,
        "total_commissions": round(total_commissions, 2),
        "fraud_trade_volume": round(fraud_volume, 2),
        "total_fraud_rings": len(rings),
    }


@app.get("/api/partners")
def list_partners(
    fraud_only: bool = Query(False, description="Filter to fraud partners only"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """List partners with summary stats."""
    partners = _load("partners")

    if fraud_only:
        partners = partners[partners["is_fraudulent"] == True]

    partners = partners.sort_values("total_trade_volume", ascending=False)
    total = len(partners)
    page = partners.iloc[offset : offset + limit]

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "partners": _df_to_records(page),
    }


@app.get("/api/partners/{partner_id}")
def get_partner_detail(partner_id: str):
    """Full partner detail with network context and quick risk summary."""
    try:
        summary = generate_quick_summary(partner_id)
        return summary
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/api/partners/{partner_id}/report")
def get_partner_report(
    partner_id: str,
    quick: bool = Query(True, description="Use quick rule-based summary (True) or LLM (False)"),
    model: str = Query("gpt-4o", description="Azure OpenAI model to use"),
):
    """Generate investigation report for a partner."""
    try:
        if quick:
            summary = generate_quick_summary(partner_id)
            return summary
        else:
            report = generate_investigation_report(partner_id, model=model)
            return {"partner_id": partner_id, "report": report, "model": model}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Report generation failed: {e}")


@app.get("/api/partners/{partner_id}/graph")
def get_partner_graph(partner_id: str):
    """Network graph data for visualization (nodes + edges).

    Returns data formatted for React Force Graph / D3.
    """
    partners = _load("partners")
    clients = _load("clients")
    referrals = _load("referrals")
    trades = _load("trades")

    partner = partners[partners["partner_id"] == partner_id]
    if partner.empty:
        raise HTTPException(status_code=404, detail=f"Partner {partner_id} not found")

    partner_row = partner.iloc[0]

    # Get this partner's clients
    partner_clients = clients[clients["partner_id"] == partner_id]
    client_ids = partner_clients["client_id"].tolist()

    # Build nodes
    nodes = []

    # Central partner node
    nodes.append({
        "id": partner_id,
        "label": str(partner_row.get("entity_name", partner_id)),
        "type": "partner",
        "is_fraudulent": bool(partner_row.get("is_fraudulent", False)),
        "size": 20,
        "color": "#ef4444" if partner_row.get("is_fraudulent") else "#3b82f6",
    })

    # Client nodes
    for _, client in partner_clients.iterrows():
        is_fraud = bool(client.get("is_in_fraud_ring", False))
        nodes.append({
            "id": client["client_id"],
            "label": str(client.get("entity_name", client["client_id"])),
            "type": "client",
            "is_fraudulent": is_fraud,
            "size": 8,
            "color": "#f97316" if is_fraud else "#6b7280",
        })

    # Build edges (referrals)
    edges = []
    partner_refs = referrals[referrals["partner_id"] == partner_id]
    for _, ref in partner_refs.iterrows():
        edges.append({
            "source": partner_id,
            "target": ref["client_id"],
            "type": "referral",
        })

    # Add trade-based edges between clients (for opposite trading visualization)
    partner_trades = trades[
        (trades["partner_id"] == partner_id) &
        (trades.get("is_opposite_trade", pd.Series(dtype=bool)).fillna(False) if "is_opposite_trade" in trades.columns else False)
    ]

    if "is_opposite_trade" in trades.columns:
        opp_trades = trades[
            (trades["partner_id"] == partner_id) &
            (trades["is_opposite_trade"] == True)
        ].sort_values("timestamp")

        # Pair consecutive opposite trades as edges between their clients
        opp_list = opp_trades.to_dict("records")
        for i in range(0, len(opp_list) - 1, 2):
            t1 = opp_list[i]
            t2 = opp_list[i + 1]
            if t1["client_id"] != t2["client_id"]:
                edges.append({
                    "source": t1["client_id"],
                    "target": t2["client_id"],
                    "type": "opposite_trade",
                    "color": "#ef4444",
                })

    return {
        "partner_id": partner_id,
        "nodes": nodes,
        "edges": edges,
        "stats": {
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "fraud_clients": sum(1 for n in nodes if n.get("is_fraudulent") and n["type"] == "client"),
        },
    }


@app.get("/api/fraud-rings")
def list_fraud_rings(
    limit: int = Query(50, ge=1, le=500),
):
    """List detected fraud rings with summary info."""
    rings = _load_fraud_rings()

    # Return summary (without full transaction lists for performance)
    summaries = []
    for ring in rings[:limit]:
        summaries.append({
            "ring_id": ring["ring_id"],
            "pattern_type": ring["pattern_type"],
            "description": ring.get("description", ""),
            "hub_account": ring["hub_account"],
            "num_accounts": len(ring["accounts"]),
            "num_transactions": ring["num_transactions"],
            "temporal_span": ring.get("temporal_span", []),
        })

    return {
        "total": len(rings),
        "rings": summaries,
    }


@app.get("/api/fraud-rings/{ring_id}")
def get_fraud_ring(ring_id: int):
    """Get full details of a specific fraud ring."""
    rings = _load_fraud_rings()
    ring = next((r for r in rings if r["ring_id"] == ring_id), None)
    if ring is None:
        raise HTTPException(status_code=404, detail=f"Ring {ring_id} not found")

    return ring


@app.get("/api/timeline")
def get_timeline_data(
    partner_id: str | None = Query(None, description="Filter by partner ID"),
    fraud_only: bool = Query(False),
    limit: int = Query(10000, ge=1, le=50000),
):
    """Temporal trade data for timeline visualization.

    Returns trades sorted by timestamp for the timeline slider.
    """
    trades = _load("trades")

    if partner_id:
        trades = trades[trades["partner_id"] == partner_id]

    if fraud_only and "is_fraudulent" in trades.columns:
        trades = trades[trades["is_fraudulent"] == True]

    trades = trades.sort_values("timestamp").head(limit)

    # Group by date for histogram
    trades_copy = trades.copy()
    trades_copy["timestamp"] = pd.to_datetime(trades_copy["timestamp"])
    trades_copy["date"] = trades_copy["timestamp"].dt.date.astype(str)

    # Build aggregation dict dynamically to handle missing columns
    agg_dict = {"trade_count": ("trade_id", "count")}

    if "is_fraudulent" in trades_copy.columns:
        # Cast to int so sum works reliably (handles bool or string "True"/"False")
        trades_copy["_fraud_int"] = trades_copy["is_fraudulent"].astype(int)
        agg_dict["fraud_count"] = ("_fraud_int", "sum")

    if "is_opposite_trade" in trades_copy.columns:
        trades_copy["_opp_int"] = trades_copy["is_opposite_trade"].astype(int)
        agg_dict["opposite_count"] = ("_opp_int", "sum")

    if "is_bonus_abuse" in trades_copy.columns:
        trades_copy["_bonus_int"] = trades_copy["is_bonus_abuse"].astype(int)
        agg_dict["bonus_abuse_count"] = ("_bonus_int", "sum")

    agg_dict["total_volume"] = ("trade_volume", "sum")

    daily = trades_copy.groupby("date").agg(**agg_dict).reset_index()

    # Ensure columns exist even if absent
    for col in ("fraud_count", "opposite_count", "bonus_abuse_count"):
        if col not in daily.columns:
            daily[col] = 0

    # Convert aggregated int columns to plain int for JSON serialization
    for col in ("fraud_count", "opposite_count", "bonus_abuse_count"):
        daily[col] = daily[col].astype(int)

    return {
        "trades": _df_to_records(trades) if partner_id else [],  # skip full list for global
        "daily_summary": _df_to_records(daily),
        "date_range": {
            "min": str(trades_copy["timestamp"].min()),
            "max": str(trades_copy["timestamp"].max()),
        },
    }


@app.get("/api/partners/{partner_id}/clients")
def get_partner_clients(partner_id: str):
    """Get all clients referred by a partner with their trade stats."""
    clients = _load("clients")
    trades = _load("trades")

    partner_clients = clients[clients["partner_id"] == partner_id]
    if partner_clients.empty:
        raise HTTPException(status_code=404, detail=f"No clients for {partner_id}")

    # Enrich with trade stats
    client_ids = partner_clients["client_id"].tolist()
    client_trades = trades[trades["client_id"].isin(client_ids)]

    trade_stats = client_trades.groupby("client_id").agg(
        num_trades=("trade_id", "count"),
        total_volume=("trade_volume", "sum"),
    ).reset_index()

    if "is_opposite_trade" in client_trades.columns:
        opp_stats = client_trades.groupby("client_id")["is_opposite_trade"].sum().reset_index()
        opp_stats.columns = ["client_id", "opposite_trades"]
        trade_stats = trade_stats.merge(opp_stats, on="client_id", how="left")

    enriched = partner_clients.merge(trade_stats, on="client_id", how="left")

    return {
        "partner_id": partner_id,
        "total_clients": len(enriched),
        "clients": _df_to_records(enriched),
    }


# ── Macro View: Global Fraud Landscape ───────────────────────────────────────

def _load_predictions() -> pd.DataFrame:
    """Load enriched predictions (from Kumo GNN)."""
    if "predictions" not in _cache:
        from pipeline.config import PREDICTIONS_DIR
        path = PREDICTIONS_DIR / "enriched_predictions.csv"
        if path.exists():
            _cache["predictions"] = pd.read_csv(path)
        else:
            _cache["predictions"] = pd.DataFrame()
    return _cache["predictions"]


@app.get("/api/macro")
def get_macro_view():
    """Global Fraud Landscape — bubble chart + pattern breakdown data.

    Returns:
      - bubble_chart: one point per partner (x=clients, y=opp_ratio, size=volume, color=risk)
      - pattern_breakdown: counts by fraud pattern type
      - risk_distribution: how many partners at each risk level
      - attack_vectors: opposite trades vs bonus abuse vs clean breakdown
      - model_performance: high-level Kumo GNN accuracy stats
    """
    partners = _load("partners")
    trades = _load("trades")
    commissions = _load("commissions")
    predictions = _load_predictions()
    rings = _load_fraud_rings()

    # ── Bubble Chart Data ────────────────────────────────────────────────
    # One point per partner: x = num_clients, y = opposite_trade_ratio,
    # size = trade_volume, color determined by risk tier
    trade_stats = trades.groupby("partner_id").agg(
        num_trades=("trade_id", "count"),
        total_volume=("trade_volume", "sum"),
    ).reset_index()

    opp_ratio = pd.DataFrame()
    if "is_opposite_trade" in trades.columns:
        trades_copy = trades.copy()
        trades_copy["_opp_int"] = trades_copy["is_opposite_trade"].astype(int)
        opp_ratio = trades_copy.groupby("partner_id").agg(
            opposite_count=("_opp_int", "sum"),
        ).reset_index()

    bubble_df = partners[["partner_id", "entity_name", "num_referred_clients",
                           "is_fraudulent", "primary_pattern_type"]].copy()
    bubble_df = bubble_df.merge(trade_stats, on="partner_id", how="left")
    if not opp_ratio.empty:
        bubble_df = bubble_df.merge(opp_ratio, on="partner_id", how="left")
        bubble_df["opposite_count"] = bubble_df["opposite_count"].fillna(0)
        bubble_df["opp_ratio"] = (
            bubble_df["opposite_count"] / bubble_df["num_trades"].clip(lower=1)
        )
    else:
        bubble_df["opp_ratio"] = 0.0
        bubble_df["opposite_count"] = 0

    # Merge Kumo fraud_score if available
    if not predictions.empty and "fraud_score" in predictions.columns:
        partner_preds = predictions[predictions["role"] == "PARTNER"][
            ["account_id", "fraud_score"]
        ].rename(columns={"account_id": "partner_id"})
        bubble_df = bubble_df.merge(partner_preds, on="partner_id", how="left")
    else:
        bubble_df["fraud_score"] = None

    bubble_df["total_volume"] = bubble_df["total_volume"].fillna(0)
    bubble_df["num_trades"] = bubble_df["num_trades"].fillna(0).astype(int)
    bubble_df["num_referred_clients"] = bubble_df["num_referred_clients"].fillna(0).astype(int)

    # Risk tier
    def _risk_tier(row):
        score = row.get("fraud_score")
        if score is not None and not pd.isna(score):
            if score >= 0.8: return "CRITICAL"
            if score >= 0.5: return "HIGH"
            if score >= 0.3: return "MEDIUM"
            return "LOW"
        if row["is_fraudulent"]: return "HIGH"
        if row["opp_ratio"] > 0.3: return "MEDIUM"
        return "LOW"

    bubble_df["risk_tier"] = bubble_df.apply(_risk_tier, axis=1)

    bubbles = _df_to_records(bubble_df[[
        "partner_id", "entity_name", "num_referred_clients", "opp_ratio",
        "total_volume", "num_trades", "opposite_count", "is_fraudulent",
        "primary_pattern_type", "fraud_score", "risk_tier",
    ]])

    # ── Pattern Breakdown ────────────────────────────────────────────────
    fraud_partners = partners[partners["is_fraudulent"] == True]
    pattern_counts = (
        fraud_partners["primary_pattern_type"]
        .fillna("UNKNOWN")
        .value_counts()
        .to_dict()
    )

    # ── Risk Distribution ────────────────────────────────────────────────
    risk_dist = bubble_df["risk_tier"].value_counts().to_dict()

    # ── Attack Vectors (trade-level breakdown) ───────────────────────────
    total_trades = len(trades)
    n_opp = int(trades["is_opposite_trade"].sum()) if "is_opposite_trade" in trades.columns else 0
    n_bonus = int(trades["is_bonus_abuse"].sum()) if "is_bonus_abuse" in trades.columns else 0
    n_fraud = int(trades["is_fraudulent"].sum()) if "is_fraudulent" in trades.columns else 0
    n_clean = total_trades - n_fraud

    attack_vectors = {
        "total_trades": total_trades,
        "clean_trades": n_clean,
        "opposite_trades": n_opp,
        "bonus_abuse_trades": n_bonus,
        "other_fraud_trades": max(0, n_fraud - n_opp - n_bonus),
    }

    # ── Ring Pattern Summary ─────────────────────────────────────────────
    ring_pattern_counts = {}
    for ring in rings:
        pt = ring.get("pattern_type", "UNKNOWN")
        ring_pattern_counts[pt] = ring_pattern_counts.get(pt, 0) + 1

    # ── Model Performance Summary ────────────────────────────────────────
    model_perf = None
    from pipeline.config import PREDICTIONS_DIR
    eval_path = PREDICTIONS_DIR / "evaluation_report.json"
    if eval_path.exists():
        model_perf = json.loads(eval_path.read_text())

    return {
        "bubble_chart": bubbles,
        "pattern_breakdown": pattern_counts,
        "risk_distribution": risk_dist,
        "attack_vectors": attack_vectors,
        "ring_patterns": ring_pattern_counts,
        "model_performance": model_perf,
        "summary": {
            "total_partners": len(partners),
            "fraud_partners": int(fraud_partners.shape[0]),
            "total_fraud_rings": len(rings),
            "total_trades": total_trades,
            "fraud_trade_volume": round(float(
                trades[trades["is_fraudulent"] == True]["trade_volume"].sum()
            ) if "is_fraudulent" in trades.columns else 0, 2),
        },
    }


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok", "service": "deriv-guardian"}


# ── Static Frontend Serving (Production) ─────────────────────────────────────
# In production, FastAPI serves the built React app.
# The frontend dist directory is mounted AFTER all /api routes so it acts as fallback.

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend" / "dist"

if FRONTEND_DIR.is_dir():
    # Serve static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIR / "assets")), name="static-assets")

    # Serve any other static files (favicon, etc.)
    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        """Catch-all: serve the React SPA for any non-API route."""
        # Try to serve the exact file first (e.g. vite.svg)
        file_path = FRONTEND_DIR / full_path
        if full_path and file_path.is_file():
            return FileResponse(str(file_path))
        # Otherwise serve index.html (SPA client-side routing)
        return FileResponse(str(FRONTEND_DIR / "index.html"))


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

