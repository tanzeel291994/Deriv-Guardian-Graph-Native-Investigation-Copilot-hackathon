"""Phase 3A: Kumo.ai integration — build graph, run PQL, get fraud predictions.

Uses KumoRFM (Relational Foundation Model) for zero-shot fraud prediction.
No training step required — the foundation model does in-context learning
on your graph structure.

Usage:
    export KUMO_API_KEY="your-rfm-api-key"
    python -m pipeline.kumo_predict

Requires:
    pip install kumoai
    Get a free API key at https://kumorfm.ai
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.config import KUMO_EXPORT_DIR, TRANSFORMED_DIR


# ── Paths ────────────────────────────────────────────────────────────────────
PREDICTIONS_DIR = Path("data/predictions")


# ── Monkey-patch: fix numpy 2.x read-only array bug in Kumo SDK ─────────────
# The SDK's PQueryPandasExecutor.execute() uses ``mask &= _mask`` which fails
# with numpy ≥ 2.0 because certain arrays are returned read-only.
# We replace all in-place ``&=`` with ``mask = mask & ...``.
def _patch_kumo_pandas_executor() -> None:
    """Patch PQueryPandasExecutor.execute to avoid read-only array errors."""
    from kumoai.experimental.rfm.pquery.pandas_executor import (
        PQueryPandasExecutor,
    )
    from kumoapi.pquery import ValidatedPredictiveQuery
    from kumoapi.pquery.AST import (
        Aggregation, Column, Condition, Filter, Join, LogicalOperation,
    )

    def _patched_execute(
        self,
        query: ValidatedPredictiveQuery,
        feat_dict: dict[str, pd.DataFrame],
        time_dict: dict[str, pd.Series],
        batch_dict: dict[str, np.ndarray],
        anchor_time: pd.Series,
        num_forecasts: int = 1,
    ) -> tuple[pd.Series, np.ndarray]:
        if isinstance(query.entity_ast, Column):
            out, mask = self.execute_column(
                column=query.entity_ast, feat_dict=feat_dict, filter_na=True,
            )
        else:
            assert isinstance(query.entity_ast, Filter)
            out, mask = self.execute_filter(
                filter=query.entity_ast, feat_dict=feat_dict,
                time_dict=time_dict, batch_dict=batch_dict,
                anchor_time=anchor_time,
            )

        # Ensure mask is writable
        mask = np.array(mask, copy=True)

        if isinstance(query.target_ast, Column):
            out, _mask = self.execute_column(
                column=query.target_ast, feat_dict=feat_dict, filter_na=True,
            )
        elif isinstance(query.target_ast, Condition):
            out, _mask = self.execute_condition(
                condition=query.target_ast, feat_dict=feat_dict,
                time_dict=time_dict, batch_dict=batch_dict,
                anchor_time=anchor_time, filter_na=True,
                num_forecasts=num_forecasts,
            )
        elif isinstance(query.target_ast, Aggregation):
            out, _mask = self.execute_aggregation(
                aggr=query.target_ast, feat_dict=feat_dict,
                time_dict=time_dict, batch_dict=batch_dict,
                anchor_time=anchor_time, filter_na=True,
                num_forecasts=num_forecasts,
            )
        elif isinstance(query.target_ast, Join):
            out, _mask = self.execute_join(
                join=query.target_ast, feat_dict=feat_dict,
                time_dict=time_dict, batch_dict=batch_dict,
                anchor_time=anchor_time, filter_na=True,
                num_forecasts=num_forecasts,
            )
        elif isinstance(query.target_ast, LogicalOperation):
            out, _mask = self.execute_logical_operation(
                logical_operation=query.target_ast, feat_dict=feat_dict,
                time_dict=time_dict, batch_dict=batch_dict,
                anchor_time=anchor_time, filter_na=True,
                num_forecasts=num_forecasts,
            )
        else:
            raise NotImplementedError(
                f'{type(query.target_ast)} compilation missing.')

        if query.whatif_ast is not None:
            if isinstance(query.whatif_ast, Condition):
                mask = mask & self.execute_condition(
                    condition=query.whatif_ast, feat_dict=feat_dict,
                    time_dict=time_dict, batch_dict=batch_dict,
                    anchor_time=anchor_time, filter_na=True,
                    num_forecasts=num_forecasts,
                )[0]
            elif isinstance(query.whatif_ast, LogicalOperation):
                mask = mask & self.execute_logical_operation(
                    logical_operation=query.whatif_ast, feat_dict=feat_dict,
                    time_dict=time_dict, batch_dict=batch_dict,
                    anchor_time=anchor_time, filter_na=True,
                    num_forecasts=num_forecasts,
                )[0]
            else:
                raise ValueError(
                    f'Unsupported ASSUMING condition {type(query.whatif_ast)}')

        out = out[mask[_mask]]
        mask = mask & _mask         # was: mask &= _mask (read-only crash)
        out = out.reset_index(drop=True)
        return out, mask

    PQueryPandasExecutor.execute = _patched_execute

_patch_kumo_pandas_executor()


# ── Step 1: Load Kumo-formatted data ─────────────────────────────────────────

def load_kumo_tables() -> dict[str, pd.DataFrame]:
    """Load the 4 Kumo-export CSVs into DataFrames.

    Note: pandas 3.0 defaults string columns to ``str`` (StringDtype) which
    the Kumo SDK does not recognise.  We cast them to ``object`` so that
    ``infer_dtype`` sees the legacy ``object`` dtype it expects.
    """
    tables = {}
    for name in ["accounts", "trades", "commissions", "referrals"]:
        path = KUMO_EXPORT_DIR / f"{name}.csv"
        df = pd.read_csv(path)

        # Fix pandas 3.0 StringDtype → object for Kumo compatibility
        for col in df.columns:
            if pd.api.types.is_string_dtype(df[col]) and str(df[col].dtype) not in (
                "object", "string", "string[python]", "string[pyarrow]",
            ):
                df[col] = df[col].astype(object)

        # Convert bool columns to int (0/1) to avoid numpy 2.x read-only
        # array issues inside Kumo SDK and to get binary classification
        # instead of multi-class.
        for col in df.columns:
            if pd.api.types.is_bool_dtype(df[col]):
                df[col] = df[col].astype(int)

        # Parse timestamp columns
        for col in df.columns:
            if "timestamp" in col.lower() or "date" in col.lower():
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # .copy() ensures all numpy backing arrays are writable
        # (avoids numpy 2.x read-only array errors in Kumo SDK)
        tables[name] = df.copy()
        print(f"  Loaded {name}: {len(df):,} rows, {list(df.columns)}")
    return tables


# ── Step 2: Build KumoRFM graph ──────────────────────────────────────────────

def build_kumo_graph(tables: dict[str, pd.DataFrame]):
    """Build a KumoRFM Graph from the 4 tables using Graph.from_data().

    Graph schema (auto-inferred):
      - accounts (Node table, PK: account_id)
      - referrals (Edge: partner_account_id → client_account_id via accounts)
      - trades (Event on client_account_id, timestamp: timestamp)
      - commissions (Event on partner_account_id, timestamp: timestamp)

    The RFM model automatically infers:
      - Primary keys per table
      - Foreign key links between tables (via matching column names)
      - Temporal columns (timestamp)
      - Data types and semantic types
    """
    from kumoai.experimental.rfm import Graph

    # Graph.from_data auto-infers metadata and links from column names
    graph = Graph.from_data(
        df_dict={
            "accounts": tables["accounts"],
            "trades": tables["trades"],
            "commissions": tables["commissions"],
            "referrals": tables["referrals"],
        },
        infer_metadata=True,
        verbose=True,
    )

    return graph


# ── Step 3: Run Predictive Queries ───────────────────────────────────────────

def run_fraud_prediction(model, tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Predict which accounts are fraudulent using the GNN.

    PQL requires fully-qualified column names: table.column
    Entity IDs must be passed explicitly via ``indices``.

    The GNN learns from multi-hop patterns:
    - Partner → referred Clients → coordinated Trades → Commissions
    - Opposite trading correlations between sibling clients
    - Temporal commission patterns
    """
    pql = "PREDICT accounts.is_fraudulent FOR EACH accounts.account_id"

    # Pass all account IDs as explicit entity indices
    all_account_ids = tables["accounts"]["account_id"].tolist()
    print(f"  Running PQL: {pql}")
    print(f"  Predicting for {len(all_account_ids)} accounts (batched)...")

    with model.batch_mode(batch_size="max"):
        predictions_df = model.predict(
            query=pql,
            indices=all_account_ids,
            num_hops=2,
            explain=False,
        )

    print(f"  Got {len(predictions_df)} predictions")
    return predictions_df


def run_partner_risk_prediction(model, tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Predict fraud risk for PARTNER accounts specifically.

    Filters to partner role so we get targeted risk scores for
    the investigation dashboard.
    """
    pql = "PREDICT accounts.is_fraudulent FOR EACH accounts.account_id"

    # Only predict for PARTNER accounts
    partner_ids = tables["accounts"][
        tables["accounts"]["role"] == "PARTNER"
    ]["account_id"].tolist()

    print(f"  Running partner risk PQL: {pql}")
    print(f"  Predicting for {len(partner_ids)} partners...")

    with model.batch_mode(batch_size="max"):
        predictions_df = model.predict(
            query=pql,
            indices=partner_ids,
            num_hops=2,
            explain=False,
        )

    print(f"  Got {len(predictions_df)} partner risk predictions")
    return predictions_df


def run_explained_prediction(model, partner_id: str) -> dict:
    """Get an explained prediction for a single partner.

    Returns prediction + natural language explanation of why
    the GNN flagged this partner.
    """
    from kumoai.experimental.rfm import ExplainConfig

    pql = "PREDICT accounts.is_fraudulent FOR EACH accounts.account_id"
    print(f"  Running explained prediction for {partner_id}...")

    explanation = model.predict(
        query=pql,
        indices=[partner_id],
        explain=ExplainConfig(
            include_summary=True,
            include_details=True,
        ),
        num_hops=2,
    )

    result = {
        "partner_id": partner_id,
        "prediction": explanation.prediction.to_dict(orient="records"),
    }

    if hasattr(explanation, "summary") and explanation.summary is not None:
        result["summary"] = explanation.summary
    if hasattr(explanation, "details") and explanation.details is not None:
        result["details"] = str(explanation.details)

    return result


# ── Step 4: Enrich predictions with context ──────────────────────────────────

def _pivot_kumo_predictions(predictions_df: pd.DataFrame) -> pd.DataFrame:
    """Convert Kumo's multi-row-per-entity output to one row per account.

    Kumo returns columns: ENTITY, ANCHOR_TIMESTAMP, CLASS, SCORE, PREDICTED
    with 2 rows per entity (one per class).  We pivot to get:
        account_id | fraud_score | predicted_fraudulent
    """
    # Keep only the fraud-class (CLASS == 1) row for each entity
    fraud_rows = predictions_df[predictions_df["CLASS"] == 1].copy()
    fraud_rows = fraud_rows.rename(columns={
        "ENTITY": "account_id",
        "SCORE": "fraud_score",
        "PREDICTED": "predicted_fraudulent",
    })
    fraud_rows = fraud_rows.drop(columns=["CLASS", "ANCHOR_TIMESTAMP"], errors="ignore")
    return fraud_rows.reset_index(drop=True)


def enrich_predictions(
    predictions_df: pd.DataFrame,
    tables: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Merge Kumo predictions with account metadata and fraud ring context."""
    accounts = tables["accounts"]
    trades = tables["trades"]
    commissions = tables["commissions"]
    referrals = tables["referrals"]

    # Pivot Kumo's raw output to one row per account
    pivoted = _pivot_kumo_predictions(predictions_df)

    # Merge predictions with account info
    enriched = pivoted.merge(accounts, on="account_id", how="left")

    # Add network stats for each account
    partner_stats = _compute_partner_stats(referrals, trades, commissions)
    enriched = enriched.merge(partner_stats, on="account_id", how="left")

    # Sort by fraud score (descending)
    enriched = enriched.sort_values("fraud_score", ascending=False)

    return enriched


def _compute_partner_stats(
    referrals: pd.DataFrame,
    trades: pd.DataFrame,
    commissions: pd.DataFrame,
) -> pd.DataFrame:
    """Compute network statistics for each partner account."""
    # Referral count per partner
    ref_stats = referrals.groupby("partner_account_id").agg(
        num_referred_clients=("client_account_id", "nunique"),
    ).reset_index().rename(columns={"partner_account_id": "account_id"})

    # Commission stats per partner
    comm_stats = commissions.groupby("partner_account_id").agg(
        total_commissions=("commission_amount", "sum"),
        avg_commission=("commission_amount", "mean"),
        num_commission_events=("commission_id", "count"),
    ).reset_index().rename(columns={"partner_account_id": "account_id"})

    # Trade stats — opposite trading ratio per partner (via client linkage)
    client_partner = referrals[["partner_account_id", "client_account_id"]].drop_duplicates()

    trades_with_partner = trades.merge(
        client_partner,
        left_on="client_account_id",
        right_on="client_account_id",
        how="inner",
    )

    if "is_opposite_trade" in trades_with_partner.columns:
        trade_stats = trades_with_partner.groupby("partner_account_id").agg(
            total_trades=("trade_id", "count"),
            opposite_trade_count=("is_opposite_trade", "sum"),
            total_trade_volume=("trade_volume", "sum"),
        ).reset_index()
        trade_stats["opposite_trade_ratio"] = (
            trade_stats["opposite_trade_count"] / trade_stats["total_trades"]
        ).fillna(0)
        trade_stats = trade_stats.rename(columns={"partner_account_id": "account_id"})
    else:
        trade_stats = trades_with_partner.groupby("partner_account_id").agg(
            total_trades=("trade_id", "count"),
            total_trade_volume=("trade_volume", "sum"),
        ).reset_index().rename(columns={"partner_account_id": "account_id"})
        trade_stats["opposite_trade_ratio"] = 0.0

    # Merge all stats
    stats = ref_stats.merge(comm_stats, on="account_id", how="outer")
    stats = stats.merge(trade_stats, on="account_id", how="outer")
    return stats


# ── Step 5: Save predictions ─────────────────────────────────────────────────

def save_predictions(
    predictions_df: pd.DataFrame,
    enriched_df: pd.DataFrame,
) -> dict[str, Path]:
    """Save raw and enriched predictions."""
    PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)

    outputs = {}

    path = PREDICTIONS_DIR / "raw_predictions.csv"
    predictions_df.to_csv(path, index=False)
    outputs["raw"] = path
    print(f"  Saved {path} ({len(predictions_df)} rows)")

    path = PREDICTIONS_DIR / "enriched_predictions.csv"
    enriched_df.to_csv(path, index=False)
    outputs["enriched"] = path
    print(f"  Saved {path} ({len(enriched_df)} rows)")

    # Also save top-risk partners as JSON for the copilot
    if "role" in enriched_df.columns:
        partners = enriched_df[enriched_df["role"] == "PARTNER"].head(50)
    else:
        partners = enriched_df.head(50)
    risk_list = partners.to_dict(orient="records")
    path = PREDICTIONS_DIR / "top_risk_partners.json"
    with open(path, "w") as f:
        json.dump(risk_list, f, indent=2, default=str)
    outputs["top_risk"] = path
    print(f"  Saved {path} ({len(risk_list)} top-risk partners)")

    return outputs


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_kumo_predictions() -> dict[str, pd.DataFrame]:
    """Full Kumo RFM prediction pipeline."""
    from kumoai.experimental.rfm import KumoRFM
    from kumoai.experimental.rfm import init as rfm_init

    # Initialize KumoRFM (NOT kumoai.init — that's for enterprise platform)
    api_key = os.environ.get("KUMO_API_KEY")
    if not api_key:
        raise ValueError(
            "KUMO_API_KEY environment variable not set. "
            "Get a free key at https://kumorfm.ai"
        )

    print("Initializing KumoRFM...")
    rfm_init(api_key=api_key)
    print("  KumoRFM initialized ✓")

    print("\nLoading Kumo-formatted tables...")
    tables = load_kumo_tables()

    print("\nBuilding Kumo graph...")
    graph = build_kumo_graph(tables)

    print("\nCreating KumoRFM model...")
    model = KumoRFM(graph=graph)

    print("\nRunning fraud predictions on all accounts...")
    predictions = run_fraud_prediction(model, tables)

    print("\nEnriching predictions with network context...")
    enriched = enrich_predictions(predictions, tables)

    print("\nSaving predictions...")
    save_predictions(predictions, enriched)

    return {"predictions": predictions, "enriched": enriched}


if __name__ == "__main__":
    run_kumo_predictions()
