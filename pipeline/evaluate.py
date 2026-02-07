"""Phase 3B: Model evaluation — measure Kumo GNN fraud-detection performance.

Compares Kumo predictions against ground-truth ``is_fraudulent`` labels and
prints classification metrics + saves a JSON report.

Usage:
    python -m pipeline.evaluate                  # defaults to enriched_predictions.csv
    python -m pipeline.evaluate --csv path.csv   # custom predictions file
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.config import PREDICTIONS_DIR


# ── Metrics helpers (no sklearn needed) ───────────────────────────────────────

def _confusion_matrix(
    y_true: np.ndarray, y_pred: np.ndarray,
) -> dict[str, int]:
    tp = int(((y_true == 1) & (y_pred == 1)).sum())
    tn = int(((y_true == 0) & (y_pred == 0)).sum())
    fp = int(((y_true == 0) & (y_pred == 1)).sum())
    fn = int(((y_true == 1) & (y_pred == 0)).sum())
    return {"tp": tp, "tn": tn, "fp": fp, "fn": fn}


def _classification_metrics(cm: dict[str, int]) -> dict[str, float]:
    tp, tn, fp, fn = cm["tp"], cm["tn"], cm["fp"], cm["fn"]
    total = tp + tn + fp + fn
    accuracy = (tp + tn) / total if total else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {
        "accuracy": round(accuracy, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
    }


def _auc_roc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Compute AUC-ROC via the trapezoidal rule (no sklearn)."""
    # Sort by descending score
    order = np.argsort(-y_score)
    y_true_sorted = y_true[order]

    pos = y_true.sum()
    neg = len(y_true) - pos
    if pos == 0 or neg == 0:
        return 0.0

    tpr_prev, fpr_prev = 0.0, 0.0
    tp_count, fp_count = 0, 0
    auc = 0.0
    prev_score = None

    for i in range(len(y_true_sorted)):
        score = y_score[order[i]]
        if score != prev_score and prev_score is not None:
            tpr = tp_count / pos
            fpr = fp_count / neg
            # Trapezoid area
            auc += 0.5 * (fpr - fpr_prev) * (tpr + tpr_prev)
            tpr_prev, fpr_prev = tpr, fpr
        if y_true_sorted[i] == 1:
            tp_count += 1
        else:
            fp_count += 1
        prev_score = score

    # Final point (fpr=1, tpr=1)
    tpr = tp_count / pos
    fpr = fp_count / neg
    auc += 0.5 * (fpr - fpr_prev) * (tpr + tpr_prev)
    return round(auc, 4)


# ── Evaluation for one cohort ─────────────────────────────────────────────────

def evaluate_cohort(
    df: pd.DataFrame,
    label: str = "All Accounts",
) -> dict:
    """Run full evaluation on a DataFrame with fraud_score, predicted_fraudulent, is_fraudulent."""
    y_true = df["is_fraudulent"].astype(int).values
    y_pred = df["predicted_fraudulent"].astype(int).values
    y_score = df["fraud_score"].astype(float).values

    cm = _confusion_matrix(y_true, y_pred)
    metrics = _classification_metrics(cm)
    auc = _auc_roc(y_true, y_score)

    return {
        "cohort": label,
        "total": len(df),
        "actual_fraud": int(y_true.sum()),
        "predicted_fraud": int(y_pred.sum()),
        "confusion_matrix": cm,
        **metrics,
        "auc_roc": auc,
    }


# ── Pretty-print ──────────────────────────────────────────────────────────────

def _print_eval(result: dict) -> None:
    cm = result["confusion_matrix"]
    tp, tn, fp, fn = cm["tp"], cm["tn"], cm["fp"], cm["fn"]
    pct = lambda v: f"{v * 100:.1f}%"

    print(f"\n╔{'═' * 58}╗")
    print(f"║  {result['cohort']:^54}  ║")
    print(f"╠{'═' * 58}╣")
    print(f"║  Total:              {result['total']:>6,}                              ║")
    print(f"║  Actual fraudulent:  {result['actual_fraud']:>6,}  "
          f"({result['actual_fraud'] / result['total'] * 100:.1f}%)"
          f"{'':>21}║")
    print(f"║  Predicted fraud:    {result['predicted_fraud']:>6,}  "
          f"({result['predicted_fraud'] / result['total'] * 100:.1f}%)"
          f"{'':>21}║")
    print(f"╠{'═' * 58}╣")
    print(f"║  {'Confusion Matrix':^54}  ║")
    print(f"║  {'':>18}Predicted 0    Predicted 1{'':>13}║")
    print(f"║  Actual 0        {tn:>6,}  (TN)    {fp:>6,}  (FP){'':>12}║")
    print(f"║  Actual 1        {fn:>6,}  (FN)    {tp:>6,}  (TP){'':>12}║")
    print(f"╠{'═' * 58}╣")
    print(f"║  Accuracy:   {pct(result['accuracy']):>7}                                   ║")
    print(f"║  Precision:  {pct(result['precision']):>7}                                   ║")
    print(f"║  Recall:     {pct(result['recall']):>7}                                   ║")
    print(f"║  F1 Score:   {pct(result['f1_score']):>7}                                   ║")
    print(f"║  AUC-ROC:    {pct(result['auc_roc']):>7}                                   ║")
    print(f"╚{'═' * 58}╝")


def _print_top_bottom(df: pd.DataFrame) -> None:
    cols = ["account_id", "role", "fraud_score", "predicted_fraudulent",
            "is_fraudulent"]
    extra = [c for c in ["opposite_trade_ratio", "num_referred_clients"]
             if c in df.columns]
    show = cols + extra

    print("\n── Top 10 highest-risk accounts ──")
    print(df.nlargest(10, "fraud_score")[show].to_string(index=False))

    print("\n── Bottom 10 lowest-risk accounts ──")
    print(df.nsmallest(10, "fraud_score")[show].to_string(index=False))

    fp_df = df[(df["is_fraudulent"] == 0) & (df["predicted_fraudulent"])]
    fn_df = df[(df["is_fraudulent"] == 1) & (~df["predicted_fraudulent"])]

    if len(fp_df):
        print(f"\n── False Positives ({len(fp_df)} total, top 5) ──")
        print(fp_df.nlargest(5, "fraud_score")[show].to_string(index=False))

    if len(fn_df):
        print(f"\n── False Negatives ({len(fn_df)} total, top 5) ──")
        print(fn_df.nsmallest(5, "fraud_score")[show].to_string(index=False))


# ── Main ──────────────────────────────────────────────────────────────────────

def run_evaluation(csv_path: Path | None = None) -> dict:
    """Load predictions and evaluate model performance."""
    if csv_path is None:
        csv_path = PREDICTIONS_DIR / "enriched_predictions.csv"

    print(f"Loading predictions from {csv_path}...")
    df = pd.read_csv(csv_path)
    print(f"  {len(df):,} rows, columns: {list(df.columns)}")

    required = {"fraud_score", "predicted_fraudulent", "is_fraudulent"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # ── All accounts ──
    all_result = evaluate_cohort(df, label="All Accounts")
    _print_eval(all_result)

    # ── Partners only ──
    results = {"all_accounts": all_result}
    if "role" in df.columns:
        partners = df[df["role"] == "PARTNER"]
        if len(partners):
            partner_result = evaluate_cohort(partners, label=f"Partners Only ({len(partners)})")
            _print_eval(partner_result)
            results["partners"] = partner_result

        clients = df[df["role"] == "CLIENT"]
        if len(clients):
            client_result = evaluate_cohort(clients, label=f"Clients Only ({len(clients)})")
            _print_eval(client_result)
            results["clients"] = client_result

    # ── Spot-checks ──
    _print_top_bottom(df)

    # ── Save report ──
    PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = PREDICTIONS_DIR / "evaluation_report.json"
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nSaved evaluation report to {report_path}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate Kumo GNN predictions")
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Path to enriched predictions CSV (default: data/predictions/enriched_predictions.csv)",
    )
    args = parser.parse_args()
    run_evaluation(csv_path=args.csv)

