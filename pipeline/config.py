from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
RAW_DATA_DIR = Path("data/ibm-transactions-for-anti-money-laundering-aml/versions/8")
TRANSFORMED_DIR = Path("data/transformed")
KUMO_EXPORT_DIR = Path("data/kumo_export")
PREDICTIONS_DIR = Path("data/predictions")
REPORTS_DIR = Path("data/reports")

TRANS_FILE = "HI-Small_Trans.csv"
ACCOUNTS_FILE = "HI-Small_accounts.csv"
PATTERNS_FILE = "HI-Small_Patterns.txt"

# ── Role assignment ────────────────────────────────────────────────────────────
PARTNER_MIN_IN_DEGREE = 15        # min unique senders to qualify as "Partner"
TOP_N_PARTNERS = 200              # cap for demo tractability

# ── Commission ─────────────────────────────────────────────────────────────────
COMMISSION_RATE = 0.05            # 5% of trade volume
COMMISSION_DELAY_MINUTES = 60     # commission paid 1 hour after trade

# ── Sampling ───────────────────────────────────────────────────────────────────
SAMPLE_TRANSACTIONS = 500_000     # None for full dataset

# ── Instruments for synthetic trades ───────────────────────────────────────────
INSTRUMENTS = ["EURUSD", "GBPJPY", "BTCUSD", "XAUUSD", "US100", "AUDCAD", "USDJPY"]

# ── Fraud injection ────────────────────────────────────────────────────────────
OPPOSITE_TRADE_PROBABILITY = 0.40   # 80% of fraud ring trades are opposite ,# Harder to detect
BONUS_ABUSE_DEPOSIT = 50.0          # minimum deposit to trigger bonus
BONUS_ABUSE_WITHDRAW_DELAY_HOURS = 24

# ── Random seed ────────────────────────────────────────────────────────────────
SEED = 42
