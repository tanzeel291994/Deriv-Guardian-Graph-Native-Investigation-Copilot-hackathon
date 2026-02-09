# Deriv Guardian: Graph-Native Investigation Copilot

> **Graph Transformer Anti-Partner Fraud Guard**
>
> A GNN-powered fraud detection system that analyzes partner-client network structure — not just transactions — to detect hidden fraud rings 2–3 weeks before rule-based systems. Kumo.ai identifies the "who," GenAI explains the "why" in 30 seconds.

---

## Key Results

| Metric | Partners (200) | All Accounts (2,358) |
|---|---|---|
| **Accuracy** | 96.5% | 95.7% |
| **Precision** | 94.2% | 96.4% |
| **Recall** | 95.6% | 92.2% |
| **F1 Score** | 94.9% | 94.2% |
| **AUC-ROC** | **99.4%** | **98.7%** |

- **65 out of 68** fraudulent partners detected
- Only **4 false positives** across 200 partners
- **$61M** in fraudulent commissions identified (60% of total)
- Early detection during **grooming phase** — weeks before rule-based systems

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DERIV GUARDIAN                               │
├──────────────────┬──────────────────┬───────────────────────────┤
│   THE BRAIN      │   THE EYES       │   THE NARRATOR            │
│   Kumo.ai RGT    │   React + Canvas │   Azure OpenAI GPT-4o    │
│                  │                  │                           │
│  Graph Neural    │  Network Graph   │  3-bullet investigation   │
│  Network learns  │  Timeline slider │  report generated in      │
│  fraud from      │  Bubble chart    │  seconds: Evidence,       │
│  relationships   │  Pattern Lab     │  Impact, Recommendation   │
└──────────────────┴──────────────────┴───────────────────────────┘
         ▲                                        ▲
         │                                        │
┌────────┴────────────────────────────────────────┴───────────────┐
│                   DATA ENGINEERING PIPELINE                      │
│  IBM AMLSim (5M txns) → Transform → Inject Fraud → Graph Export │
└─────────────────────────────────────────────────────────────────┘
```

---

## Graph Schema (Kumo.ai)

```
              accounts (2,358 NODES)
             ┌─────────┬──────────┐
             │ PARTNER  │  CLIENT  │
             │  (200)   │ (2,158)  │
             └────┬─────┴─────┬────┘
                  │           │
    ┌─────────────┼───────────┼──────────────┐
    │             │           │              │
 referrals   commissions    trades          │
 (2,158)      (2,252)      (2,473)          │
 Partner→     Client→      Client→          │
 Client       Partner      Market           │
    └─────────────┴───────────┴──────────────┘
                      │
              GNN Message Passing
              (2 hops through graph)
                      │
                      ▼
        PREDICT accounts.is_fraudulent
        FOR EACH accounts.account_id
```

**Nodes:** `accounts.csv` — 2,358 accounts (200 Partners + 2,158 Clients) with role, bank, entity, and `is_fraudulent` label.

**Edges:**
| Table | Count | Relationship | Key Features |
|---|---|---|---|
| `referrals.csv` | 2,158 | Partner → Client | referral_date (temporal) |
| `trades.csv` | 2,473 | Client → Market | timestamp, instrument, direction, volume, is_opposite_trade, is_bonus_abuse |
| `commissions.csv` | 2,252 | Client → Partner | timestamp, commission_amount, currency |

---

## Dataset

**Source:** IBM AMLSim (Anti-Money Laundering Simulation)
- 518,581 accounts
- 5,078,345 transactions (475 MB)
- 370 pre-labeled fraud rings across 8 pattern types

**Transformation Pipeline (4 stages):**

| Stage | Script | What It Does |
|---|---|---|
| 1. Parse Patterns | `parse_patterns.py` | Extract 370 fraud rings from Patterns.txt into structured JSON |
| 2. Transform | `transform.py` | Assign Partner/Client roles via in-degree analysis (≥15 unique senders → Partner), map to Deriv schema |
| 3. Inject Fraud | `inject_patterns.py` | Inject **Opposite Trading** (714 mirrored BUY/SELL pairs) and **Bonus Abuse** (221 coordinated deposits) |
| 4. Export Graph | `export_kumo.py` | Export unified graph schema for Kumo.ai (nodes + edges CSVs) |

**Transformed Dataset:**

| Table | Rows | Description |
|---|---|---|
| `partners.csv` | 200 | Top 200 partners by fan-in, 68 fraudulent (34%) |
| `clients.csv` | 2,158 | Referred clients, 828 in fraud rings (38%) |
| `trades.csv` | 2,473 | Trades with 7 instruments, 714 opposite + 221 bonus abuse |
| `commissions.csv` | 2,252 | Commission events at 5% of trade volume |
| `referrals.csv` | 2,158 | Partner → Client referral edges |
| `withdrawals.csv` | 221 | Bonus abuse withdrawal events |
| `fraud_rings.json` | 370 | Labeled fraud ring definitions |

**Fraud Patterns Detected:**
- GATHER-SCATTER (24 partners) · SCATTER-GATHER (23) · FAN-IN (19) · BIPARTITE (1) · CYCLE (1)

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data Pipeline | Python, Pandas, NumPy | 4-stage ETL: Parse → Transform → Inject → Export |
| Graph ML | **Kumo.ai (KumoRFM)** | Relational Graph Transformer, Predictive Query Language |
| GenAI | **Azure OpenAI (GPT-4o)** | AI Copilot investigation reports |
| Backend | **FastAPI** + Uvicorn | REST API serving graph data + predictions |
| Frontend | **React** + Vite + Canvas API | Network graph, timeline, bubble chart |
| Visualization | **react-force-graph-2d**, D3, Canvas | Force-directed graphs, animated topology lab |
| Deploy | **Docker** → Render.com | Single container, free hosting |

---

## Project Structure

```
├── pipeline/                    # Python backend
│   ├── run_pipeline.py          # Main CLI entrypoint (orchestrates all stages)
│   ├── config.py                # Configuration & paths
│   ├── parse_patterns.py        # Stage 1: Parse fraud rings from text
│   ├── transform.py             # Stage 2: Role assignment & schema mapping
│   ├── inject_patterns.py       # Stage 3: Inject opposite trading & bonus abuse
│   ├── export_kumo.py           # Stage 4: Export Kumo-ready CSVs
│   ├── kumo_predict.py          # Stage 5: Kumo.ai GNN predictions
│   ├── evaluate.py              # Stage 6: Model evaluation metrics
│   ├── copilot.py               # GenAI investigation report generator
│   └── api.py                   # FastAPI backend (serves API + frontend)
│
├── frontend/                    # React frontend
│   ├── src/
│   │   ├── App.jsx              # Main app with 3-tab navigation
│   │   ├── api.js               # API client
│   │   └── components/
│   │       ├── Dashboard.jsx    # Macro view: bubble chart + stats
│   │       ├── BubbleChart.jsx  # Risk landscape canvas visualization
│   │       ├── PatternBreakdown.jsx  # Attack vectors + model metrics
│   │       ├── PartnerList.jsx  # Partner selector sidebar
│   │       ├── GraphView.jsx    # Force-directed network graph
│   │       ├── Timeline.jsx     # Temporal intelligence slider
│   │       ├── Copilot.jsx      # AI investigation report sidebar
│   │       └── PatternLab.jsx   # Interactive fraud topology textbook
│   └── vite.config.js           # Vite config with API proxy
│
├── data/
│   ├── transformed/             # Stage 2-3 output (2.1 MB)
│   ├── kumo_export/             # Stage 4 output — graph CSVs (556 KB)
│   └── predictions/             # Stage 5-6 output — GNN results (484 KB)
│
├── Dockerfile                   # Multi-stage: Node build + Python serve
├── render.yaml                  # Render.com deploy config (free tier)
├── railway.json                 # Railway deploy config (alternative)
└── pyproject.toml               # Python dependencies
```

---

## Quick Start

### Prerequisites
- Python 3.12+
- Node.js 20+
- [Poetry](https://python-poetry.org/) for Python dependency management

### 1. Install Dependencies

```bash
# Python
poetry install

# Frontend
cd frontend && npm install && cd ..
```

### 2. Run the Data Pipeline

```bash
# Full pipeline: parse → transform → inject → export
poetry run python -m pipeline.run_pipeline
```

### 3. Run Kumo.ai Predictions (requires API key)

```bash
# Set your Kumo API key
export KUMO_API_KEY="your-key-here"

# Run GNN predictions
poetry run python -m pipeline.kumo_predict

# Evaluate model performance
poetry run python -m pipeline.evaluate
```

### 4. Start the Demo

```bash
# Terminal 1: Backend API
poetry run uvicorn pipeline.api:app --reload --port 8000

# Terminal 2: Frontend dev server
cd frontend && npm run dev
```

Open **http://localhost:5173** — the full demo with all 3 tabs.

### 5. Production Build

```bash
# Build frontend
cd frontend && npm run build && cd ..

# Run production (FastAPI serves both API + frontend)
poetry run uvicorn pipeline.api:app --host 0.0.0.0 --port 8000
```

Open **http://localhost:8000** — single-server production mode.

---

## Demo Flow (3 Scenes)

### Scene 1: The Alert (Dashboard → Investigate)
- Global fraud landscape bubble chart shows 200 partners
- Click a red bubble → network graph reveals the fraud ring
- *"Standard systems see 20 separate users. Our system sees ONE coordinated ring."*

### Scene 2: The Time-Travel (Timeline)
- Drag the slider through 28 days of trading data
- Watch the fraud evolve: Grooming → Escalation → Active Fraud → Full Exposure
- *"The GNN detected this ring in Week 1. Rule-based systems would catch it in Week 3."*

### Scene 3: The Copilot (AI Report)
- Click the Partner node in the graph
- AI generates: Evidence · Impact · Recommendation
- *"Investigation time: from hours to 30 seconds."*

### Bonus: Pattern Lab
- Interactive textbook with animated fraud topologies
- Fan-In, Fan-Out, Scatter-Gather, Cycle, Bipartite, Opposite Trading
- *"This is what the GNN is looking for — explained visually."*

---

## Deploy (Free)

### Render.com (Recommended)

1. Push to GitHub
2. Go to [render.com](https://render.com) → New Web Service → Connect repo
3. Render auto-detects the Dockerfile
4. Select **Free** plan → Deploy

The `render.yaml` blueprint auto-configures everything.

### Docker (Local)

```bash
docker build -t deriv-guardian .
docker run -p 8000:8000 deriv-guardian
```

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `KUMO_API_KEY` | For GNN predictions | Kumo.ai RFM API key |
| `AZURE_OPENAI_URL` | For LLM reports | Azure OpenAI endpoint |
| `AZURE_OPENAI_KEY` | For LLM reports | Azure OpenAI API key |
| `PORT` | For deploy | Server port (default: 8000) |

> **Note:** The quick-mode Copilot (rule-based summaries) works without any API keys. Pre-computed GNN predictions are included in the repo.

---

## License

Built for the Deriv Hackathon 2025.

