# NK Protein Executive CoPilot (v6.0)

An institutional-grade, AI-powered Business Intelligence platform designed for real-time manufacturing and supply chain analysis. This system leverages advanced machine learning (XGBoost, Prophet, Isolation Forest) and a modular "Brain & Body" architecture to provide high-fidelity insights to the CMD.

## 🏛️ System Architecture (v6.0 Modular)

The project is structured into three specialized packages for maximum maintainability and "Executive Monochrome" aesthetics:

- **`interface/` (The Body):** Premium UI components, Plotly dashboards, and CSS design tokens.
- **`brain/` (The Voice):** Multi-provider AI orchestration (OpenAI, Google, Claude, Ollama, Watsonx).
- **`hub/` (The Reasoning Engine):** Intent classification, SQL generation, data governance, and ML routing.
- **`models/` (The Analytics):** Domain-specific ML implementations (Sales, Cashflow, GST, Inventory, Profitability).

## 🚀 Key Features

- **Executive Monochrome UI:** A sharp, minimal, high-contrast interface designed for rapid decision-making.
- **Dual-Mode ML Pipelines:** Automated training (`train_pipeline.py`) with high-speed inference from cached models.
- **Multi-Pillar AI Intelligence:** Parallel execution of all business domains (Sales, AR, GST, Inventory) in a single query.
- **Model Health Monitoring:** Real-time MAPE tracking and accuracy badges in the sidebar.
- **Confidence Gating:** Automated reliability checks (Emerald/Amber/Red) to ensure data integrity.
- **Identity-Aware AI:** Multi-provider support including local, air-gapped LLMs (Ollama) for sensitive data.

## 🛠️ Setup Instructions

### 1. Prepare Environment
```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Create a `.env` file in the root with:
```env
# AI Orchestration
AI_PROVIDER_ORCHESTRATION_API_URL="your_url"
AI_PROVIDER_ORCHESTRATION_API_KEY="your_key"

# Database (MS SQL Server)
MSSQL_PASS="your_password"

# Optional: Enterprise Watsonx / Ollama
WATSONX_API_KEY="your_key"
WATSONX_PROJECT_ID="your_id"
```

### 3. Initialize Database & Models
```powershell
python db_setup.py         # Load raw CSVs into SQL Server
python train_pipeline.py   # Run initial ML training
```

### 4. Launch CoPilot
```powershell
streamlit run app.py
```

## 📊 Analytics & ML Logic

| Model | Algorithm | Purpose |
|---|---|---|
| **Sales** | XGBoost + Prophet | Multi-horizon revenue forecasting and trend detection. |
| **Cashflow** | Probabilistic Aging | AR risk scoring and expected inflow calculation. |
| **Compliance** | Isolation Forest | Anomaly detection in GST filings and ITC risk. |
| **Inventory** | Threshold Analytics | Dead-stock detection and capital-locked analysis. |
| **Segments** | KMeans Clustering | Customer value segmentation and margin profiling. |

## 🛡️ Governance & Security
- **Data Sovereignty:** Local Python engines process all raw data; LLMs only receive sanitized summaries.
- **Model Gating:** Forecasts with >25% MAPE are automatically suppressed to prevent "noisy" decision-making.
- **Institutional Design:** No "AI slop" or emojis—designed for a professional boardroom environment.

---
*Developed for NK Protein as an Executive Business Intelligence POC.*
