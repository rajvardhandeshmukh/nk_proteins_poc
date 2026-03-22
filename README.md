# NK Protein AI Command Center

An advanced, AI-powered SAP business intelligence dashboard designed for real-time manufacturing and supply chain analysis. This platform leverages XGBoost, Facebook Prophet, and Isolation Forest algorithms to provide actionable insights directly from SAP datasets.

## Features

- **Sales Forecasting:** Predictive revenue modeling using a hybrid XGBoost + Prophet engine.
- **Cash Flow Analysis:** Real-time AR aging and DSO (Days Sales Outstanding) tracking.
- **Inventory Optimization:** Automated dead-stock detection and reorder point (ROP) alerting.
- **GST Reconciliation:** AI-driven anomaly detection (Isolation Forest) for Input Tax Credit (ITC) risk.
- **Profitability Analysis:** Customer segmentation (K-Means) and unit economics monitoring.
- **AI Chatbot:** A natural language interface to query business data and generate executive reports.

## Modular Architecture

The project is structured for high maintainability and scalability:

- **/data:** Houses all raw SAP-exported CSV datasets.
- **/models:** Contains independent, domain-specific AI modules:
  - `sales.py`: Forecasting and trend analysis.
  - `cashflow.py`: AR aging and collection probability.
  - `inventory.py`: Stock health and capital-locked calculations.
  - `gst.py`: Reconciliation and risk flagging.
  - `profitability.py`: Clustering and margin analysis.
  - `__init__.py`: Orchestrates the `load_all()` logic and result caching.
- `app.py`: Streamlit-based executive dashboard.
- `chatbot.py`: NLP routing and report generation engine.

## Setup Instructions

### 1. Prerequisites
- Python 3.8 or higher installed on your system.
- Access to a terminal (PowerShell, CMD, or Bash).

### 2. Clone and Prepare Environment
```powershell
# Create and activate a virtual environment
python -m venv venv
.\venv\Scripts\activate

# Install required dependencies
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Create a `.env` file in the root directory with the following keys:
```env
AI_PROVIDER_ORCHESTRATION_API_URL="your_api_url"
AI_PROVIDER_ORCHESTRATION_API_KEY="your_api_key"
```

### 4. Run the Application
```powershell
streamlit run app.py
```

## Troubleshooting: Prophet/Stan Backend Error
If you see an error related to "Stan backend" or "Prophet installation" on Windows:
1.  **Ensure you have a C++ compiler** installed (e.g., Mingw-w64 via MSYS2).
2.  **Manually install cmdstan**:
    ```powershell
    python -c "import cmdstanpy; cmdstanpy.install_cmdstan()"
    ```
3.  **Fallback Mode:** The application is designed to automatically detect if Prophet is broken and will fall back to a robust XGBoost-only forecast to ensure the dashboard remains functional.

## Usage

- **Dashboard:** View high-level metrics across Sales, Operations, and Finance in the main grid.
- **Interactive Charts:** Hover over the Plotly visualizations to see specific data points and confidence intervals.
- **Quick Questions:** Use the "Quick Question" buttons in the sidebar or main panel to trigger predefined NLP routes.
- **Executive Reports:** Ask the chatbot to "Generate a full executive report" for a consolidated business health summary.

## Data Security & Persistence
- **Zero-Trust AI Logic:** The AI never sees raw SAP data; it only interprets high-level insights from local Python engines.
- **Session Privacy:** Chat history is not persisted across sessions to ensure executive privacy. Refreshing the browser starts a clean session.
- **Local Analysis:** All calculations are performed on-premise within the secure `/data` sandbox.

## Deployment Options
Though currently running locally, this POC is designed for easy cloud deployment:
1. **Streamlit Cloud:** Direct integration with GitHub for instant hosting.
2. **Dockerization:** Create a container for deployment on AWS ECS, GCP Cloud Run, or Azure App Service.
3. **Enterprise VPC:** Host on-premise or within a private cloud using Nginx/Apache as a reverse proxy.

---
*Developed for NK Protein as an Executive Business Intelligence POC.*
