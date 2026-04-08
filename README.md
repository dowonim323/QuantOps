# 📈 QuantOps
> **Automated Trading System for Korea Investment & Securities (KIS)**

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-2.0%2B-black?logo=flask&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

**QuantOps** is an all-in-one quantitative trading platform designed for the Korean stock market, utilizing the **Korea Investment & Securities (KIS) REST API**. It seamlessly integrates financial data acquisition, algorithmic stock selection, automated trade execution, and real-time performance monitoring into a unified, high-performance system.

---

## ✨ Key Features

### 🔍 Intelligent Data Crawler
- **Automated Collection**: Scrapes comprehensive financial statements and market data.
- **Data Normalization**: Cleans and processes raw data for accurate analysis.

### 📊 Quantitative Strategy
- **Algorithmic Selection**: Filters stocks based on advanced metrics (PER, PBR, ROE, etc.).
- **Scoring System**: Ranks potential investments to identify top-tier opportunities.

### 🤖 Automated Trading Bot
- **Smart Execution**: Auto-executes buy/sell orders with precision.
- **Risk Management**: Built-in position sizing and stop-loss mechanisms.
- **Trading Session Automation**: Orchestrates market-open execution and end-of-day handling automatically.
- **Retry Logic**: Automatically retries unfilled orders when orderbook prices are unfavorable.
- **Discord Notifications**: Real-time alerts for order status and unfilled orders.
- **Supervisor-Native Scheduling**: Runs nightly preparation and trading-day control as long-lived supervisor-managed controllers.

### 🖥️ Professional Dashboard
- **Real-time Monitoring**: Track total assets, daily returns, and profit/loss at a glance.
- **Portfolio Analytics**: Visual breakdown of holdings and asset allocation.
- **Secure Access**: Password-protected interface with a guest mode for safe sharing.

---

## 🛠️ Tech Stack

- **Core**: Python 3.10
- **Web**: Flask, Gunicorn
- **Frontend**: HTML5, TailwindCSS, Chart.js
- **Database**: SQLite
- **Infrastructure**: Docker, Docker Compose

---

## 🚀 Getting Started

### Prerequisites
- **Docker** & **Docker Compose** installed.
- **KIS API Keys** (Korea Investment & Securities).
- **Anaconda** or **Miniconda** (recommended for local development).

### 🐍 Environment Setup (Conda)
For local development, use the `quantops` environment:

```bash
# Create and activate environment
conda create -n quantops python=3.10
conda activate quantops

# Install dependencies
pip install pandas numpy requests websocket-client flask flask-login python-dotenv werkzeug fake-useragent tqdm colorlog cryptography
```

### 📦 Installation & Deployment

1.  **Clone the Repository**
    ```bash
    git clone https://github.com/dowonim323/QuantOps.git
    cd QuantOps
    ```

2.  **Configure Secrets**
    Create a `secrets/` directory and add your KIS API keys.

3.  **Set Dashboard Password**
    ```bash
    python web/set_password.py
    ```

4.  **Launch with Docker**
    ```bash
    docker compose up -d --build
    ```

    This integrated compose setup starts both the web dashboard and the supervisor-managed scheduler container.

    Dashboard-only alternatives:
    ```bash
    docker compose -f web/docker-compose.yml up -d --build
    ```

    Or from the `web/` directory:
    ```bash
    docker compose up -d --build
    ```

5.  **Access the Dashboard**
    Open your browser and navigate to `http://localhost:15000`.

---

## 🧭 Runtime Architecture

QuantOps currently runs as two containers in the root `docker-compose.yml` setup:

- **`web`**: Flask dashboard on port `15000`
- **`scheduler`**: a dedicated container that starts `supervisord`

The scheduler container no longer depends on cron. `supervisord` directly manages two long-lived controller processes:

- **`nightly-prep-controller`** (`python -m pipelines.nightly_prep_controller`)
  - Runs the nightly preparation window in KST
  - Executes `financial_crawler` first and `stock_selection` second
  - Persists progress so selection can resume if crawler already completed

- **`trading-day-controller`** (`python -m pipelines.trading_day_controller`)
  - Launches the daily trading session once per account/day
  - Preserves manual-review blocks for incomplete or abnormal prior sessions
  - Allows sell-capable sessions to start even when saved selections are unavailable
  - Persists launch mode and heartbeat metadata for operator visibility

The VMQ strategy keeps its existing sell behavior. If saved selections are missing, trading-day launch is still allowed and selection-dependent buy/rebalance paths are skipped at runtime. This degraded mode is recorded as `launch_mode=degraded_sell_only` in scheduler state.

---

## 🛎️ Scheduler Operations

Inspect the current scheduler state:

```bash
python -m pipelines.scheduler_admin status
python -m pipelines.scheduler_admin status --run-date 2026-04-08
```

If the trading controller blocks a day for manual review, clear it explicitly after operator inspection:

```bash
python -m pipelines.scheduler_admin clear-trading-review --run-date 2026-04-08 --account-id krx_vmq
```

The persisted trading-day state now exposes:

- `status` and `phase`
- `launch_mode` and `launch_reason`
- `last_heartbeat_at`
- unresolved manual-review entries via `pending_manual_reviews`

---

## 📂 Project Structure

```
QuantOps/
├── 📂 pykis/                   # KIS API Wrapper Library
├── 📂 strategies/              # Strategy registry and runtime strategy logic
├── 📂 web/                     # Web Dashboard Application
├── 📂 scheduler/               # Scheduler container image and supervisord config
├── 📂 tools/                   # Utility Scripts, persistence helpers, scheduler state
├── 📁 pipelines/
│   ├── 📄 financial_crawler.py       # Financial data collection pipeline
│   ├── 📄 stock_selection.py         # Quantitative stock selection pipeline
│   ├── 📄 trading_session.py         # Trading session runner
│   ├── 📄 nightly_prep_controller.py # Supervisor-managed nightly prep controller
│   ├── 📄 trading_day_controller.py  # Supervisor-managed trading-day controller
│   └── 📄 scheduler_admin.py         # Scheduler state inspection/admin CLI
├── 📄 docker-compose.yml       # Integrated web + scheduler deployment
└── 📄 README.md            # Project Documentation
```

---

## 🙏 Acknowledgements
This project references significant portions of code from [python-kis](https://github.com/Soju06/python-kis) by Soju06. We extend our gratitude for the excellent open-source contribution which served as a foundation for the API wrapper implementation.

---

## ⚠️ Disclaimer
This software is for educational and research purposes only. **Trading stocks involves significant risk.** The developers are not responsible for any financial losses incurred while using this system. Use at your own risk.
