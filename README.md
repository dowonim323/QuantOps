# 📈 QuantOps
> **Automated Trading System for Korea Investment & Securities (KIS)**

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-2.0%2B-black?logo=flask&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

**QuantOps** is an all-in-one quantitative trading platform designed for the Korean stock market. It seamlessly integrates financial data acquisition, algorithmic stock selection, automated trade execution, and real-time performance monitoring into a unified, high-performance system.

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
- **Auto-Rebalancing**: Maintains optimal portfolio allocation automatically.

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

5.  **Access the Dashboard**
    Open your browser and navigate to `http://localhost:80`.

---

## 📂 Project Structure

```
QuantOps/
├── 📂 pykis/               # KIS API Wrapper Library
├── 📂 web/                 # Web Dashboard Application
├── 📂 tools/               # Utility Scripts & Helpers
├── 📄 financial_crawler.py # Financial Data Collection Script
├── 📄 stock_selection.py   # Quantitative Selection Script
├── 📄 autorebalance.py     # Automated Trading Script
└── 📄 README.md            # Project Documentation
```

---

## ⚠️ Disclaimer
This software is for educational and research purposes only. **Trading stocks involves significant risk.** The developers are not responsible for any financial losses incurred while using this system. Use at your own risk.
