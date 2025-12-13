# QuantOps

**QuantOps** is a comprehensive automated trading system designed for the Korean stock market (KIS). It integrates financial data crawling, quantitative stock selection, automated trading execution, and a web-based performance dashboard into a unified platform.

## Features

### 1. Financial Data Crawler
-   **Automated Collection**: Scrapes financial statements and market data.
-   **Data Processing**: Cleans and normalizes data for analysis.

### 2. Quantitative Stock Selection
-   **Algorithm**: Selects stocks based on predefined quantitative metrics (e.g., PER, PBR, ROE).
-   **Ranking**: Scores and ranks stocks to identify the best investment opportunities.

### 3. Automated Trading
-   **Execution**: Automatically executes buy/sell orders based on selection results.
-   **Risk Management**: Implements position sizing and stop-loss logic.
-   **Rebalancing**: Periodically rebalances the portfolio to maintain target allocations.

### 4. Web Dashboard
-   **Performance Monitoring**: Visualizes total assets, returns, and daily profit/loss.
-   **Portfolio View**: Displays current holdings, asset allocation, and detailed metrics.
-   **History**: Tracks all order executions and trading history.
-   **Secure Access**: Password-protected login for sensitive data, with a guest mode for public sharing.

## Tech Stack
-   **Language**: Python 3.10+
-   **Web Framework**: Flask
-   **Database**: SQLite
-   **Frontend**: HTML5, TailwindCSS, Chart.js
-   **Deployment**: Docker & Docker Compose

## Getting Started

### Prerequisites
-   Docker & Docker Compose
-   Korea Investment & Securities (KIS) API Keys

### Installation
1.  **Clone the repository**:
    ```bash
    git clone https://github.com/yourusername/QuantOps.git
    cd QuantOps
    ```

2.  **Setup Secrets**:
    Create a `secrets/` directory and add your API keys (refer to `secrets/example.json` if available).

3.  **Setup Password**:
    ```bash
    python web/set_password.py
    ```

4.  **Run with Docker**:
    ```bash
    docker compose up -d --build
    ```

5.  **Access Dashboard**:
    Open `http://localhost:80` (or your configured port) in a browser.

## License
This project is licensed under the MIT License.
