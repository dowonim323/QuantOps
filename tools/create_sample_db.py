import sqlite3
import random
from datetime import date, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_DIR = BASE_DIR / "db" / "account"
DB_PATH = DB_DIR / "sample_daily_assets.db"

def init_db():
    DB_DIR.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()
        
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE daily_assets (
                date TEXT PRIMARY KEY,
                initial_asset REAL,
                final_asset REAL,
                deposit_d2 REAL DEFAULT 0.0
            )
        """)
        conn.execute("""
            CREATE TABLE daily_orders (
                order_number TEXT PRIMARY KEY,
                date TEXT,
                time TEXT,
                type TEXT,
                name TEXT,
                qty INTEGER,
                executed_qty INTEGER,
                price REAL,
                status TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE daily_stock_performance (
                date TEXT,
                symbol TEXT,
                name TEXT,
                invested_amount REAL,
                current_value REAL,
                realized_profit REAL,
                sell_amount REAL DEFAULT 0.0,
                quantity INTEGER DEFAULT 0,
                PRIMARY KEY (date, symbol)
            )
        """)

def generate_data():
    start_date = date.today() - timedelta(days=30)
    initial_capital = 10_000_000.0
    current_capital = initial_capital
    
    stocks = [
        {"symbol": "005930", "name": "SamsungElec"},
        {"symbol": "000660", "name": "SKHynix"},
        {"symbol": "035420", "name": "NAVER"},
        {"symbol": "035720", "name": "Kakao"},
    ]
    
    held_stocks = {} # symbol: {qty, avg_price}
    
    with sqlite3.connect(DB_PATH) as conn:
        for i in range(31):
            curr_date = start_date + timedelta(days=i)
            date_str = curr_date.strftime("%Y-%m-%d")
            
            # 1. Daily Assets
            daily_initial = current_capital
            
            # Simulate market movement
            market_change = random.uniform(-0.02, 0.02)
            
            # Update held stocks value
            current_stock_value = 0
            stock_performances = []
            
            for symbol, info in held_stocks.items():
                # Price fluctuation
                curr_price = info["avg_price"] * (1 + random.uniform(-0.03, 0.03))
                val = curr_price * info["qty"]
                current_stock_value += val
                
                stock_performances.append({
                    "symbol": symbol,
                    "name": stocks[[s["symbol"] for s in stocks].index(symbol)]["name"],
                    "invested_amount": info["avg_price"] * info["qty"], # Simplified
                    "current_value": val,
                    "realized_profit": 0.0,
                    "sell_amount": 0.0
                })
            
            # Cash is what's left
            cash = current_capital - current_stock_value # This logic is a bit circular, let's simplify
            # Actually, let's just track total asset
            current_capital *= (1 + market_change)
            
            conn.execute(
                "INSERT INTO daily_assets (date, initial_asset, final_asset, deposit_d2) VALUES (?, ?, ?, ?)",
                (date_str, daily_initial, current_capital, random.uniform(100000, 500000))
            )
            
            # 2. Random Trade
            if i > 0 and random.random() < 0.3: # 30% chance to trade
                action = random.choice(["buy", "sell"])
                stock = random.choice(stocks)
                price = 50000 * (1 + random.uniform(-0.1, 0.1))
                qty = random.randint(1, 10)
                
                # Simplified order logging
                conn.execute("""
                    INSERT INTO daily_orders (order_number, date, time, type, name, qty, executed_qty, price, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    f"{date_str.replace('-', '')}{i}",
                    date_str,
                    "10:00:00",
                    action,
                    stock["name"],
                    qty,
                    qty,
                    price,
                    "체결"
                ))
                
                # Update performance if needed (simplified)
                
            # 3. Stock Performance
            # Just insert some dummy performance data
            for idx, stock in enumerate(stocks):
                if random.random() < 0.5 or i == 30: # Ensure data for last day
                    invested = random.uniform(1000000, 5000000)
                    current = invested * (1 + random.uniform(-0.1, 0.1))
                    
                    # Force one stock to have 0 quantity on the last day
                    qty = random.randint(10, 100)
                    if i == 30 and idx == 3: # Make the 4th stock (Kakao) have 0 quantity on the last day
                        qty = 0
                        current = 0 # Value is 0 if qty is 0
                    
                    conn.execute("""
                        INSERT INTO daily_stock_performance (date, symbol, name, invested_amount, current_value, realized_profit, sell_amount, quantity)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        date_str,
                        stock["symbol"],
                        stock["name"],
                        invested,
                        current,
                        random.uniform(-50000, 50000),
                        random.uniform(0, 1000000),
                        qty
                    ))

if __name__ == "__main__":
    init_db()
    generate_data()
    print(f"Sample DB created at {DB_PATH}")
