from flask import Flask, render_template, jsonify, request, session
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
import sqlite3
from pathlib import Path
from datetime import datetime
import sys
import os
from dotenv import load_dotenv
from werkzeug.security import check_password_hash

# Load environment variables
load_dotenv()

# Add parent directory to path to allow importing tools
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from tools.selection_store import load_stock_selection
from tools.financial_db import get_stock_selection_db_path
from tools.trading_profiles import get_enabled_accounts, get_primary_selection_account
from strategies import get_strategy_definition

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # Change this to a random secret key

# Flask-Login Setup
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

class User(UserMixin):
    def __init__(self, id):
        self.id = id

@login_manager.user_loader
def load_user(user_id):
    return User(user_id)

@app.route('/login', methods=['POST'])
def login():
    data = request.json
    password = data.get('password')
    
    # Get password hash from environment variable
    password_hash = os.getenv('DASHBOARD_PASSWORD_HASH')
    
    if password_hash and check_password_hash(password_hash, password):
        user = User(id='admin')
        login_user(user)
        return jsonify({'success': True})
    return jsonify({'success': False, 'message': 'Invalid password'}), 401

@app.route('/logout', methods=['POST'])
@login_required
def logout():
    logout_user()
    return jsonify({'success': True})

@app.route('/api/auth/status')
def auth_status():
    return jsonify({'authenticated': current_user.is_authenticated})

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "account" / "daily_assets.db"
ACCOUNT_DB_DIR = BASE_DIR / "db" / "account"

ENABLED_ACCOUNTS = tuple(get_enabled_accounts())
ACCOUNT_MAP = {account.account_id: account for account in ENABLED_ACCOUNTS}
PRIMARY_SELECTION_ACCOUNT = get_primary_selection_account(list(ENABLED_ACCOUNTS))


def _resolve_account_id(raw_account_id):
    if raw_account_id in (None, "", "all"):
        return "all"

    account_id = str(raw_account_id)
    if account_id not in ACCOUNT_MAP:
        raise KeyError(f"Unknown account_id: {account_id}")

    return account_id


def _resolve_account_db_path(account_id):
    if account_id in (None, "", "default", "krx_vmq"):
        return DB_PATH

    return ACCOUNT_DB_DIR / f"daily_assets_{account_id}.db"


def get_db_connection(account_id=None):
    conn = sqlite3.connect(_resolve_account_db_path(account_id))
    conn.row_factory = sqlite3.Row
    return conn


def _get_selected_account_id():
    return _resolve_account_id(request.args.get("account_id", "all"))


def _iter_account_ids(selected_account_id):
    if selected_account_id == "all":
        return [account.account_id for account in ENABLED_ACCOUNTS]

    return [selected_account_id]


def _fetch_account_rows(query, *, selected_account_id, params=()):
    rows = []
    for account_id in _iter_account_ids(selected_account_id):
        db_path = _resolve_account_db_path(account_id)
        if not db_path.exists():
            continue

        with get_db_connection(account_id) as conn:
            fetched = conn.execute(query, params).fetchall()

        account = ACCOUNT_MAP[account_id]
        for row in fetched:
            item = dict(row)
            item["account_id"] = account_id
            item["account_display_name"] = account.display_name
            rows.append(item)

    return rows


def _aggregate_asset_rows(rows):
    grouped = {}
    for row in rows:
        date_key = row["date"]
        if date_key not in grouped:
            grouped[date_key] = {
                "date": date_key,
                "initial_asset": 0.0,
                "final_asset": 0.0,
                "deposit_d2": 0.0,
                "transfer_amount": 0.0,
            }

        grouped_row = grouped[date_key]
        for key in ("initial_asset", "final_asset", "deposit_d2", "transfer_amount"):
            grouped_row[key] += row.get(key, 0.0) or 0.0

    return [grouped[key] for key in sorted(grouped.keys())]


@app.route('/api/accounts')
def get_accounts():
    accounts = [{
        'account_id': 'all',
        'display_name': 'All Accounts',
    }]

    if current_user.is_authenticated:
        for account in ENABLED_ACCOUNTS:
            accounts.append({
                'account_id': account.account_id,
                'display_name': account.display_name,
            })

    return jsonify({'accounts': accounts})

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/assets')
def get_assets():
    selected_account_id = _get_selected_account_id()
    assets = _fetch_account_rows(
        'SELECT * FROM daily_assets ORDER BY date',
        selected_account_id=selected_account_id,
    )
    if selected_account_id == 'all':
        assets = _aggregate_asset_rows(assets)

    # Filter out incomplete days (where final_asset is None or 0)
    data = []
    for row in assets:
        if row['final_asset'] is not None and row['final_asset'] > 0:
            data.append(dict(row))
    
    # Calculate Cumulative Return
    cumulative_index = 1.0
    
    if len(data) > 0:
        data[0]['daily_return'] = 0.0
        data[0]['cumulative_return'] = 0.0
        
    for i in range(1, len(data)):
        prev = data[i-1]['final_asset'] or 0.0
        curr = data[i]['final_asset'] or 0.0
        transfer = data[i].get('transfer_amount', 0.0) or 0.0
        
        # Base asset for return calculation is previous close + net transfer
        base = prev + transfer
        
        # Profit is current close - base
        profit = curr - base
        
        daily_return = (profit / base) if base > 0 else 0.0
        
        # Update cumulative index
        cumulative_index *= (1 + daily_return)
        
        data[i]['daily_return'] = daily_return
        data[i]['cumulative_return'] = (cumulative_index - 1.0) * 100

    # Data Scrubbing for Guest Users
    if not current_user.is_authenticated:
        for d in data:
            d['final_asset'] = 0  # Mask Total Asset
            d['transfer_amount'] = 0 # Mask Transfers
            # daily_return and cumulative_return are percentages, so they are safe to show.

    return jsonify(data)

@app.route('/api/performance')
def get_performance():
    selected_account_id = _get_selected_account_id()
    perfs = _fetch_account_rows(
        'SELECT * FROM daily_stock_performance ORDER BY date, symbol',
        selected_account_id=selected_account_id,
    )
    assets = _fetch_account_rows(
        'SELECT date, final_asset FROM daily_assets WHERE final_asset IS NOT NULL',
        selected_account_id=selected_account_id,
    )

    asset_map = {}
    for row in assets:
        asset_map[row['date']] = asset_map.get(row['date'], 0.0) + (row['final_asset'] or 0.0)

    data = perfs
    
    # Data Scrubbing for Guest Users
    if not current_user.is_authenticated:
        for d in data:
            # Mask absolute values
            # Moved masking to end of loop to allow calculation of return/weight
            # Return % is calculated on frontend usually, but if we mask invested/current, frontend calc will fail.
            # We should calculate return % here if it's not in DB, or provide a 'masked_return' field?
            # The frontend uses: (current_value - invested_amount) / invested_amount.
            # If we set them to 0, return is NaN.
            # So we MUST calculate return rate here and send it, OR send fake values that result in correct return?
            # Fake values are risky. Better to send explicit return_rate if possible.
            # But frontend logic is complex.
            # Alternative: Send a special flag 'is_masked': True.
            # And let frontend handle display.
            # But we must NOT send real data.
            # So we calculate return rate here.
            
            invested = d['invested_amount']
            current = d['current_value']
            sell = d['sell_amount']
            date = d['date']
            
            # Calculate Return Rate
            if invested > 0:
                d['return_rate'] = ((current + sell) - invested) / invested * 100
            else:
                d['return_rate'] = 0
                
            # Calculate Weight (Portfolio Allocation)
            total_asset = asset_map.get(date, 0)
            if total_asset > 0:
                d['weight'] = (current / total_asset) * 100
            else:
                d['weight'] = 0
                
            # Now we can safely zero out the amounts
            d['invested_amount'] = 0
            d['current_value'] = 0
            d['quantity'] = 0
            d['average_price'] = 0
            d['current_price'] = 0 
            d['sell_amount'] = 0
            d['sell_quantity'] = 0
            
    return jsonify(data)

@app.route('/api/orders')
def get_orders():
    selected_account_id = _get_selected_account_id()
    data = _fetch_account_rows(
        'SELECT * FROM daily_orders ORDER BY date DESC, time DESC',
        selected_account_id=selected_account_id,
    )
    data.sort(key=lambda row: (row['date'], row['time']), reverse=True)
    
    if not current_user.is_authenticated:
        for d in data:
            d['price'] = 0
            d['qty'] = 0
            d['executed_qty'] = 0
            # Keep symbol, type, date, time
            
    return jsonify(data)

@app.route('/api/analytics')
def get_analytics():
    selected_account_id = _get_selected_account_id()
    assets = _fetch_account_rows(
        'SELECT * FROM daily_assets ORDER BY date ASC',
        selected_account_id=selected_account_id,
    )
    if selected_account_id == 'all':
        assets = _aggregate_asset_rows(assets)
    
    # Filter out incomplete days
    data = []
    for row in assets:
        if row['final_asset'] is not None and row['final_asset'] > 0:
            data.append(dict(row))
    
    if not data:
        return jsonify({})
    
    # Calculate Daily Returns
    for i in range(1, len(data)):
        prev = data[i-1]['final_asset'] or 0.0
        curr = data[i]['final_asset'] or 0.0
        transfer = data[i].get('transfer_amount', 0.0) or 0.0
        
        # Base asset for return calculation is previous close + net transfer
        base = prev + transfer
        
        # Profit is current close - base
        profit = curr - base
        
        data[i]['daily_return'] = (profit / base) if base > 0 else 0.0
        data[i]['daily_profit'] = profit
    
    if len(data) > 0:
        data[0]['daily_return'] = 0.0
        data[0]['daily_profit'] = 0.0

    if not current_user.is_authenticated:
        for d in data:
            d['final_asset'] = 0
            d['daily_profit'] = 0
            d['transfer_amount'] = 0

    # 1. MDD Calculation (Based on Cumulative Return)
    # We must use cumulative return index to filter out deposit/withdrawal effects
    cumulative_index = 1.0
    peak_index = 1.0
    mdd = 0.0
    drawdowns = []
    
    # Initialize first day
    if len(data) > 0:
        drawdowns.append({'date': data[0]['date'], 'drawdown': 0.0})

    for i in range(1, len(data)):
        # daily_return is already calculated above
        r = data[i].get('daily_return', 0.0)
        
        # Update cumulative index
        cumulative_index *= (1 + r)
        
        # Update Peak
        if cumulative_index > peak_index:
            peak_index = cumulative_index
            
        # Calculate Drawdown
        dd = (cumulative_index - peak_index) / peak_index if peak_index > 0 else 0.0
        
        drawdowns.append({'date': data[i]['date'], 'drawdown': dd * 100})
        
        if dd < mdd:
            mdd = dd

    # 2. Win Rate & Best/Worst
    daily_returns = [d['daily_return'] for d in data if d['date'] != data[0]['date']] # Exclude first day
    if daily_returns:
        wins = [r for r in daily_returns if r > 0]
        win_rate = len(wins) / len(daily_returns) * 100
        best_day = max(daily_returns) * 100
        worst_day = min(daily_returns) * 100
        
        # Volatility (Std Dev of daily returns) * sqrt(252) for annualized? 
        # Let's just do daily volatility for now or simple std dev
        mean_ret = sum(daily_returns) / len(daily_returns)
        variance = sum((x - mean_ret) ** 2 for x in daily_returns) / len(daily_returns)
        volatility = (variance ** 0.5) * 100
    else:
        win_rate = 0
        best_day = 0
        worst_day = 0
        volatility = 0

    # 3. Monthly Returns (Based on Compounded Daily Returns)
    monthly_returns_map = {}
    
    for day in data:
        month_key = day['date'][:7] # YYYY-MM
        if month_key not in monthly_returns_map:
            monthly_returns_map[month_key] = 1.0
        
        # Compound the daily return
        r = day.get('daily_return', 0.0)
        monthly_returns_map[month_key] *= (1 + r)

    # Format for response
    sorted_months = sorted(monthly_returns_map.keys())
    monthly_returns = []
    
    for m in sorted_months:
        # Convert cumulative factor to percentage return
        m_return = (monthly_returns_map[m] - 1.0) * 100
        monthly_returns.append({'month': m, 'return': m_return})

    return jsonify({
        'mdd': mdd * 100,
        'win_rate': win_rate,
        'best_day': best_day,
        'worst_day': worst_day,
        'volatility': volatility,
        'mdd_history': drawdowns,
        'monthly_returns': monthly_returns,
        'daily_returns': [{'date': d['date'], 'daily_return': d.get('daily_return', 0.0) * 100, 'daily_profit': d.get('daily_profit', 0.0)} for d in data]
    })

@app.route('/api/recommendations/dates')
def get_recommendation_dates():
    selected_account_id = _get_selected_account_id()
    recommendation_account_id = PRIMARY_SELECTION_ACCOUNT.account_id if selected_account_id == 'all' else selected_account_id
    strategy_id = ACCOUNT_MAP[recommendation_account_id].strategy_id
    strategy_def = get_strategy_definition(strategy_id)
    if not strategy_def.requires_selection:
        return jsonify([])

    db_path = get_stock_selection_db_path(strategy_id)
    if not db_path.exists():
        return jsonify([])
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name DESC")
        tables = [row[0] for row in cursor.fetchall()]
        
    # Filter for YYYYMMDD format
    dates = [t for t in tables if t.isdigit() and len(t) == 8]
    return jsonify(dates)

@app.route('/api/recommendations/<date>')
def get_recommendations(date):
    try:
        selected_account_id = _get_selected_account_id()
        recommendation_account_id = PRIMARY_SELECTION_ACCOUNT.account_id if selected_account_id == 'all' else selected_account_id
        strategy_id = ACCOUNT_MAP[recommendation_account_id].strategy_id
        strategy_def = get_strategy_definition(strategy_id)
        if not strategy_def.requires_selection:
            return jsonify([])

        df = load_stock_selection(
            table_date=date,
            kis=None,
            rerank=False,
            top_n=20,
            strategy_id=strategy_id,
        )
        if df.empty:
            return jsonify([])
        
        # Select relevant columns
        cols = ['rank_total', '단축코드', '한글명', 'rank_value', 'rank_momentum', 'rank_quality']
        # Add some factor columns if they exist
        factors = ['1/per', '1/pbr', 'gp/a']
        for f in factors:
            if f in df.columns:
                cols.append(f)
                
        # Handle NaN values for JSON serialization
        df = df.fillna(0) # Or None, but 0 is safer for numbers
        
        result = df[cols].to_dict(orient='records')
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=15000)
