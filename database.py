import sqlite3

import os

# Determine the database path - use Railway volume if available, otherwise local
if os.path.exists('/data') or os.getenv('RAILWAY_VOLUME_MOUNT_PATH'):
    # We're on Railway with a volume mounted
    DB_PATH = '/data/exchange_bot.db'
else:
    # Local development
    DB_PATH = 'exchange_bot.db'

DB_NAME = DB_PATH


def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (telegram_id INTEGER PRIMARY KEY, role TEXT NOT NULL)''')

    c.execute('''CREATE TABLE IF NOT EXISTS market_rates
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  rate REAL NOT NULL,
                  timestamp TEXT NOT NULL,
                  entered_by INTEGER NOT NULL)''')

    c.execute('''CREATE TABLE IF NOT EXISTS bulk_transfers
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  usd_amount REAL NOT NULL,
                  market_rate REAL NOT NULL,
                  ghs_received REAL NOT NULL,
                  date TEXT NOT NULL,
                  notes TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS inventory_batches
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  bulk_id INTEGER NOT NULL,
                  remaining_ghs REAL NOT NULL,
                  usd_cost_per_ghs REAL NOT NULL,
                  FOREIGN KEY(bulk_id) REFERENCES bulk_transfers(id))''')

    c.execute('''CREATE TABLE IF NOT EXISTS customer_transactions
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  usd_received REAL NOT NULL,
                  suggested_ghs REAL NOT NULL,
                  actual_ghs_paid REAL NOT NULL,
                  market_rate_at_time REAL NOT NULL,
                  owner_rate_at_time REAL NOT NULL,
                  intermediary_rate_at_time REAL NOT NULL,
                  date TEXT NOT NULL,
                  recorded_by INTEGER NOT NULL,
                  notes TEXT,
                  status TEXT DEFAULT 'completed')''')

    # Users table (approved users)
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (telegram_id INTEGER PRIMARY KEY,
                  role TEXT NOT NULL CHECK(role IN ('owner', 'intermediary')))''')

    # Pending users table (requests awaiting approval)
    c.execute('''CREATE TABLE IF NOT EXISTS pending_users
                 (telegram_id INTEGER PRIMARY KEY,
                  username TEXT,
                  first_name TEXT,
                  last_name TEXT,
                  requested_at TEXT NOT NULL)''')

    c.execute('''CREATE TABLE IF NOT EXISTS tx_batch_usage
                 (tx_id INTEGER NOT NULL,
                  batch_id INTEGER NOT NULL,
                  ghs_used REAL NOT NULL,
                  FOREIGN KEY(tx_id) REFERENCES customer_transactions(id),
                  FOREIGN KEY(batch_id) REFERENCES inventory_batches(id))''')

    conn.commit()
    conn.close()
    print("Database initialized.")