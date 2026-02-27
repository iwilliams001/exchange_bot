import math
import sqlite3

DB_NAME = "exchange_bot.db"

def floor_step(value, step=0.5):
    """Floor to nearest multiple of step"""
    return math.floor(value / step) * step

def owner_rate(market):
    base = market * 0.8
    floored = floor_step(base)
    if (base - floored) > 0.5:
        return floored - 0.5
    else:
        return floored

def intermediary_rate(market):
    base = market * 0.75
    floored = floor_step(base)
    if (base - floored) > 0.5:
        return floored - 0.5
    else:
        return floored

def deduct_from_inventory(ghs_needed):
    """
    Deduct GHS from oldest batches (FIFO) and return usage list and total USD cost.
    """
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT id, remaining_ghs, usd_cost_per_ghs FROM inventory_batches WHERE remaining_ghs > 0 ORDER BY id")
    batches = c.fetchall()
    usage = []
    remaining = ghs_needed
    total_cost_usd = 0.0
    for batch_id, avail, cost in batches:
        if remaining <= 0:
            break
        take = min(avail, remaining)
        usage.append((batch_id, take, cost))
        total_cost_usd += take * cost
        remaining -= take
        c.execute("UPDATE inventory_batches SET remaining_ghs = remaining_ghs - ? WHERE id = ?", (take, batch_id))
    conn.commit()
    conn.close()
    if remaining > 0:
        raise Exception("Insufficient GHS in inventory")
    return usage, total_cost_usd