import logging
import sqlite3
import csv
from io import StringIO
from datetime import datetime
import os
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ConversationHandler,
    CallbackQueryHandler,
    ContextTypes
)

from database import init_db, DB_NAME
from utils import owner_rate, intermediary_rate, deduct_from_inventory

# ------------------ CONFIGURATION ------------------
TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))          # The initial owner (from env)

if not TOKEN or OWNER_ID == 0:
    raise ValueError("Missing required environment variables!")

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Conversation states
(USD_AMOUNT, CONFIRM_SUGGESTION, ACTUAL_AMOUNT) = range(3)
SET_MARKET_RATE = 3

# Scheduler for auto‑fetching market rate
scheduler = AsyncIOScheduler()

# ------------------ Database Helpers for User Management ------------------
async def get_user_role(user_id: int) -> str | None:
    """Return role of user if approved, else None."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT role FROM users WHERE telegram_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

async def add_user(user_id: int, role: str):
    """Add or update a user in the users table."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO users (telegram_id, role) VALUES (?, ?)", (user_id, role))
    conn.commit()
    conn.close()

async def remove_pending(user_id: int):
    """Remove a user from pending requests after processing."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM pending_users WHERE telegram_id = ?", (user_id,))
    conn.commit()
    conn.close()

async def add_pending(user_id: int, username: str = "", first_name: str = "", last_name: str = ""):
    """Add a user to pending requests."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''INSERT OR IGNORE INTO pending_users
                 (telegram_id, username, first_name, last_name, requested_at)
                 VALUES (?, ?, ?, ?, ?)''',
              (user_id, username, first_name, last_name, datetime.now().isoformat()))
    conn.commit()
    conn.close()

async def is_pending(user_id: int) -> bool:
    """Check if user is already pending."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT 1 FROM pending_users WHERE telegram_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row is not None

# ------------------ Helper: Main Menu Keyboard ------------------
def get_main_menu_keyboard(role: str):
    """Return inline keyboard with actions based on user role."""
    # Define all possible buttons (label, callback_data)
    all_buttons = [
        ("💰 Set Market Rate", "menu_setmarket"),
        ("📦 Bulk Transfer", "menu_bulktransfer"),
        ("📊 Inventory", "menu_inventory"),
        ("📈 Profit", "menu_profit"),
        ("📉 Current Rates", "menu_currentrates"),
        ("💸 Pay Customer", "menu_paycustomer"),
        ("📋 List Transactions", "menu_listtx"),
        ("📤 Export CSV", "menu_export"),
        ("🔍 Audit Log", "menu_audit"),
        ("🗑️ Delete Transaction", "menu_deletetx"),
        ("🔄 Reset Database", "menu_resetdb"),
        ("❌ Cancel", "menu_cancel"),
    ]

    # Determine allowed actions based on role
    if role == 'owner':
        selected = all_buttons  # owners see everything
    else:  # intermediary
        allowed = [
            "menu_setmarket", "menu_bulktransfer", "menu_inventory",
            "menu_profit", "menu_currentrates", "menu_paycustomer",
            "menu_listtx", "menu_cancel"
        ]
        selected = [btn for btn in all_buttons if btn[1] in allowed]

    # Arrange in rows of 2 (grid)
    keyboard = []
    for i in range(0, len(selected), 2):
        row = []
        row.append(InlineKeyboardButton(selected[i][0], callback_data=selected[i][1]))
        if i + 1 < len(selected):
            row.append(InlineKeyboardButton(selected[i+1][0], callback_data=selected[i+1][1]))
        keyboard.append(row)

    return InlineKeyboardMarkup(keyboard)

async def show_main_menu(update_or_obj, context: ContextTypes.DEFAULT_TYPE, user_id: int, text="Main Menu:"):
    """
    Send or edit a message with the main menu.
    update_or_obj can be an Update (from command) or a CallbackQuery (from button).
    """
    role = await get_user_role(user_id)
    if role is None:
        return
    keyboard = get_main_menu_keyboard(role)

    # Case 1: It's an Update with a callback_query (menu button click)
    if isinstance(update_or_obj, Update) and update_or_obj.callback_query:
        await update_or_obj.callback_query.edit_message_text(text, reply_markup=keyboard)
    # Case 2: It's a CallbackQuery directly (e.g., after finishing a conversation)
    elif isinstance(update_or_obj, CallbackQuery):
        await update_or_obj.edit_message_text(text, reply_markup=keyboard)
    # Case 3: It's an Update without callback (e.g., command)
    else:
        await update_or_obj.message.reply_text(text, reply_markup=keyboard)

# ------------------ Start Command & Approval Flow ------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id

    # Check if already approved
    role = await get_user_role(user_id)
    if role:
        await show_main_menu(update, context, user_id, f"Welcome back! Choose an action:")
        return

    # Check if already pending
    if await is_pending(user_id):
        await update.message.reply_text("Your request is already pending. Please wait for owner approval.")
        return

    # New user – add to pending and notify owner
    await add_pending(user_id, user.username or "", user.first_name or "", user.last_name or "")

    # Notify owner
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve as Owner", callback_data=f"approve_owner_{user_id}"),
            InlineKeyboardButton("✅ Approve as Intermediary", callback_data=f"approve_inter_{user_id}")
        ],
        [InlineKeyboardButton("❌ Reject", callback_data=f"reject_{user_id}")]
    ])
    name_parts = []
    if user.first_name:
        name_parts.append(user.first_name)
    if user.last_name:
        name_parts.append(user.last_name)
    full_name = " ".join(name_parts) or "Unknown"
    username_disp = f" (@{user.username})" if user.username else ""
    await context.bot.send_message(
        chat_id=OWNER_ID,
        text=(
            f"🆕 New user request:\n"
            f"ID: `{user_id}`\n"
            f"Name: {full_name}{username_disp}\n"
            f"Choose action:"
        ),
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

    await update.message.reply_text("Your request has been sent to the owner. You will be notified once approved.")

# ------------------ Approval Callback Handler ------------------
async def approval_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    owner_id = query.from_user.id

    if owner_id != OWNER_ID:
        await query.edit_message_text("Only the owner can approve users.")
        return

    # Parse callback data
    parts = data.split('_')
    action = parts[0]        # approve or reject
    role = parts[1] if action == 'approve' else None  # owner or inter
    user_id = int(parts[-1])

    if action == 'reject':
        await remove_pending(user_id)
        await query.edit_message_text(f"User {user_id} rejected.")
        # Optionally notify the rejected user
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text="Your request to use the bot was rejected by the owner."
            )
        except Exception as e:
            logger.error(f"Could not notify rejected user {user_id}: {e}")
        return

    # Approve
    full_role = 'owner' if role == 'owner' else 'intermediary'
    await add_user(user_id, full_role)
    await remove_pending(user_id)
    await query.edit_message_text(f"User {user_id} approved as {full_role}.")

    # Notify the user
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"✅ Your request has been approved! You now have {full_role} access. Send /start to begin."
        )
    except Exception as e:
        logger.error(f"Could not notify approved user {user_id}: {e}")

# ------------------ Automatic Market Rate Fetching ------------------
async def fetch_market_rate():
    """Fetch USD/GHS rate from a free API and store it."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get("https://api.exchangerate-api.com/v4/latest/USD")
            data = response.json()
            rate = data['rates']['GHS']
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT INTO market_rates (rate, timestamp, entered_by) VALUES (?, ?, ?)",
                      (rate, datetime.now().isoformat(), 0))  # 0 = system
            conn.commit()
            conn.close()
            logger.info(f"Auto-fetched market rate: {rate}")
    except Exception as e:
        logger.error(f"Failed to fetch market rate: {e}")

async def fetch_rate_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual command to trigger rate fetch."""
    user_id = update.effective_user.id
    role = await get_user_role(user_id)
    if role is None:
        await update.message.reply_text("Unauthorized.")
        return
    await fetch_market_rate()
    await update.message.reply_text("Market rate fetched and stored.")
    await show_main_menu(update, context, user_id, "Main Menu:")

# ------------------ Set Market Rate Conversation ------------------
async def setmarket_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if isinstance(update, Update) else update.callback_query.from_user.id
    role = await get_user_role(user_id)
    if role is None:
        if isinstance(update, Update) and update.callback_query:
            await update.callback_query.edit_message_text("Unauthorized.")
        else:
            await update.message.reply_text("Unauthorized.")
        return ConversationHandler.END

    if isinstance(update, Update) and update.callback_query:
        await update.callback_query.edit_message_text("Enter the new market rate (e.g., 15.5):")
    else:
        await update.message.reply_text("Enter the new market rate (e.g., 15.5):")
    return SET_MARKET_RATE

async def setmarket_rate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        rate = float(update.message.text)
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("INSERT INTO market_rates (rate, timestamp, entered_by) VALUES (?, ?, ?)",
                  (rate, datetime.now().isoformat(), user_id))
        conn.commit()
        conn.close()
        await update.message.reply_text(f"Market rate set to {rate}")
        await show_main_menu(update, context, user_id, "Main Menu:")
    except ValueError:
        await update.message.reply_text("Invalid number. Please enter a valid rate.")
        return SET_MARKET_RATE
    return ConversationHandler.END

# ------------------ Bulk Transfer Conversation ------------------
async def bulk_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(update, Update) and update.callback_query:
        user_id = update.callback_query.from_user.id
        role = await get_user_role(user_id)
        if role is None:
            await update.callback_query.edit_message_text("Unauthorized.")
            return ConversationHandler.END
        await update.callback_query.edit_message_text("Enter USD amount sent:")
    else:
        user_id = update.effective_user.id
        role = await get_user_role(user_id)
        if role is None:
            await update.message.reply_text("Unauthorized.")
            return ConversationHandler.END
        await update.message.reply_text("Enter USD amount sent:")
    return "BULK_USD"

async def bulk_usd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        usd = float(update.message.text)
        context.user_data['bulk_usd'] = usd
        await update.message.reply_text("Enter market rate at time of transfer (or type 'current' to use latest):")
        return "BULK_RATE"
    except ValueError:
        await update.message.reply_text("Please enter a valid number.")
        return "BULK_USD"

async def bulk_rate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text.lower() == 'current':
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT rate FROM market_rates ORDER BY timestamp DESC LIMIT 1")
        row = c.fetchone()
        conn.close()
        if row:
            rate = row[0]
        else:
            await update.message.reply_text("No market rate set. Please enter manually.")
            return "BULK_RATE"
    else:
        try:
            rate = float(text)
        except ValueError:
            await update.message.reply_text("Invalid number. Try again:")
            return "BULK_RATE"
    usd = context.user_data['bulk_usd']
    ghs = usd * rate
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT INTO bulk_transfers (usd_amount, market_rate, ghs_received, date) VALUES (?, ?, ?, ?)",
              (usd, rate, ghs, datetime.now().isoformat()))
    bulk_id = c.lastrowid
    c.execute("INSERT INTO inventory_batches (bulk_id, remaining_ghs, usd_cost_per_ghs) VALUES (?, ?, ?)",
              (bulk_id, ghs, 1/rate))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"Bulk transfer recorded: {usd} USD @ {rate} = {ghs:.2f} GHS")
    await show_main_menu(update, context, update.effective_user.id, "Main Menu:")
    return ConversationHandler.END

# ------------------ Pay Customer Conversation ------------------
async def pay_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(update, Update) and update.callback_query:
        user_id = update.callback_query.from_user.id
        role = await get_user_role(user_id)
        if role is None:
            await update.callback_query.edit_message_text("Unauthorized.")
            return ConversationHandler.END
        await update.callback_query.edit_message_text("Enter USD amount received from customer:")
    else:
        user_id = update.effective_user.id
        role = await get_user_role(user_id)
        if role is None:
            await update.message.reply_text("Unauthorized.")
            return ConversationHandler.END
        await update.message.reply_text("Enter USD amount received from customer:")
    return USD_AMOUNT

async def pay_usd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        usd = float(update.message.text)
        context.user_data['usd_received'] = usd
        # Get latest market rate
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT rate FROM market_rates ORDER BY timestamp DESC LIMIT 1")
        row = c.fetchone()
        conn.close()
        if not row:
            await update.message.reply_text("No market rate set. Owner must set rate first.")
            return ConversationHandler.END
        market = row[0]
        owner = owner_rate(market)
        inter = intermediary_rate(market)
        suggested = usd * inter  # Use intermediary's rate for suggestion
        context.user_data['market'] = market
        context.user_data['owner_rate'] = owner
        context.user_data['intermediary_rate'] = inter
        context.user_data['suggested'] = suggested
        keyboard = [
            [InlineKeyboardButton("✅ Use suggested", callback_data='use_suggested'),
             InlineKeyboardButton("✏️ Enter different", callback_data='enter_different')],
            [InlineKeyboardButton("❌ Cancel", callback_data='cancel_transaction')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            f"Suggested GHS amount (using intermediary's rate): {suggested:.2f}\n"
            f"Owner rate: {owner:.4f} GHS/USD | Intermediary rate: {inter:.4f} GHS/USD\n"
            "What do you want to do?",
            reply_markup=reply_markup
        )
        return CONFIRM_SUGGESTION
    except ValueError:
        await update.message.reply_text("Please enter a valid number.")
        return USD_AMOUNT

async def pay_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == 'use_suggested':
        context.user_data['actual_ghs'] = context.user_data['suggested']
        return await finalize_transaction(query, context)
    elif query.data == 'enter_different':
        await query.edit_message_text("Enter the actual GHS amount paid:")
        return ACTUAL_AMOUNT
    elif query.data == 'cancel_transaction':
        await query.edit_message_text("Transaction cancelled.")
        await show_main_menu(update, context, query.from_user.id, "Main Menu:")
        return ConversationHandler.END

async def pay_actual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        actual = float(update.message.text)
        context.user_data['actual_ghs'] = actual
        return await finalize_transaction(update, context)
    except ValueError:
        await update.message.reply_text("Invalid number. Try again:")
        return ACTUAL_AMOUNT

async def finalize_transaction(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(update_or_query, Update):
        user_id = update_or_query.effective_user.id
        message = update_or_query.message
        is_callback = False
    else:
        user_id = update_or_query.from_user.id
        message = update_or_query.message
        is_callback = True

    usd = context.user_data['usd_received']
    actual = context.user_data['actual_ghs']
    suggested = context.user_data['suggested']
    market = context.user_data['market']
    owner = context.user_data['owner_rate']
    inter = context.user_data['intermediary_rate']

    owner_share = usd * owner  # GHS to be deducted from inventory (owner's cost)

    # Prevent intermediary from overpaying beyond owner's share
    if actual > owner_share:
        await message.reply_text(
            f"❌ Amount exceeds owner's share ({owner_share:.2f} GHS).\n"
            "To avoid a loss, you cannot pay more than the owner's share.\n"
            "Please enter a lower amount or negotiate with the customer."
        )
        await show_main_menu(update_or_query, context, user_id, "Main Menu:")
        return ConversationHandler.END

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT SUM(remaining_ghs) FROM inventory_batches")
    total_ghs = c.fetchone()[0] or 0.0

    # Check inventory against owner_share (not actual)
    if total_ghs < owner_share:
        await message.reply_text(
            f"Insufficient GHS to cover owner's share!\n"
            f"Available: {total_ghs:.2f} GHS, needed: {owner_share:.2f} GHS.\n"
            f"Please top up inventory and try again."
        )
        await show_main_menu(update_or_query, context, user_id, "Main Menu:")
        return ConversationHandler.END

    # Deduct owner_share from inventory (FIFO)
    try:
        usage, total_cost_usd = deduct_from_inventory(owner_share)  # now deducts owner_share
    except Exception as e:
        await message.reply_text(str(e))
        await show_main_menu(update_or_query, context, user_id, "Main Menu:")
        return ConversationHandler.END

    # Owner profit based on cost of owner_share
    owner_profit_usd = usd - total_cost_usd
    if owner_profit_usd < 0:
        await message.reply_text(
            f"Critical error: owner profit negative (${owner_profit_usd:.2f}). "
            "Transaction cancelled. Contact support."
        )
        # In production, you might want to rollback the inventory deduction here.
        await show_main_menu(update_or_query, context, user_id, "Main Menu:")
        return ConversationHandler.END

    # Record transaction (store owner_share as well? Not necessary, but we have owner_rate)
    c = conn.cursor()
    c.execute('''INSERT INTO customer_transactions
                 (usd_received, suggested_ghs, actual_ghs_paid, market_rate_at_time,
                  owner_rate_at_time, intermediary_rate_at_time, date, recorded_by,
                  owner_profit_usd)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (usd, suggested, actual, market, owner, inter,
               datetime.now().isoformat(), user_id, owner_profit_usd))
    tx_id = c.lastrowid

    # Record batch usage (for owner_share deduction)
    for batch_id, ghs_used, cost in usage:
        c.execute("INSERT INTO tx_batch_usage (tx_id, batch_id, ghs_used) VALUES (?, ?, ?)",
                  (tx_id, batch_id, ghs_used))

    conn.commit()
    conn.close()

    remaining = total_ghs - owner_share  # remaining after deducting owner's share

    # Message for the user who performed the transaction (intermediary or owner)
    # Do NOT include owner profit
    user_message = (
        f"✅ Transaction recorded!\n"
        f"USD: {usd}\n"
        f"GHS paid: {actual:.2f}\n"
        f"Suggested: {suggested:.2f}\n"
        f"Remaining GHS: {remaining:.2f}"
    )
    await message.reply_text(user_message)

    # Notify owner separately with profit details, unless the user is the owner
    if user_id != OWNER_ID:
        try:
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=(
                    f"🔔 Transaction by intermediary:\n"
                    f"USD: {usd}\n"
                    f"GHS paid: {actual:.2f}\n"
                    f"Owner's share deducted: {owner_share:.2f} GHS\n"
                    f"Owner profit: ${owner_profit_usd:.2f}\n"
                    f"Remaining GHS: {remaining:.2f}"
                )
            )
        except Exception as e:
            logger.error(f"Failed to notify owner: {e}")

    # Low inventory alert (using remaining after owner_share deduction)
    if remaining < 1000:  # threshold
        for uid in (OWNER_ID,):
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=f"⚠️ Low GHS balance: only {remaining:.2f} GHS left. Consider a top-up."
                )
            except Exception as e:
                logger.error(f"Failed to alert user {uid}: {e}")

    await show_main_menu(update_or_query, context, user_id, "Main Menu:")
    return ConversationHandler.END

# ------------------ Inventory Check ------------------
async def inventory(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        user_id = update.callback_query.from_user.id
        reply_func = update.callback_query.edit_message_text
        is_callback = True
    else:
        user_id = update.effective_user.id
        reply_func = update.message.reply_text
        is_callback = False

    role = await get_user_role(user_id)
    if role is None:
        await reply_func("Unauthorized.")
        return

    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT SUM(remaining_ghs) FROM inventory_batches")
        total_ghs = c.fetchone()[0] or 0.0
        c.execute("SELECT remaining_ghs, usd_cost_per_ghs FROM inventory_batches WHERE remaining_ghs > 0")
        rows = c.fetchall()
        total_value_usd = sum(r[0] * r[1] for r in rows)
        avg_ghs_per_usd = total_ghs / total_value_usd if total_value_usd > 0 else 0
        conn.close()
        text = (
            f"📦 GHS balance: {total_ghs:.2f}\n"
            f"Average rate: {avg_ghs_per_usd:.4f} GHS/USD\n"
            f"Total value: ${total_value_usd:.2f}"
        )
        await reply_func(text)
    except Exception as e:
        await reply_func(f"Error: {e}")
        logger.exception("Inventory failed")

    if is_callback:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Main Menu:",
            reply_markup=get_main_menu_keyboard(role)
        )
    else:
        await show_main_menu(update, context, user_id, "Main Menu:")

# ------------------ Profit with Date Range ------------------
async def profit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        user_id = update.callback_query.from_user.id
        reply_func = update.callback_query.edit_message_text
        is_callback = True
    else:
        user_id = update.effective_user.id
        reply_func = update.message.reply_text
        is_callback = False

    role = await get_user_role(user_id)
    if role is None:
        await reply_func("Unauthorized.")
        return

    # Parse optional date arguments (same as before)
    start_date = None
    end_date = None
    if context.args:
        try:
            if len(context.args) >= 2:
                start_date = context.args[0]
                end_date = context.args[1]
            elif len(context.args) == 1:
                start_date = context.args[0]
                end_date = start_date
        except:
            await reply_func("Usage: /profit [YYYY-MM-DD] [YYYY-MM-DD]")
            return

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # Owner profit: sum of stored owner_profit_usd
    owner_query = 'SELECT SUM(owner_profit_usd) FROM customer_transactions'
    # Intermediary profit: unchanged
    inter_query = 'SELECT SUM(usd_received * market_rate_at_time - actual_ghs_paid) FROM customer_transactions'

    params = []
    if start_date and end_date:
        where = " WHERE date BETWEEN ? AND ?"
        owner_query += where
        inter_query += where
        params = [f"{start_date} 00:00:00", f"{end_date} 23:59:59"]

    c.execute(owner_query, params)
    owner_profit_usd = c.fetchone()[0] or 0.0
    c.execute(inter_query, params)
    inter_profit_ghs = c.fetchone()[0] or 0.0
    conn.close()

    date_str = f" from {start_date} to {end_date}" if start_date else ""
    text = (
        f"💰 Owner profit{date_str}: ${owner_profit_usd:.2f}\n"
        f"💸 Intermediary profit{date_str}: {inter_profit_ghs:.2f} GHS"
    )

    if is_callback:
        await reply_func(text, parse_mode='Markdown')
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Main Menu:",
            reply_markup=get_main_menu_keyboard(role)
        )
    else:
        await update.message.reply_text(text, parse_mode='Markdown')
        await show_main_menu(update, context, user_id, "Main Menu:")

# ------------------ Current Rates ------------------
async def current_rates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        user_id = update.callback_query.from_user.id
        reply_func = update.callback_query.edit_message_text
        is_callback = True
    else:
        user_id = update.effective_user.id
        reply_func = update.message.reply_text
        is_callback = False

    role = await get_user_role(user_id)
    if role is None:
        await reply_func("Unauthorized.")
        return

    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("SELECT rate, timestamp FROM market_rates ORDER BY timestamp DESC LIMIT 1")
        row = c.fetchone()
        conn.close()

        if not row:
            text = "No market rate set yet."
        else:
            market, ts = row
            owner = owner_rate(market)
            inter = intermediary_rate(market)
            text = (f"📊 **Current Rates**\n"
                    f"Market: {market:.2f} GHS/USD (as of {ts[:10]})\n"
                    f"Owner rate: {owner:.1f} GHS/USD\n"
                    f"Intermediary rate: {inter:.1f} GHS/USD")
        await reply_func(text, parse_mode='Markdown')
    except Exception as e:
        await reply_func(f"Error: {e}")
        logger.exception("Current rates failed")

    if is_callback:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Main Menu:",
            reply_markup=get_main_menu_keyboard(role)
        )
    else:
        await show_main_menu(update, context, user_id, "Main Menu:")

# ------------------ Export to CSV ------------------
async def export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        user_id = update.callback_query.from_user.id
        is_callback = True
    else:
        user_id = update.effective_user.id
        is_callback = False

    role = await get_user_role(user_id)
    if role is None:
        if is_callback:
            await update.callback_query.edit_message_text("Unauthorized.")
        else:
            await update.message.reply_text("Unauthorized.")
        return

    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute('''SELECT id, usd_received, suggested_ghs, actual_ghs_paid,
                            market_rate_at_time, owner_rate_at_time, intermediary_rate_at_time,
                            date, recorded_by
                     FROM customer_transactions ORDER BY date''')
        rows = c.fetchall()
        conn.close()

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['ID', 'USD Received', 'Suggested GHS', 'Actual GHS Paid',
                         'Market Rate', 'Owner Rate', 'Intermediary Rate', 'Date', 'Recorded By'])
        writer.writerows(rows)
        output.seek(0)

        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=output.getvalue().encode('utf-8'),
            filename='transactions.csv'
        )
    except Exception as e:
        if is_callback:
            await update.callback_query.edit_message_text(f"Error: {e}")
        else:
            await update.message.reply_text(f"Error: {e}")
        logger.exception("Export CSV failed")

    if is_callback:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Main Menu:",
            reply_markup=get_main_menu_keyboard(role)
        )
    else:
        await show_main_menu(update, context, user_id, "Main Menu:")

# ------------------ List Transactions ------------------
async def list_transactions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        user_id = update.callback_query.from_user.id
        reply_func = update.callback_query.edit_message_text
        is_callback = True
    else:
        user_id = update.effective_user.id
        reply_func = update.message.reply_text
        is_callback = False

    role = await get_user_role(user_id)
    if role is None:
        await reply_func("Unauthorized.")
        return

    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute('''SELECT id, usd_received, actual_ghs_paid, date
                     FROM customer_transactions ORDER BY date DESC LIMIT 10''')
        rows = c.fetchall()
        conn.close()

        if not rows:
            text = "No transactions yet."
        else:
            text = "Recent transactions:\n"
            for r in rows:
                text += f"ID {r[0]}: {r[1]} USD → {r[2]} GHS on {r[3][:10]}\n"
        await reply_func(text)
    except Exception as e:
        await reply_func(f"Error: {e}")
        logger.exception("List transactions failed")

    if is_callback:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Main Menu:",
            reply_markup=get_main_menu_keyboard(role)
        )
    else:
        await show_main_menu(update, context, user_id, "Main Menu:")

# ------------------ Delete Transaction (Owner Only) ------------------
async def delete_transaction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    role = await get_user_role(user_id)
    if role != 'owner':
        await update.message.reply_text("Only owner can delete transactions.")
        return

    try:
        tx_id = int(context.args[0])
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        # Note: This does not adjust inventory – for a production system you'd need to restore batches.
        # For simplicity, we just delete the transaction record.
        c.execute("DELETE FROM customer_transactions WHERE id = ?", (tx_id,))
        conn.commit()
        conn.close()
        await update.message.reply_text(f"Transaction {tx_id} deleted. (Inventory not restored.)")
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /deletetx <transaction_id>")
    await show_main_menu(update, context, user_id, "Main Menu:")

# ------------------ Audit Log (Owner Only) ------------------
async def audit_log(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        user_id = update.callback_query.from_user.id
        reply_func = update.callback_query.edit_message_text
        is_callback = True
    else:
        user_id = update.effective_user.id
        reply_func = update.message.reply_text
        is_callback = False

    role = await get_user_role(user_id)
    if role != 'owner':
        await reply_func("Only owner can view audit log.")
        return

    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute('''SELECT id, usd_received, actual_ghs_paid, recorded_by, date
                     FROM customer_transactions ORDER BY date DESC LIMIT 20''')
        rows = c.fetchall()
        conn.close()

        if not rows:
            text = "No transactions yet."
        else:
            text = "Audit log (last 20 transactions):\n"
            for r in rows:
                user_role = await get_user_role(r[3])
                user_type = user_role if user_role else "Unknown"
                text += f"ID {r[0]}: {r[1]} USD → {r[2]} GHS by {user_type} on {r[4][:19]}\n"
        await reply_func(text)
    except Exception as e:
        await reply_func(f"Error: {e}")
        logger.exception("Audit log failed")

    if is_callback:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Main Menu:",
            reply_markup=get_main_menu_keyboard(role)
        )
    else:
        await show_main_menu(update, context, user_id, "Main Menu:")

# ------------------ Reset Database (Owner Only) ------------------
async def reset_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        user_id = update.callback_query.from_user.id
        reply_func = update.callback_query.edit_message_text
        is_callback = True
    else:
        user_id = update.effective_user.id
        reply_func = update.message.reply_text
        is_callback = False

    role = await get_user_role(user_id)
    if role != 'owner':
        await reply_func("Only owner can reset the database.")
        return

    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        tables = ["customer_transactions", "tx_batch_usage", "inventory_batches", "bulk_transfers", "market_rates"]
        for table in tables:
            c.execute(f"DELETE FROM {table}")
        conn.commit()
        conn.close()
        await reply_func("✅ All data cleared. Database is now empty.")
    except Exception as e:
        await reply_func(f"❌ Error: {e}")
        logger.exception("Reset database failed")

    if is_callback:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Main Menu:",
            reply_markup=get_main_menu_keyboard(role)
        )
    else:
        await show_main_menu(update, context, user_id, "Main Menu:")

# ------------------ General Menu Callback Handler (Fallback) ------------------
async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id

    role = await get_user_role(user_id)
    if role is None:
        await query.edit_message_text("Unauthorized.")
        return

    # For buttons that are not handled by specific handlers (like delete tx prompt)
    if data == "menu_deletetx":
        await query.edit_message_text("Use /deletetx <transaction_id> to delete.")
    elif data == "menu_cancel":
        await query.edit_message_text("Cancelled. Use /start to see menu again.")
    else:
        # Should not happen – but just in case
        await query.edit_message_text("This feature is under construction.")

# ------------------ Post Init & Main ------------------
async def post_init(application: Application) -> None:
    """Runs after the Application is initialized but before it starts polling."""
    # Ensure the hardcoded owner is in the users table
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (telegram_id, role) VALUES (?, 'owner')", (OWNER_ID,))
    conn.commit()
    conn.close()

    # Start the scheduler for auto‑fetching market rate
    scheduler.add_job(fetch_market_rate, IntervalTrigger(hours=24))
    scheduler.start()
    logger.info("Scheduler started for automatic market rate fetching.")

def main():
    init_db()  # Creates all tables (including users and pending_users)

    application = Application.builder().token(TOKEN).post_init(post_init).build()

    # --- Error handler ---
    async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.error("Exception while handling an update:", exc_info=context.error)
    application.add_error_handler(error_handler)

    # --- Basic commands ---
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("fetchrate", fetch_rate_now))

    # --- Approval callback handler ---
    application.add_handler(CallbackQueryHandler(approval_callback, pattern="^(approve_|reject_)"))

    # --- Conversation handlers (must come before general callback) ---
    # Set market conversation
    setmarket_conv = ConversationHandler(
        entry_points=[
            CommandHandler("setmarket", setmarket_start),
            CallbackQueryHandler(setmarket_start, pattern="^menu_setmarket$")
        ],
        states={
            SET_MARKET_RATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, setmarket_rate)]
        },
        fallbacks=[CallbackQueryHandler(menu_callback, pattern="^menu_cancel$")],
        per_message=False
    )
    application.add_handler(setmarket_conv)

    # Bulk transfer conversation
    bulk_conv = ConversationHandler(
        entry_points=[
            CommandHandler("bulktransfer", bulk_start),
            CallbackQueryHandler(bulk_start, pattern="^menu_bulktransfer$")
        ],
        states={
            "BULK_USD": [MessageHandler(filters.TEXT & ~filters.COMMAND, bulk_usd)],
            "BULK_RATE": [MessageHandler(filters.TEXT & ~filters.COMMAND, bulk_rate)],
        },
        fallbacks=[CallbackQueryHandler(menu_callback, pattern="^menu_cancel$")],
        per_message=False
    )
    application.add_handler(bulk_conv)

    # Pay customer conversation
    pay_conv = ConversationHandler(
        entry_points=[
            CommandHandler("paycustomer", pay_start),
            CallbackQueryHandler(pay_start, pattern="^menu_paycustomer$")
        ],
        states={
            USD_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, pay_usd)],
            CONFIRM_SUGGESTION: [CallbackQueryHandler(pay_confirm)],
            ACTUAL_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, pay_actual)],
        },
        fallbacks=[CallbackQueryHandler(menu_callback, pattern="^menu_cancel$")],
        per_message=False
    )
    application.add_handler(pay_conv)

    # --- Handlers for non-conversation menu actions ---
    application.add_handler(CallbackQueryHandler(inventory, pattern="^menu_inventory$"))
    application.add_handler(CallbackQueryHandler(profit, pattern="^menu_profit$"))
    application.add_handler(CallbackQueryHandler(current_rates, pattern="^menu_currentrates$"))
    application.add_handler(CallbackQueryHandler(export_csv, pattern="^menu_export$"))
    application.add_handler(CallbackQueryHandler(list_transactions, pattern="^menu_listtx$"))
    application.add_handler(CallbackQueryHandler(audit_log, pattern="^menu_audit$"))
    application.add_handler(CallbackQueryHandler(reset_database, pattern="^menu_resetdb$"))
    # Delete transaction and cancel use a generic handler
    application.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_deletetx$"))
    application.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_cancel$"))

    # --- Additional command handlers ---
    application.add_handler(CommandHandler("profit", profit))
    application.add_handler(CommandHandler("inventory", inventory))
    application.add_handler(CommandHandler("export", export_csv))
    application.add_handler(CommandHandler("listtx", list_transactions))
    application.add_handler(CommandHandler("deletetx", delete_transaction))
    application.add_handler(CommandHandler("audit", audit_log))
    application.add_handler(CommandHandler("resetdb", reset_database))

    application.run_polling()

if __name__ == '__main__':
    main()