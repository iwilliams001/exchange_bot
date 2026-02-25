import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ConversationHandler,
    CallbackQueryHandler,
    ContextTypes
)
import sqlite3
from datetime import datetime

from database import init_db, DB_NAME
from utils import owner_rate, intermediary_rate, deduct_from_inventory
import os

TOKEN = os.getenv("8529710957:AAEKVks31ctycVxJZAUPE40sPCOo-YSr7z0")
OWNER_ID = int(os.getenv("347546519", "0"))
INTERMEDIARY_ID = int(os.getenv("347546519", "0"))

if not TOKEN or OWNER_ID == 0:
    raise ValueError("Missing required environment variables!")

# ------------------ CONFIGURATION ------------------
TOKEN = "8529710957:AAEKVks31ctycVxJZAUPE40sPCOo-YSr7z0"      # Replace with your bot token
OWNER_ID = 347546519                # Replace with your Telegram user ID
INTERMEDIARY_ID = 347546519          # Replace with partner's ID (or same for testing)
# ----------------------------------------------------

# Enable logging (fixed the typo)
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Conversation states
(USD_AMOUNT, CONFIRM_SUGGESTION, ACTUAL_AMOUNT) = range(3)
SET_MARKET_RATE = 3   # for set market conversation

# ------------------ Helper: Main Menu Keyboard ------------------
def get_main_menu_keyboard(user_id):
    buttons = []
    if user_id == OWNER_ID:
        buttons = [
            [InlineKeyboardButton("💰 Set Market Rate", callback_data="menu_setmarket")],
            [InlineKeyboardButton("📦 Bulk Transfer", callback_data="menu_bulktransfer")],
            [InlineKeyboardButton("📊 Inventory", callback_data="menu_inventory")],
            [InlineKeyboardButton("📈 Profit", callback_data="menu_profit")],
            [InlineKeyboardButton("📉 Current Rates", callback_data="menu_currentrates")],
            [InlineKeyboardButton("💸 Pay Customer", callback_data="menu_paycustomer")],
        ]
    elif user_id == INTERMEDIARY_ID:
        buttons = [
            [InlineKeyboardButton("💸 Pay Customer", callback_data="menu_paycustomer")],
            [InlineKeyboardButton("📊 Inventory", callback_data="menu_inventory")],
            [InlineKeyboardButton("📉 Current Rates", callback_data="menu_currentrates")],
        ]
    else:
        # Unauthorized user – no buttons
        return None
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="menu_cancel")])
    return InlineKeyboardMarkup(buttons)

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, text="Main Menu:"):
    """Sends the main menu as a new message (or edits if called from callback)."""
    user_id = update.effective_user.id
    keyboard = get_main_menu_keyboard(user_id)
    if keyboard is None:
        await (update.callback_query.edit_message_text("Unauthorized.") if update.callback_query else update.message.reply_text("Unauthorized."))
        return
    if update.callback_query:
        # Edit the current message (which might be a result) to show the menu
        await update.callback_query.edit_message_text(text, reply_markup=keyboard)
    else:
        await update.message.reply_text(text, reply_markup=keyboard)

# ------------------ Start Command ------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in (OWNER_ID, INTERMEDIARY_ID):
        await update.message.reply_text("Unauthorized.")
        return
    await show_main_menu(update, context, "Welcome! Choose an action:")

# ------------------ Set Market Rate Conversation ------------------
async def setmarket_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if isinstance(update, Update) else update.callback_query.from_user.id
    if user_id != OWNER_ID:
        if isinstance(update, Update) and update.callback_query:
            await update.callback_query.edit_message_text("Only owner can set market rate.")
        else:
            await update.message.reply_text("Only owner can set market rate.")
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
        # Return to main menu
        await show_main_menu(update, context, "Main Menu:")
    except ValueError:
        await update.message.reply_text("Invalid number. Please enter a valid rate.")
        return SET_MARKET_RATE
    return ConversationHandler.END

# ------------------ Bulk Transfer Conversation ------------------
async def bulk_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(update, Update) and update.callback_query:
        user_id = update.callback_query.from_user.id
        if user_id != OWNER_ID:
            await update.callback_query.edit_message_text("Only owner can record bulk transfers.")
            return ConversationHandler.END
        await update.callback_query.edit_message_text("Enter USD amount sent:")
    else:
        user_id = update.effective_user.id
        if user_id != OWNER_ID:
            await update.message.reply_text("Only owner can record bulk transfers.")
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
    await show_main_menu(update, context, "Main Menu:")
    return ConversationHandler.END

# ------------------ Pay Customer Conversation ------------------
async def pay_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(update, Update) and update.callback_query:
        user_id = update.callback_query.from_user.id
        if user_id not in (OWNER_ID, INTERMEDIARY_ID):
            await update.callback_query.edit_message_text("Unauthorized.")
            return ConversationHandler.END
        await update.callback_query.edit_message_text("Enter USD amount received from customer:")
    else:
        user_id = update.effective_user.id
        if user_id not in (OWNER_ID, INTERMEDIARY_ID):
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
        suggested = usd * owner
        context.user_data['market'] = market
        context.user_data['owner_rate'] = owner
        context.user_data['suggested'] = suggested
        keyboard = [
            [InlineKeyboardButton("✅ Use suggested", callback_data='use_suggested'),
             InlineKeyboardButton("✏️ Enter different", callback_data='enter_different')],
            [InlineKeyboardButton("❌ Cancel", callback_data='cancel_transaction')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(f"Suggested GHS amount: {suggested:.2f}\nWhat do you want to do?",
                                        reply_markup=reply_markup)
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
        await show_main_menu(update, context, "Main Menu:")
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

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT SUM(remaining_ghs) FROM inventory_batches")
    total_ghs = c.fetchone()[0] or 0.0
    if total_ghs < actual:
        await message.reply_text(f"Insufficient GHS! Available: {total_ghs:.2f}. Transaction cancelled.")
        await show_main_menu(update_or_query, context, "Main Menu:")
        return ConversationHandler.END

    try:
        usage, total_cost_usd = deduct_from_inventory(actual)
    except Exception as e:
        await message.reply_text(str(e))
        await show_main_menu(update_or_query, context, "Main Menu:")
        return ConversationHandler.END

    owner_profit = usd - total_cost_usd
    if owner_profit < 0:
        await message.reply_text(f"This transaction would result in negative owner profit (${owner_profit:.2f}). Not allowed. Transaction cancelled.")
        # Note: inventory deduction already happened – in production you'd roll back.
        await show_main_menu(update_or_query, context, "Main Menu:")
        return ConversationHandler.END

    c = conn.cursor()
    c.execute('''INSERT INTO customer_transactions
                 (usd_received, suggested_ghs, actual_ghs_paid, market_rate_at_time,
                  owner_rate_at_time, intermediary_rate_at_time, date, recorded_by)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
              (usd, suggested, actual, market, owner, intermediary_rate(market),
               datetime.now().isoformat(), user_id))
    tx_id = c.lastrowid

    for batch_id, ghs_used, cost in usage:
        c.execute("INSERT INTO tx_batch_usage (tx_id, batch_id, ghs_used) VALUES (?, ?, ?)",
                  (tx_id, batch_id, ghs_used))

    conn.commit()
    conn.close()

    remaining = total_ghs - actual
    await message.reply_text(
        f"✅ Transaction recorded!\n"
        f"USD: {usd}\n"
        f"GHS paid: {actual:.2f}\n"
        f"Suggested: {suggested:.2f}\n"
        f"Owner profit: ${owner_profit:.2f}\n"
        f"Remaining GHS: {remaining:.2f}"
    )

    # Notify owner if the transaction was performed by someone else (intermediary)
    if user_id != OWNER_ID:
        try:
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=(
                    f"🔔 Intermediary recorded a transaction:\n"
                    f"USD: {usd}\n"
                    f"GHS paid: {actual:.2f}\n"
                    f"Suggested: {suggested:.2f}\n"
                    f"Owner profit: ${owner_profit:.2f}\n"
                    f"Remaining GHS: {remaining:.2f}"
                )
            )
        except Exception as e:
            logger.error(f"Failed to notify owner: {e}")

    await show_main_menu(update_or_query, context, "Main Menu:")
    return ConversationHandler.END

# ------------------ Inventory Check ------------------
async def inventory(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if isinstance(update, Update) else update.callback_query.from_user.id
    if user_id not in (OWNER_ID, INTERMEDIARY_ID):
        if isinstance(update, Update) and update.callback_query:
            await update.callback_query.edit_message_text("Unauthorized.")
        else:
            await update.message.reply_text("Unauthorized.")
        return

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

    if isinstance(update, Update) and update.callback_query:
        await update.callback_query.edit_message_text(text)
        # Send a new message with main menu
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Main Menu:",
            reply_markup=get_main_menu_keyboard(user_id)
        )
    else:
        await update.message.reply_text(text)
        await show_main_menu(update, context, "Main Menu:")

# ------------------ Profit Summary ------------------
async def profit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if isinstance(update, Update) else update.callback_query.from_user.id
    if user_id != OWNER_ID:
        if isinstance(update, Update) and update.callback_query:
            await update.callback_query.edit_message_text("Only owner can view profit.")
        else:
            await update.message.reply_text("Only owner can view profit.")
        return

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        SELECT SUM(t.usd_received - ub.total_cost)
        FROM customer_transactions t
        JOIN (
            SELECT tx_id, SUM(ghs_used * b.usd_cost_per_ghs) as total_cost
            FROM tx_batch_usage u
            JOIN inventory_batches b ON u.batch_id = b.id
            GROUP BY tx_id
        ) ub ON t.id = ub.tx_id
    ''')
    total_profit = c.fetchone()[0] or 0.0
    conn.close()

    text = f"💰 Total owner profit (USD): ${total_profit:.2f}"

    if isinstance(update, Update) and update.callback_query:
        await update.callback_query.edit_message_text(text)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Main Menu:",
            reply_markup=get_main_menu_keyboard(user_id)
        )
    else:
        await update.message.reply_text(text)
        await show_main_menu(update, context, "Main Menu:")

# ------------------ Current Rates ------------------
async def current_rates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if isinstance(update, Update) else update.callback_query.from_user.id
    if user_id not in (OWNER_ID, INTERMEDIARY_ID):
        if isinstance(update, Update) and update.callback_query:
            await update.callback_query.edit_message_text("Unauthorized.")
        else:
            await update.message.reply_text("Unauthorized.")
        return

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
                f"Market: {market:.4f} GHS/USD (as of {ts[:10]})\n"
                f"Owner rate: {owner:.4f} GHS/USD\n"
                f"Intermediary rate: {inter:.4f} GHS/USD")

    if isinstance(update, Update) and update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='Markdown')
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Main Menu:",
            reply_markup=get_main_menu_keyboard(user_id)
        )
    else:
        await update.message.reply_text(text, parse_mode='Markdown')
        await show_main_menu(update, context, "Main Menu:")

# ------------------ General Menu Callback Handler ------------------
async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id

    if user_id not in (OWNER_ID, INTERMEDIARY_ID):
        await query.edit_message_text("Unauthorized.")
        return

    # This handler only catches buttons that are NOT handled by conversation entry points
    if data == "menu_cancel":
        await query.edit_message_text("Cancelled. Use /start to see menu again.")
    else:
        # Should not happen – but just in case
        await query.edit_message_text("This feature is under construction.")

# ------------------ Main ------------------
def main():
    init_db()
    application = Application.builder().token(TOKEN).build()

    # Basic commands
    application.add_handler(CommandHandler("start", start))

    # --- Conversation handlers (must come before general callback handler) ---
    # Set market conversation
    setmarket_conv = ConversationHandler(
        entry_points=[
            CommandHandler("setmarket", setmarket_start),
            CallbackQueryHandler(setmarket_start, pattern="^menu_setmarket$")
        ],
        states={
            SET_MARKET_RATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, setmarket_rate)]
        },
        fallbacks=[CallbackQueryHandler(menu_callback, pattern="^menu_cancel$")]
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
        fallbacks=[CallbackQueryHandler(menu_callback, pattern="^menu_cancel$")]
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
        fallbacks=[CallbackQueryHandler(menu_callback, pattern="^menu_cancel$")]
    )
    application.add_handler(pay_conv)

    # --- Handlers for non-conversation menu actions ---
    application.add_handler(CallbackQueryHandler(inventory, pattern="^menu_inventory$"))
    application.add_handler(CallbackQueryHandler(profit, pattern="^menu_profit$"))
    application.add_handler(CallbackQueryHandler(current_rates, pattern="^menu_currentrates$"))
    application.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_cancel$"))

    # Also allow direct commands (fallback)
    application.add_handler(CommandHandler("inventory", inventory))
    application.add_handler(CommandHandler("profit", profit))
    # /setmarket is already handled by conversation entry

    application.run_polling()

if __name__ == '__main__':
    main()