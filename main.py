"""
SCALPING SİQNAL BOTU (OKX + MULTI-TIMEFRAME, OPTİMALLAŞDIRILMIŞ KORREKTƏ OLUNMUŞ VERSİYA)
========================================================================================
DÜZƏLİŞLƏR:
1. SQLite Persistence: Aktiv tradelər bazada 'ACTIVE' kimi saxlanılır və restart-da bərpa olunur.
2. Partial TP Breakeven: Partial TP vurulduqda Trailing Stop dərhal Entry qiymətinə çəkilir.
3. Polling Sürətləndirildi: Price Check = 1s, Candle Check = 4s.
4. Resurs Optimallaşdırılması: Klines limit=80 edinildi (OKX API rate limit qorunması).
5. Toplu Ticker Sorğusu: Bütün qiymətlər tək API sorğusu ilə çəkilir.
"""

import os
import time
import atexit
import sqlite3
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request


# ============================================================
# CONFIG
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID_HERE")

USE_POLLING = os.getenv("USE_POLLING", "True").lower() == "true"
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "True").lower() == "true"

OKX_BASE = "https://www.okx.com"
OKX_KLINE_URL = f"{OKX_BASE}/api/v5/market/candles"
OKX_TICKERS_URL = f"{OKX_BASE}/api/v5/market/tickers"

OKX_PROXY_URL = os.getenv("OKX_PROXY_URL", "").strip()
OKX_PROXIES = {"http": OKX_PROXY_URL, "https": OKX_PROXY_URL} if OKX_PROXY_URL else None

SYMBOLS = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"]
TIMEFRAMES = ["1m", "3m", "5m"]

# Scalping üçün sürətli poll vaxtları
CANDLE_POLL_SECONDS = 4
PRICE_POLL_SECONDS = 1
MAX_CANDLES = 80  # Indikatorlar üçün 80 şam tam kifayətdir

EMA_FAST_PERIOD = 9
EMA_SLOW_PERIOD = 21
RSI_PERIOD = 14
ATR_PERIOD = 14

RSI_LONG_MIN = 40
RSI_LONG_MAX = 75
RSI_SHORT_MIN = 25
RSI_SHORT_MAX = 60

CHANDELIER_ATR_MULT = 1.2

ACCOUNT_BALANCE_USDT = float(os.getenv("ACCOUNT_BALANCE_USDT", "1000"))
RISK_PER_TRADE_PCT = 0.005
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "99999"))

MAX_CONSECUTIVE_FETCH_FAILS = 5

ENABLE_VOLUME_FILTER = os.getenv("ENABLE_VOLUME_FILTER", "True").lower() == "true"
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.3"))

ENABLE_ATR_FILTER = os.getenv("ENABLE_ATR_FILTER", "True").lower() == "true"
MIN_ATR_PCT = float(os.getenv("MIN_ATR_PCT", "0.08"))

ENABLE_SPREAD_FILTER = os.getenv("ENABLE_SPREAD_FILTER", "True").lower() == "true"
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.12"))

MAX_SAME_DIRECTION_TRADES = int(os.getenv("MAX_SAME_DIRECTION_TRADES", "5"))

ENABLE_PARTIAL_TP = os.getenv("ENABLE_PARTIAL_TP", "True").lower() == "true"
PARTIAL_TP_R_MULTIPLE = float(os.getenv("PARTIAL_TP_R_MULTIPLE", "0.8"))
PARTIAL_TP_CLOSE_PCT = float(os.getenv("PARTIAL_TP_CLOSE_PCT", "0.6"))

DB_FILE = "scalping_bot.db"
PID_FILE = "scalping_bot.lock"


# ============================================================
# FLASK & STATE
# ============================================================

app = Flask(__name__)

lock = threading.Lock()
db_lock = threading.Lock()

candles = {s: {tf: [] for tf in TIMEFRAMES} for s in SYMBOLS}
active_trades = {}
last_signal_candle = {s: {tf: None for tf in TIMEFRAMES} for s in SYMBOLS}

daily_trade_count = 0
daily_count_date = None

fetch_fail_counts = {s: {tf: 0 for tf in TIMEFRAMES} for s in SYMBOLS}
fetch_fail_alerted = {s: {tf: False for tf in TIMEFRAMES} for s in SYMBOLS}

_startup_done = False
_startup_lock = threading.Lock()
_owns_pid_lock = False


def trade_key(symbol, tf):
    return f"{symbol}|{tf}"


def display_symbol(symbol):
    parts = symbol.split("-")
    return f"{parts[0]}/{parts[1]}" if len(parts) >= 2 else symbol


# ============================================================
# SINGLE INSTANCE GUARD
# ============================================================

def check_single_instance():
    global _owns_pid_lock
    pid = str(os.getpid())
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r") as f:
                old_pid = int(f.read().strip())
            if old_pid != os.getpid():
                try:
                    os.kill(old_pid, 0)
                    print(f"⚠️ [PID Lock] Bot artıq başqa prosesdə işləyir (PID: {old_pid}).")
                    return False
                except (OSError, ProcessLookupError):
                    pass
        except (OSError, ValueError):
            pass
    try:
        with open(PID_FILE, "w") as f:
            f.write(pid)
        _owns_pid_lock = True
        return True
    except Exception as e:
        print(f"❌ PID lock faylı xətası: {e}")
        return True


def release_pid_lock():
    if not _owns_pid_lock:
        return
    try:
        if os.path.exists(PID_FILE):
            with open(PID_FILE, "r") as f:
                if f.read().strip() == str(os.getpid()):
                    os.remove(PID_FILE)
    except Exception as e:
        print("⚠️ PID lock təmizləmə xətası:", e)


atexit.register(release_pid_lock)


# ============================================================
# DATABASE persistence (AKTİV TRADELƏRİN Saxlanması)
# ============================================================

def init_db():
    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=15)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                side TEXT NOT NULL,
                entry REAL NOT NULL,
                initial_stop REAL NOT NULL,
                trailing_stop REAL NOT NULL,
                extreme_price REAL NOT NULL,
                atr REAL NOT NULL,
                r_value REAL NOT NULL,
                position_size_usdt REAL NOT NULL,
                partial_taken INTEGER DEFAULT 0,
                status TEXT NOT NULL,
                exit_price REAL,
                created_at REAL NOT NULL,
                closed_at REAL
            )
        """)
        conn.commit()
        conn.close()


def db_save_new_trade(trade):
    try:
        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=15)
            cur = conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO trades (
                    trade_key, symbol, timeframe, side, entry, initial_stop,
                    trailing_stop, extreme_price, atr, r_value, position_size_usdt,
                    partial_taken, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade_key(trade["symbol"], trade["tf"]), trade["symbol"], trade["tf"],
                trade["side"], trade["entry"], trade["initial_stop"], trade["trailing_stop"],
                trade["extreme_price"], trade["atr"], trade["r_value"],
                trade["position_size_usdt"], 1 if trade["partial_taken"] else 0,
                "ACTIVE", trade["created_at"]
            ))
            conn.commit()
            conn.close()
    except Exception as e:
        print("❌ db_save_new_trade xətası:", e)


def db_update_active_trade(trade):
    try:
        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=15)
            cur = conn.cursor()
            cur.execute("""
                UPDATE trades SET 
                    trailing_stop = ?, extreme_price = ?, partial_taken = ?
                WHERE trade_key = ? AND status = 'ACTIVE'
            """, (
                trade["trailing_stop"], trade["extreme_price"],
                1 if trade["partial_taken"] else 0,
                trade_key(trade["symbol"], trade["tf"])
            ))
            conn.commit()
            conn.close()
    except Exception as e:
        print("❌ db_update_active_trade xətası:", e)


def db_close_trade(trade):
    try:
        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=15)
            cur = conn.cursor()
            cur.execute("""
                UPDATE trades SET 
                    status = ?, exit_price = ?, closed_at = ?, trailing_stop = ?
                WHERE trade_key = ?
            """, (
                trade["status"], trade["exit_price"], trade["closed_at"],
                trade["trailing_stop"], trade_key(trade["symbol"], trade["tf"])
            ))
            conn.commit()
            conn.close()
    except Exception as e:
        print("❌ db_close_trade xətası:", e)


def db_load_active_trades():
    loaded = {}
    try:
        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=15)
            cur = conn.cursor()
            cur.execute("""
                SELECT symbol, timeframe, side, entry, initial_stop, trailing_stop,
                       extreme_price, atr, r_value, position_size_usdt, partial_taken, created_at
                FROM trades WHERE status = 'ACTIVE'
            """)
            rows = cur.fetchall()
            conn.close()

        for r in rows:
            symbol, tf, side, entry, init_stop, trail_stop, ext_price, atr_val, r_val, pos_size, partial, created = r
            key = trade_key(symbol, tf)
            loaded[key] = {
                "symbol": symbol, "tf": tf, "side": side, "entry": entry,
                "initial_stop": init_stop, "trailing_stop": trail_stop,
                "extreme_price": ext_price, "atr": atr_val, "r_value": r_val,
                "position_size_usdt": pos_size, "partial_taken": bool(partial),
                "status": "ACTIVE", "created_at": created
            }
    except Exception as e:
        print("❌ db_load_active_trades xətası:", e)
    return loaded


def get_statistics():
    try:
        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=15)
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*),
                       COALESCE(SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END), 0)
                FROM trades WHERE status IN ('WIN', 'LOSS')
            """)
            total, wins, losses = cur.fetchone()
            conn.close()
        win_rate = round((wins / total) * 100, 2) if total else 0
        return {"total": total, "wins": wins, "losses": losses, "win_rate": win_rate}
    except Exception as e:
        print("❌ get_statistics xətası:", e)
        return {"total": 0, "wins": 0, "losses": 0, "win_rate": 0}


init_db()


# ============================================================
# TELEGRAM ENGINE
# ============================================================

def send_telegram(message, chat_id=None, parse_mode=None):
    token = TELEGRAM_BOT_TOKEN
    target_chat_id = chat_id or TELEGRAM_CHAT_ID
    if not token or token == "YOUR_BOT_TOKEN_HERE" or not target_chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": target_chat_id, "text": message}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        r = requests.post(url, json=payload, timeout=10)
        return r.json().get("ok", False)
    except Exception as e:
        print("❌ Telegram xətası:", e)
        return False


def process_telegram_update(update_data):
    if not update_data or "message" not in update_data:
        return
    msg = update_data["message"]
    chat_id = msg.get("chat", {}).get("id")
    raw_text = msg.get("text", "").strip()
    if not chat_id or not raw_text:
        return

    cmd = raw_text.split("@")[0].strip().lower()
    response_text = ""

    if cmd in ["/start", "/help"]:
        response_text = (
            "🤖 *SCALPING BOTU ƏMRLƏRİ*\n\n"
            "📊 /stats - WIN/LOSS göstəriciləri\n"
            "⚡ /active - Açıq olan pozisiyalar\n"
            "🟢 /status - Botun ümumi vəziyyəti"
        )
    elif cmd in ["/status", "status"]:
        with lock:
            active_count = len(active_trades)
            current_daily = daily_trade_count
        response_text = (
            "🤖 *BOT VƏZİYYƏTİ*\n\n"
            "🟢 Status: ONLINE (OKX)\n"
            f"📈 Açıq Trade: {active_count}\n"
            f"📅 Bugünkü Trade: {current_daily}/{MAX_TRADES_PER_DAY if MAX_TRADES_PER_DAY < 9999 else 'Limitsiz'}"
        )
    elif cmd in ["/stats", "stats"]:
        stats = get_statistics()
        response_text = (
            "📊 *STATİSTİKA*\n\n"
            f"Cəmi: {stats['total']} | ✅ WIN: {stats['wins']} | ❌ LOSS: {stats['losses']}\n"
            f"🎯 Win Rate: %{stats['win_rate']}"
        )
    elif cmd in ["/active", "active"]:
        with lock:
            trades_list = list(active_trades.values())
        if not trades_list:
            response_text = "ℹ️ Aktiv trade yoxdur."
        else:
            response_text = "⚡ *AÇIQ TRADELƏR*\n\n"
            for t in trades_list:
                emoji = "🟢" if t["side"] == "LONG" else "🔴"
                response_text += (
                    f"{emoji} *{display_symbol(t['symbol'])} {t['side']} [{t['tf']}]*\n"
                    f"Entry: `{t['entry']:.4f}` | Stop: `{t['trailing_stop']:.4f}`\n\n"
                )

    if response_text:
        send_telegram(response_text, chat_id=chat_id, parse_mode="Markdown")


def telegram_polling_worker():
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        return
    offset = 0
    while True:
        try:
            resp = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                                params={"offset": offset, "timeout": 20}, timeout=25)
            if resp.status_code == 200 and resp.json().get("ok"):
                for update in resp.json().get("result", []):
                    offset = update["update_id"] + 1
                    process_telegram_update(update)
        except Exception:
            time.sleep(3)
        time.sleep(1)


# ============================================================
# OKX OPTIMIZED REST API
# ============================================================

def fetch_klines(symbol, bar, limit=MAX_CANDLES):
    params = {"instId": symbol, "bar": bar, "limit": min(limit, 100)}
    try:
        r = requests.get(OKX_KLINE_URL, params=params, timeout=8, proxies=OKX_PROXIES)
        data = r.json()
        if data.get("code") != "0":
            return []
        rows = data.get("data", [])
        rows.reverse()
        return [{
            "time": int(row[0]), "open": float(row[1]), "high": float(row[2]),
            "low": float(row[3]), "close": float(row[4]), "volume": float(row[5])
        } for row in rows]
    except Exception:
        return []


def fetch_all_swap_tickers():
    """Bütün SWAP qiymətlərini TƏK API sorğusu ilə çəkir."""
    params = {"instType": "SWAP"}
    try:
        r = requests.get(OKX_TICKERS_URL, params=params, timeout=5, proxies=OKX_PROXIES)
        data = r.json()
        if data.get("code") == "0":
            res = {}
            for item in data.get("data", []):
                inst = item.get("instId")
                last = item.get("last")
                bid = item.get("bidPx")
                ask = item.get("askPx")
                if inst and last:
                    res[inst] = {
                        "last": float(last),
                        "bid": float(bid) if bid else float(last),
                        "ask": float(ask) if ask else float(last)
                    }
            return res
    except Exception as e:
        print("❌ Tickers fetch error:", e)
    return {}


# ============================================================
# İNDİKATORLAR
# ============================================================

def ema_series(values, period):
    if len(values) < period:
        return [None] * len(values)
    k = 2 / (period + 1)
    result = [None] * (period - 1)
    seed = sum(values[:period]) / period
    result.append(seed)
    prev = seed
    for v in values[period:]:
        prev = (v - prev) * k + prev
        result.append(prev)
    return result


def rsi(values, period=RSI_PERIOD):
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
    if len(gains) < period:
        return None
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + (avg_gain / avg_loss)))


def atr(candles_list, period=ATR_PERIOD):
    if len(candles_list) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles_list)):
        h, l = candles_list[i]["high"], candles_list[i]["low"]
        pc = candles_list[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / period if len(trs) >= period else None


# ============================================================
# SİQNAL & TRADE YARADILMASI
# ============================================================

def calc_position_size(entry, stop):
    risk_amount = ACCOUNT_BALANCE_USDT * RISK_PER_TRADE_PCT
    stop_distance = abs(entry - stop)
    if stop_distance <= 0:
        return 0
    return round((risk_amount / stop_distance) * entry, 2)


def check_for_signal(symbol, tf):
    with lock:
        tf_data = list(candles[symbol][tf])

    min_needed = max(EMA_SLOW_PERIOD, RSI_PERIOD, ATR_PERIOD) + 15
    if len(tf_data) < min_needed:
        return None

    closes = [c["close"] for c in tf_data[:-1]]
    ema_fast = ema_series(closes, EMA_FAST_PERIOD)
    ema_slow = ema_series(closes, EMA_SLOW_PERIOD)

    if None in (ema_fast[-1], ema_slow[-1], ema_fast[-2], ema_slow[-2]):
        return None

    bullish_cross = ema_fast[-2] <= ema_slow[-2] and ema_fast[-1] > ema_slow[-1]
    bearish_cross = ema_fast[-2] >= ema_slow[-2] and ema_fast[-1] < ema_slow[-1]

    if not (bullish_cross or bearish_cross):
        return None

    side = "LONG" if bullish_cross else "SHORT"
    rsi_value = rsi(closes, RSI_PERIOD)
    if rsi_value is None:
        return None

    if side == "LONG" and not (RSI_LONG_MIN <= rsi_value <= RSI_LONG_MAX):
        return None
    if side == "SHORT" and not (RSI_SHORT_MIN <= rsi_value <= RSI_SHORT_MAX):
        return None

    closed_candle = tf_data[-2]
    current_atr = atr(tf_data[:-1], ATR_PERIOD)
    if not current_atr or current_atr <= 0:
        return None

    if ENABLE_VOLUME_FILTER:
        volume_window = tf_data[:-2][-20:]
        avg_vol = (sum(c["volume"] for c in volume_window) / len(volume_window)) if volume_window else 0
        if avg_vol > 0 and (closed_candle["volume"] / avg_vol) < MIN_VOLUME_RATIO:
            return None

    if ENABLE_ATR_FILTER:
        if ((current_atr / closed_candle["close"]) * 100) < MIN_ATR_PCT:
            return None

    entry = closed_candle["close"]
    initial_stop = entry - (current_atr * CHANDELIER_ATR_MULT) if side == "LONG" else entry + (current_atr * CHANDELIER_ATR_MULT)

    return {
        "symbol": symbol, "tf": tf, "side": side, "entry": entry,
        "initial_stop": initial_stop, "candle_time": closed_candle["time"],
        "atr": current_atr, "rsi": round(rsi_value, 1),
    }


def open_trade(signal):
    symbol, tf = signal["symbol"], signal["tf"]
    key = trade_key(symbol, tf)

    with lock:
        if key in active_trades or last_signal_candle[symbol][tf] == signal["candle_time"]:
            return

        last_signal_candle[symbol][tf] = signal["candle_time"]
        position_size = calc_position_size(signal["entry"], signal["initial_stop"])

        trade = {
            "symbol": symbol, "tf": tf, "side": signal["side"],
            "entry": signal["entry"], "initial_stop": signal["initial_stop"],
            "trailing_stop": signal["initial_stop"], "extreme_price": signal["entry"],
            "atr": signal["atr"], "status": "ACTIVE", "position_size_usdt": position_size,
            "created_at": time.time(), "r_value": abs(signal["entry"] - signal["initial_stop"]),
            "partial_taken": False
        }

        active_trades[key] = trade
        db_save_new_trade(trade)  # DƏRHAL SQLite BAZASINA YAZILIR

    emoji = "🟢" if trade["side"] == "LONG" else "🔴"
    send_telegram(
        f"⚡ SCALPING SİQNALI (OKX)\n\n"
        f"{emoji} {display_symbol(symbol)} {trade['side']} [{tf}]\n"
        f"Entry: {trade['entry']:.4f}\n"
        f"Initial Stop: {trade['initial_stop']:.4f}\n"
        f"RSI: {signal['rsi']} | Həcm: ~{trade['position_size_usdt']:.2f} USDT"
    )


def update_trailing_stops(key, price):
    trade_snapshot = None
    partial_tp_hit = False

    with lock:
        trade = active_trades.get(key)
        if not trade:
            return

        # 1. PARTIAL TP & BREAKEVEN DÜZƏLİŞİ
        if ENABLE_PARTIAL_TP and not trade["partial_taken"] and trade["r_value"] > 0:
            r_val = trade["r_value"]
            target_hit = (price >= trade["entry"] + r_val * PARTIAL_TP_R_MULTIPLE) if trade["side"] == "LONG" else (price <= trade["entry"] - r_val * PARTIAL_TP_R_MULTIPLE)
            
            if target_hit:
                trade["partial_taken"] = True
                partial_tp_hit = True
                # BREAKEVEN FIX: Trailing Stop-u ƏN AZI giriş qiymətinə çəkirik
                if trade["side"] == "LONG":
                    trade["trailing_stop"] = max(trade["trailing_stop"], trade["entry"])
                else:
                    trade["trailing_stop"] = min(trade["trailing_stop"], trade["entry"])

        # 2. TRAILING STOP YENİLƏNMƏSİ
        result = None
        if trade["side"] == "LONG":
            if price > trade["extreme_price"]:
                trade["extreme_price"] = price
                new_stop = trade["extreme_price"] - trade["atr"] * CHANDELIER_ATR_MULT
                if new_stop > trade["trailing_stop"]:
                    trade["trailing_stop"] = new_stop
            if price <= trade["trailing_stop"]:
                result = "WIN" if trade["trailing_stop"] >= trade["entry"] else "LOSS"
        else:
            if price < trade["extreme_price"]:
                trade["extreme_price"] = price
                new_stop = trade["extreme_price"] + trade["atr"] * CHANDELIER_ATR_MULT
                if new_stop < trade["trailing_stop"]:
                    trade["trailing_stop"] = new_stop
            if price >= trade["trailing_stop"]:
                result = "WIN" if trade["trailing_stop"] <= trade["entry"] else "LOSS"

        # Bazadakı aktiv trade məlumatını yeniləyirik
        if partial_tp_hit or result is None:
            db_update_active_trade(trade)

        if result:
            trade["status"] = result
            trade["exit_price"] = price
            trade["closed_at"] = time.time()
            active_trades.pop(key, None)
            trade_snapshot = dict(trade)
            db_close_trade(trade_snapshot)  # BAZADA BAĞLAYIRIQ

    if partial_tp_hit:
        send_telegram(
            f"💰 PARTIAL TP & BREAKEVEN — {display_symbol(trade['symbol'])} [{trade['tf']}]\n"
            f"0.8R mənfəət götürüldü (60%). Stop loss Giriş Qiymətinə (`{trade['entry']:.4f}`) çəkildi!"
        )

    if trade_snapshot:
        emoji = "✅" if trade_snapshot["status"] == "WIN" else "❌"
        send_telegram(
            f"{emoji} TRADE BAĞLANDI — {trade_snapshot['status']}\n\n"
            f"{display_symbol(trade_snapshot['symbol'])} {trade_snapshot['side']} [{trade_snapshot['tf']}]\n"
            f"Entry: {trade_snapshot['entry']:.4f} | Exit: {trade_snapshot['exit_price']:.4f}"
        )


# ============================================================
# POLLING WORKERS
# ============================================================

def candle_worker():
    last_seen_time = {s: {tf: None for tf in TIMEFRAMES} for s in SYMBOLS}
    while True:
        for symbol in SYMBOLS:
            for tf in TIMEFRAMES:
                tf_candles = fetch_klines(symbol, tf)
                if tf_candles:
                    with lock:
                        candles[symbol][tf] = tf_candles[-MAX_CANDLES:]
                    closed_time = tf_candles[-2]["time"] if len(tf_candles) >= 2 else None
                    if closed_time and closed_time != last_seen_time[symbol][tf]:
                        last_seen_time[symbol][tf] = closed_time
                        sig = check_for_signal(symbol, tf)
                        if sig:
                            open_trade(sig)
                time.sleep(0.05)
        time.sleep(CANDLE_POLL_SECONDS)


def price_worker():
    while True:
        with lock:
            keys_to_check = list(active_trades.keys())

        if keys_to_check:
            tickers = fetch_all_swap_tickers()
            for key in keys_to_check:
                symbol = key.split("|")[0]
                if symbol in tickers:
                    update_trailing_stops(key, tickers[symbol]["last"])

        time.sleep(PRICE_POLL_SECONDS)


# ============================================================
# STARTUP
# ============================================================

def startup():
    global _startup_done
    with _startup_lock:
        if _startup_done or not check_single_instance():
            return
        _startup_done = True

    print("🚀 SCALPING BOTU İŞƏ DÜŞDÜ (OKX)...")

    # BAZADAN AKTİV TRADELƏRİN BƏRPASI (RESTART DEFENCE)
    restored_trades = db_load_active_trades()
    if restored_trades:
        with lock:
            active_trades.update(restored_trades)
        print(f"📦 SQLite Bazadan {len(restored_trades)} aktiv trade bərpa olundu!")

    threading.Thread(target=candle_worker, daemon=True).start()
    threading.Thread(target=price_worker, daemon=True).start()

    if USE_POLLING:
        threading.Thread(target=telegram_polling_worker, daemon=True).start()

    if SEND_STARTUP_MESSAGE:
        send_telegram(
            f"⚡ SCALPING BOTU AKTİVDİR!\n\n"
            f"📡 Cütlər: {', '.join(display_symbol(s) for s in SYMBOLS)}\n"
            f"⏱️ TF: {', '.join(TIMEFRAMES)}\n"
            f"📦 Bərpa olunan trade: {len(restored_trades)}"
        )


# ============================================================
# ROUTES & MAIN
# ============================================================

@app.route("/")
def home():
    with lock:
        ac = len(active_trades)
    return jsonify({"status": "online", "active_trades": ac, "stats": get_statistics()})

@app.route("/health")
def health(): return "OK", 200

startup()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, use_reloader=False)
