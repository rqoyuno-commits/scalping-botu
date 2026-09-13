"""
SCALPING SİQNAL BOTU (OKX + MULTI-TIMEFRAME, TELEGRAM-ONLY)
===========================================================
STRATEGİYA: EMA(9)/EMA(21) KROSOVER + RSI FİLTRİ + HƏCM TƏSDİQİ
------------------------------------------------------------
Bu bot əvvəlki "Trend Breakout" botundan tamam fərqlidir:

1. TIMEFRAME-LƏR QISADIR (scalping üçün): 1m, 3m, 5m — hər biri
   MÜSTƏQİL izlənilir (əvvəlki bot kimi).
2. SİQNAL MƏNTİQİ - EMA CROSSOVER:
   - EMA(9) EMA(21)-i yuxarı keçəndə (bullish cross) -> LONG siqnalı
   - EMA(9) EMA(21)-i aşağı keçəndə (bearish cross) -> SHORT siqnalı
   Yalnız TƏZƏ bağlanmış şamda baş vermiş kross qəbul edilir (repaint yoxdur).
3. RSI(14) FİLTRİ: Scalping-də ən pis şey artıq "yorulmuş" hərəkətə
   girməkdir. Ona görə:
   - LONG üçün RSI 40-75 aralığında olmalıdır (artıq həddindən aşırı
     alınmış olmamalıdır)
   - SHORT üçün RSI 25-60 aralığında olmalıdır
4. HƏCM FİLTRİ: Kross anında həcm son 20 şamın ortalamasından
   MIN_VOLUME_RATIO qədər yüksək olmalıdır (yalanı krosslardan qorunmaq üçün).
5. ATR FİLTRİ: Bazar çox "ölü" olduqda (aşağı volatilite) scalping siqnalı
   verilmir.
6. RİSK İDARƏETMƏSİ: Sıx Chandelier-tipli trailing stop (ATR x 1.2) və
   tez partial take-profit (0.8R-də), çünki scalping-də mənfəət pəncərəsi
   kiçikdir və tez bağlamaq lazımdır.

QALAN HİSSƏLƏR (thread-safety, PID lock, SQLite, Telegram komandaları,
Flask endpoint-ləri) əvvəlki bot ilə eyni memarlıqdadır ki, tanış interfeys
qalsın (/status, /stats, /active, /help).

QEYD: Bu bot yalnız Telegram-a SİQNAL göndərir, real sifariş açmır/bağlamır.
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

# ------------------------------------------------------------
# OKX ENDPOINTS
# ------------------------------------------------------------
OKX_BASE = "https://www.okx.com"
OKX_KLINE_URL = f"{OKX_BASE}/api/v5/market/candles"
OKX_TICKER_URL = f"{OKX_BASE}/api/v5/market/ticker"

OKX_PROXY_URL = os.getenv("OKX_PROXY_URL", "").strip()
OKX_PROXIES = {"http": OKX_PROXY_URL, "https": OKX_PROXY_URL} if OKX_PROXY_URL else None

SYMBOLS = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"]

# Scalping üçün QISA timeframe-lər. OKX bar formatı: 1m, 3m, 5m
# (NOT: bunların 'utc' variantı yoxdur və lazım da deyil - saat sərhədi
# problemi yalnız 6H+ üçündür).
TIMEFRAMES = ["1m", "3m", "5m"]

# Scalping-də tez reaksiya lazımdır - qısa poll interval-ları
CANDLE_POLL_SECONDS = 8
PRICE_POLL_SECONDS = 4
MAX_CANDLES = 300

EMA_FAST_PERIOD = 9
EMA_SLOW_PERIOD = 21
RSI_PERIOD = 14
ATR_PERIOD = 14

# LONG üçün qəbul edilən RSI aralığı (artıq "yorulmuş" bazara girməmək üçün)
RSI_LONG_MIN = 40
RSI_LONG_MAX = 75
# SHORT üçün qəbul edilən RSI aralığı
RSI_SHORT_MIN = 25
RSI_SHORT_MAX = 60

# Scalping-də stop daha sıxdır (trend botunda 3.0 idi)
CHANDELIER_ATR_MULT = 1.2

ACCOUNT_BALANCE_USDT = float(os.getenv("ACCOUNT_BALANCE_USDT", "1000"))
RISK_PER_TRADE_PCT = 0.005  # scalping-də daha kiçik risk/trade

MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "99999"))
MAX_CONSECUTIVE_LOSSES = 4
COOLDOWN_HOURS_AFTER_LOSSES = 6  # scalping-də daha qısa cooldown

MAX_CONSECUTIVE_FETCH_FAILS = 5

# ------------------------------------------------------------
# SİQNAL FİLTRLƏRİ
# ------------------------------------------------------------
ENABLE_VOLUME_FILTER = os.getenv("ENABLE_VOLUME_FILTER", "True").lower() == "true"
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.3"))

ENABLE_ATR_FILTER = os.getenv("ENABLE_ATR_FILTER", "True").lower() == "true"
MIN_ATR_PCT = float(os.getenv("MIN_ATR_PCT", "0.08"))

ENABLE_SPREAD_FILTER = os.getenv("ENABLE_SPREAD_FILTER", "True").lower() == "true"
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.12"))

MAX_SAME_DIRECTION_TRADES = int(os.getenv("MAX_SAME_DIRECTION_TRADES", "5"))

# Scalping-də erkən qismən mənfəət götürmək vacibdir
ENABLE_PARTIAL_TP = os.getenv("ENABLE_PARTIAL_TP", "True").lower() == "true"
PARTIAL_TP_R_MULTIPLE = float(os.getenv("PARTIAL_TP_R_MULTIPLE", "0.8"))
PARTIAL_TP_CLOSE_PCT = float(os.getenv("PARTIAL_TP_CLOSE_PCT", "0.6"))

DB_FILE = "scalping_bot.db"
PID_FILE = "scalping_bot.lock"


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# GLOBAL STATE & LOCKS
# ============================================================

lock = threading.Lock()
db_lock = threading.Lock()

candles = {s: {tf: [] for tf in TIMEFRAMES} for s in SYMBOLS}
active_trades = {}
last_signal_candle = {s: {tf: None for tf in TIMEFRAMES} for s in SYMBOLS}

daily_trade_count = 0
daily_count_date = None

consecutive_losses = 0
cooldown_until = None

fetch_fail_counts = {s: {tf: 0 for tf in TIMEFRAMES} for s in SYMBOLS}
fetch_fail_alerted = {s: {tf: False for tf in TIMEFRAMES} for s in SYMBOLS}

_startup_done = False
_startup_lock = threading.Lock()
_owns_pid_lock = False


def trade_key(symbol, tf):
    return f"{symbol}|{tf}"


def display_symbol(symbol):
    parts = symbol.split("-")
    if len(parts) >= 2:
        return f"{parts[0]}/{parts[1]}"
    return symbol


# ============================================================
# SINGLE INSTANCE GUARD (PID LOCK)
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
                saved_pid = f.read().strip()
            if saved_pid == str(os.getpid()):
                os.remove(PID_FILE)
    except Exception as e:
        print("⚠️ PID lock təmizləmə xətası:", e)


atexit.register(release_pid_lock)


# ============================================================
# DATABASE
# ============================================================

def init_db():
    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=15)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                side TEXT NOT NULL,
                entry REAL NOT NULL,
                initial_stop REAL NOT NULL,
                exit_price REAL,
                status TEXT NOT NULL,
                position_size_usdt REAL,
                created_at REAL NOT NULL,
                closed_at REAL
            )
        """)
        conn.commit()
        conn.close()


def save_trade(trade):
    try:
        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=15)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO trades
                (symbol, timeframe, side, entry, initial_stop, exit_price, status,
                 position_size_usdt, created_at, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade["symbol"], trade.get("tf", ""), trade["side"], trade["entry"],
                trade["initial_stop"], trade.get("exit_price"),
                trade["status"], trade.get("position_size_usdt"),
                trade["created_at"], trade.get("closed_at"),
            ))
            conn.commit()
            conn.close()
    except Exception as e:
        print("❌ save_trade xətası:", e)


def get_statistics():
    try:
        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=15)
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*),
                       COALESCE(SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END), 0)
                FROM trades
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
        print("❌ Telegram token və ya chat_id təyin edilməyib.")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": target_chat_id, "text": message}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        r = requests.post(url, json=payload, timeout=10)
        res = r.json()
        if not res.get("ok"):
            if parse_mode:
                payload.pop("parse_mode", None)
                r = requests.post(url, json=payload, timeout=10)
                return r.json().get("ok", False)
            print("❌ Telegram xətası:", res)
            return False
        return True
    except Exception as e:
        print("❌ Telegram bağlantı xətası:", e)
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

    if cmd in ["/start", "/help", "komek", "kömək", "yardim", "yardım"]:
        response_text = (
            "🤖 *SCALPING BOTU ƏMRLƏRİ*\n\n"
            "📊 /stats - Ümumi WIN/LOSS və Win Rate\n"
            "⚡ /active - Açıq olan pozisiyalar (timeframe ilə)\n"
            "🟢 /status - Botun vəziyyəti və günlük limitlər\n"
            "❓ /help - Bu menyu"
        )

    elif cmd in ["/status", "status"]:
        with lock:
            active_count = len(active_trades)
            reset_daily_counter_if_needed()
            current_daily = daily_trade_count
            current_cooldown = cooldown_until

        cooldown_str = "Aktiv deyil"
        if current_cooldown:
            cooldown_str = current_cooldown.strftime("%d.%m.%Y %H:%M UTC")

        limit_str = "Limitsiz (Test)" if MAX_TRADES_PER_DAY >= 9999 else str(MAX_TRADES_PER_DAY)

        response_text = (
            "🤖 *BOT VƏZİYYƏTİ (SCALPING)*\n\n"
            "🟢 Status: ONLINE (OKX)\n"
            f"📡 TF-lər: {', '.join(TIMEFRAMES)}\n"
            f"📈 Açıq Trade Sayı: {active_count}\n"
            f"📅 Bugünkü Trade Sayı: {current_daily}/{limit_str}\n"
            f"❄️ Cooldown: {cooldown_str}"
        )

    elif cmd in ["/stats", "stats", "statistika"]:
        stats = get_statistics()
        response_text = (
            "📊 *ÜMUMİ STATİSTİKA*\n\n"
            f"Cəmi Trade: {stats['total']}\n"
            f"✅ WIN: {stats['wins']}\n"
            f"❌ LOSS: {stats['losses']}\n"
            f"🎯 Win Rate: %{stats['win_rate']}"
        )

    elif cmd in ["/active", "active", "aciq"]:
        with lock:
            trades_list = list(active_trades.values())
        if not trades_list:
            response_text = "ℹ️ Hal-hazırda aktiv trade yoxdur."
        else:
            response_text = "⚡ *AÇIQ TRADELƏR*\n\n"
            for t in trades_list:
                emoji = "🟢" if t["side"] == "LONG" else "🔴"
                response_text += (
                    f"{emoji} *{display_symbol(t['symbol'])} {t['side']} [{t['tf']}]*\n"
                    f"Entry: `{t['entry']:.4f}`\n"
                    f"Trailing Stop: `{t['trailing_stop']:.4f}`\n"
                    f"Həcm: ~`{t['position_size_usdt']:.2f}` USDT\n\n"
                )

    if response_text:
        send_telegram(response_text, chat_id=chat_id, parse_mode="Markdown")


def telegram_polling_worker():
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("⚠️ TELEGRAM_BOT_TOKEN təyin edilmədiyi üçün Polling işə düşmədi.")
        return
    print("🤖 Telegram Long Polling başladıldı...")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook", timeout=10)
    except Exception as e:
        print("⚠️ deleteWebhook xətası:", e)

    offset = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {"offset": offset, "timeout": 20}
            resp = requests.get(url, params=params, timeout=25)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("ok"):
                    for update in data.get("result", []):
                        offset = update["update_id"] + 1
                        process_telegram_update(update)
        except Exception as e:
            print("❌ Telegram Polling xətası:", e)
            time.sleep(5)
        time.sleep(1)


# ============================================================
# OKX REST
# ============================================================

def fetch_klines(symbol, bar, limit=MAX_CANDLES):
    params = {"instId": symbol, "bar": bar, "limit": min(limit, 300)}
    try:
        r = requests.get(OKX_KLINE_URL, params=params, timeout=15, proxies=OKX_PROXIES)
        data = r.json()
        if data.get("code") != "0":
            print(f"❌ OKX kline xətası {symbol} {bar}:", data)
            _note_fetch_failure(symbol, bar)
            return []
        rows = data.get("data", [])
        rows.reverse()
        _note_fetch_success(symbol, bar)
        return [{
            "time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
        } for row in rows]
    except Exception as e:
        print(f"❌ {symbol} {bar} kline sorğu xətası:", e)
        _note_fetch_failure(symbol, bar)
        return []


def fetch_price(symbol):
    params = {"instId": symbol}
    try:
        r = requests.get(OKX_TICKER_URL, params=params, timeout=10, proxies=OKX_PROXIES)
        data = r.json()
        if data.get("code") != "0":
            _note_fetch_failure(symbol, "_price")
            return None
        lst = data.get("data", [])
        if not lst:
            _note_fetch_failure(symbol, "_price")
            return None
        _note_fetch_success(symbol, "_price")
        return float(lst[0]["last"])
    except Exception as e:
        print(f"❌ {symbol} qiymət sorğu xətası:", e)
        _note_fetch_failure(symbol, "_price")
        return None


def fetch_spread_pct(symbol):
    params = {"instId": symbol}
    try:
        r = requests.get(OKX_TICKER_URL, params=params, timeout=10, proxies=OKX_PROXIES)
        data = r.json()
        if data.get("code") != "0":
            return None
        lst = data.get("data", [])
        if not lst:
            return None
        bid = float(lst[0].get("bidPx", 0) or 0)
        ask = float(lst[0].get("askPx", 0) or 0)
        if bid <= 0 or ask <= 0:
            return None
        mid = (bid + ask) / 2
        return ((ask - bid) / mid) * 100
    except Exception as e:
        print(f"⚠️ {symbol} spread sorğu xətası:", e)
        return None


def _note_fetch_failure(symbol, tf):
    alert_needed = False
    with lock:
        d = fetch_fail_counts.setdefault(symbol, {})
        a = fetch_fail_alerted.setdefault(symbol, {})
        d[tf] = d.get(tf, 0) + 1
        if d[tf] >= MAX_CONSECUTIVE_FETCH_FAILS and not a.get(tf, False):
            a[tf] = True
            alert_needed = True
    if alert_needed:
        send_telegram(
            f"⚠️ {display_symbol(symbol)} [{tf}]: OKX-dən {MAX_CONSECUTIVE_FETCH_FAILS} ardıcıl dəfə "
            f"məlumat alına bilmədi. Şəbəkə/API problemi ola bilər."
        )


def _note_fetch_success(symbol, tf):
    was_alerted = False
    with lock:
        a = fetch_fail_alerted.setdefault(symbol, {})
        d = fetch_fail_counts.setdefault(symbol, {})
        if a.get(tf, False):
            was_alerted = True
        d[tf] = 0
        a[tf] = False
    if was_alerted:
        send_telegram(f"✅ {display_symbol(symbol)} [{tf}]: OKX bağlantısı bərpa olundu, məlumat axını normaldır.")


# ============================================================
# İNDİKATORLAR
# ============================================================

def ema_series(values, period):
    """Bütün seriya üçün EMA dəyərləri qaytarır (None-larla doldurulmuş baş hissə)."""
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
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(candles_list, period=ATR_PERIOD):
    if len(candles_list) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles_list)):
        h, l = candles_list[i]["high"], candles_list[i]["low"]
        pc = candles_list[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


# ============================================================
# RİSK İDARƏETMƏSİ
# ============================================================

def reset_daily_counter_if_needed():
    global daily_trade_count, daily_count_date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if daily_count_date != today:
        daily_count_date = today
        daily_trade_count = 0


def risk_checks_pass():
    reset_daily_counter_if_needed()
    if cooldown_until is not None:
        if datetime.now(timezone.utc) < cooldown_until:
            return False, f"Cooldown aktivdir, {cooldown_until.isoformat()} tarixinə qədər"
    if daily_trade_count >= MAX_TRADES_PER_DAY:
        return False, "Günlük trade limiti dolub"
    return True, ""


def register_trade_opened():
    global daily_trade_count
    daily_trade_count += 1


def register_trade_result(result):
    global consecutive_losses, cooldown_until
    should_alert_cooldown = False
    if result == "LOSS":
        consecutive_losses += 1
        if consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            cooldown_until_ts = datetime.now(timezone.utc).timestamp() + COOLDOWN_HOURS_AFTER_LOSSES * 3600
            cooldown_until = datetime.fromtimestamp(cooldown_until_ts, tz=timezone.utc)
            should_alert_cooldown = True
    else:
        consecutive_losses = 0
    return should_alert_cooldown


def calc_position_size(entry, stop):
    risk_amount = ACCOUNT_BALANCE_USDT * RISK_PER_TRADE_PCT
    stop_distance = abs(entry - stop)
    if stop_distance <= 0:
        return 0
    qty = risk_amount / stop_distance
    position_value_usdt = qty * entry
    return round(position_value_usdt, 2)


# ============================================================
# SİQNAL MƏNTİQİ - EMA CROSSOVER + RSI + HƏCM (SCALPING)
# ============================================================

def check_for_signal(symbol, tf):
    with lock:
        tf_data = list(candles[symbol][tf])

    min_needed = max(EMA_SLOW_PERIOD, RSI_PERIOD, ATR_PERIOD) + 30
    if len(tf_data) < min_needed:
        return None

    # Son bağlanmış şam tf_data[-2]-dir (tf_data[-1] hələ aktiv/açıq şam ola bilər)
    closes = [c["close"] for c in tf_data[:-1]]

    ema_fast = ema_series(closes, EMA_FAST_PERIOD)
    ema_slow = ema_series(closes, EMA_SLOW_PERIOD)

    if ema_fast[-1] is None or ema_slow[-1] is None or ema_fast[-2] is None or ema_slow[-2] is None:
        return None

    prev_fast, prev_slow = ema_fast[-2], ema_slow[-2]
    curr_fast, curr_slow = ema_fast[-1], ema_slow[-1]

    bullish_cross = prev_fast <= prev_slow and curr_fast > curr_slow
    bearish_cross = prev_fast >= prev_slow and curr_fast < curr_slow

    if not (bullish_cross or bearish_cross):
        return None

    side = "LONG" if bullish_cross else "SHORT"

    rsi_value = rsi(closes[-(RSI_PERIOD + 50):], RSI_PERIOD)
    if rsi_value is None:
        return None

    if side == "LONG" and not (RSI_LONG_MIN <= rsi_value <= RSI_LONG_MAX):
        print(f"⏸️ {symbol}[{tf}] LONG kross rədd edildi: RSI aralıq xaricində ({rsi_value:.1f})")
        return None
    if side == "SHORT" and not (RSI_SHORT_MIN <= rsi_value <= RSI_SHORT_MAX):
        print(f"⏸️ {symbol}[{tf}] SHORT kross rədd edildi: RSI aralıq xaricində ({rsi_value:.1f})")
        return None

    closed_candle = tf_data[-2]
    current_atr = atr(tf_data[:-1], ATR_PERIOD)
    if current_atr is None or current_atr <= 0:
        return None

    volume_window = tf_data[:-2][-20:]
    avg_volume = (sum(c["volume"] for c in volume_window) / len(volume_window)) if volume_window else None
    breakout_volume = closed_candle["volume"]

    if ENABLE_VOLUME_FILTER and avg_volume:
        volume_ratio = breakout_volume / avg_volume if avg_volume else 0
        if volume_ratio < MIN_VOLUME_RATIO:
            print(f"⏸️ {symbol}[{tf}] kross rədd edildi: həcm zəif ({volume_ratio:.2f}x < {MIN_VOLUME_RATIO}x)")
            return None

    if ENABLE_ATR_FILTER:
        atr_pct = (current_atr / closed_candle["close"]) * 100
        if atr_pct < MIN_ATR_PCT:
            print(f"⏸️ {symbol}[{tf}] kross rədd edildi: ATR çox aşağıdır ({atr_pct:.3f}% < {MIN_ATR_PCT}%)")
            return None

    if ENABLE_SPREAD_FILTER:
        spread_pct = fetch_spread_pct(symbol)
        if spread_pct is not None and spread_pct > MAX_SPREAD_PCT:
            print(f"⏸️ {symbol}[{tf}] kross rədd edildi: spread çox geniş ({spread_pct:.3f}% > {MAX_SPREAD_PCT}%)")
            return None

    entry = closed_candle["close"]
    if side == "LONG":
        initial_stop = entry - current_atr * CHANDELIER_ATR_MULT
        if initial_stop >= entry:
            return None
    else:
        initial_stop = entry + current_atr * CHANDELIER_ATR_MULT
        if initial_stop <= entry:
            return None

    return {
        "symbol": symbol, "tf": tf, "side": side, "entry": entry,
        "initial_stop": initial_stop, "candle_time": closed_candle["time"],
        "atr": current_atr, "rsi": round(rsi_value, 1),
    }


def open_trade(signal):
    symbol = signal["symbol"]
    tf = signal["tf"]
    key = trade_key(symbol, tf)
    trade = None

    with lock:
        if key in active_trades:
            return
        if last_signal_candle[symbol][tf] == signal["candle_time"]:
            return

        ok, reason = risk_checks_pass()
        if not ok:
            print(f"⏸️ {symbol}[{tf}] siqnalı rədd edildi: {reason}")
            return

        same_direction_count = sum(
            1 for t in active_trades.values() if t["side"] == signal["side"]
        )
        if same_direction_count >= MAX_SAME_DIRECTION_TRADES:
            print(
                f"⏸️ {symbol}[{tf}] siqnalı rədd edildi: {signal['side']} istiqamətində "
                f"artıq {same_direction_count} aktiv trade var (limit: {MAX_SAME_DIRECTION_TRADES})"
            )
            return

        last_signal_candle[symbol][tf] = signal["candle_time"]
        position_size = calc_position_size(signal["entry"], signal["initial_stop"])

        trade = {
            "symbol": symbol,
            "tf": tf,
            "side": signal["side"],
            "entry": signal["entry"],
            "initial_stop": signal["initial_stop"],
            "trailing_stop": signal["initial_stop"],
            "extreme_price": signal["entry"],
            "atr": signal["atr"],
            "status": "ACTIVE",
            "position_size_usdt": position_size,
            "created_at": time.time(),
            "r_value": abs(signal["entry"] - signal["initial_stop"]),
            "partial_taken": False,
        }
        active_trades[key] = trade
        register_trade_opened()
        current_daily_count = daily_trade_count

    if trade is None:
        return

    emoji = "🟢" if trade["side"] == "LONG" else "🔴"
    limit_str = "Limitsiz (Test)" if MAX_TRADES_PER_DAY >= 9999 else str(MAX_TRADES_PER_DAY)

    message = f"""
⚡ SCALPING SİQNALI (TEST REJİMİ) — OKX

{emoji} {display_symbol(symbol)} {trade["side"]}  [{tf}]

Entry: {trade["entry"]:.4f}
İlkin Stop: {trade["initial_stop"]:.4f}
RSI: {signal.get("rsi", "-")}
Tövsiyə olunan pozisiya: ~{trade["position_size_usdt"]:.2f} USDT

Səbəb: EMA{EMA_FAST_PERIOD}/EMA{EMA_SLOW_PERIOD} krosover + RSI filtri + həcm təsdiqi ({tf} timeframe-də)

⏳ Status: ACTIVE — Sıx Trailing Stop Aktivdir
📅 Günlük Trade Sayı: {current_daily_count}/{limit_str}
"""
    print(message)
    send_telegram(message)


def update_trailing_stops(key, price):
    trade_snapshot = None
    should_alert_cooldown = False
    partial_tp_hit = None

    with lock:
        trade = active_trades.get(key)
        if not trade:
            return

        if ENABLE_PARTIAL_TP and not trade.get("partial_taken", False) and trade.get("r_value", 0) > 0:
            r_value = trade["r_value"]
            if trade["side"] == "LONG":
                target = trade["entry"] + r_value * PARTIAL_TP_R_MULTIPLE
                hit = price >= target
            else:
                target = trade["entry"] - r_value * PARTIAL_TP_R_MULTIPLE
                hit = price <= target
            if hit:
                trade["partial_taken"] = True
                trade["partial_exit_price"] = price
                partial_tp_hit = dict(trade)

        result = None

        if trade["side"] == "LONG":
            if price > trade["extreme_price"]:
                trade["extreme_price"] = price
                new_stop = trade["extreme_price"] - trade["atr"] * CHANDELIER_ATR_MULT
                if new_stop > trade["trailing_stop"]:
                    trade["trailing_stop"] = new_stop
            if price <= trade["trailing_stop"]:
                result = "WIN" if trade["trailing_stop"] > trade["entry"] else "LOSS"
        else:
            if price < trade["extreme_price"]:
                trade["extreme_price"] = price
                new_stop = trade["extreme_price"] + trade["atr"] * CHANDELIER_ATR_MULT
                if new_stop < trade["trailing_stop"]:
                    trade["trailing_stop"] = new_stop
            if price >= trade["trailing_stop"]:
                result = "WIN" if trade["trailing_stop"] < trade["entry"] else "LOSS"

        if result is None:
            if partial_tp_hit is None:
                return
        else:
            trade["status"] = result
            trade["exit_price"] = price
            trade["closed_at"] = time.time()
            active_trades.pop(key, None)
            should_alert_cooldown = register_trade_result(result)
            trade_snapshot = dict(trade)

    if partial_tp_hit is not None:
        pct = int(PARTIAL_TP_CLOSE_PCT * 100)
        send_telegram(
            f"💰 PARTIAL TAKE-PROFIT — {display_symbol(partial_tp_hit['symbol'])} "
            f"{partial_tp_hit['side']} [{partial_tp_hit['tf']}]\n\n"
            f"{PARTIAL_TP_R_MULTIPLE}R hədəfinə çatıldı, pozisiyanın ~{pct}%-i bağlandı (konseptual).\n"
            f"Entry: {partial_tp_hit['entry']:.4f}\n"
            f"Partial Exit: {price:.4f}\n"
            f"Qalan {100-pct}% trailing stop ilə davam edir."
        )

    if trade_snapshot is None:
        return

    save_trade(trade_snapshot)

    emoji = "✅" if trade_snapshot["status"] == "WIN" else "❌"
    message = f"""
{emoji} TRADE BAĞLANDI — {trade_snapshot["status"]}

{display_symbol(trade_snapshot["symbol"])} {trade_snapshot["side"]}  [{trade_snapshot["tf"]}]
Entry: {trade_snapshot["entry"]:.4f}
Exit (trailing stop): {trade_snapshot["exit_price"]:.4f}

RESULT: {trade_snapshot["status"]}
"""
    send_telegram(message)

    stats = get_statistics()
    send_telegram(
        f"📊 STATİSTİKA\nTotal: {stats['total']} | "
        f"WIN: {stats['wins']} | LOSS: {stats['losses']} | "
        f"Win Rate: {stats['win_rate']}%"
    )

    if should_alert_cooldown:
        send_telegram(
            f"⏸️ {MAX_CONSECUTIVE_LOSSES} ardıcıl itkidən sonra bot "
            f"{COOLDOWN_HOURS_AFTER_LOSSES} saat dayandırılır."
        )


# ============================================================
# POLLING WORKERS
# ============================================================

def process_pending_signals(pending_signals):
    """Scalping botunda score-qruplaşdırma yoxdur - hər (symbol, tf)
    siqnalı bir-birindən müstəqil açılır."""
    for (symbol, tf), signal in pending_signals.items():
        open_trade(signal)


def candle_worker():
    last_seen_time = {s: {tf: None for tf in TIMEFRAMES} for s in SYMBOLS}

    while True:
        pending_signals = {}

        for symbol in SYMBOLS:
            for tf in TIMEFRAMES:
                tf_candles = fetch_klines(symbol, tf)

                if tf_candles:
                    with lock:
                        candles[symbol][tf] = tf_candles[-MAX_CANDLES:]

                    closed_time = tf_candles[-2]["time"] if len(tf_candles) >= 2 else None
                    if closed_time and closed_time != last_seen_time[symbol][tf]:
                        last_seen_time[symbol][tf] = closed_time
                        signal = check_for_signal(symbol, tf)
                        if signal:
                            pending_signals[(symbol, tf)] = signal

                time.sleep(0.25)  # OKX rate-limit üçün kiçik fasilə

        process_pending_signals(pending_signals)

        time.sleep(CANDLE_POLL_SECONDS)


def price_worker():
    while True:
        with lock:
            keys_to_check = list(active_trades.keys())

        symbols_needed = {k.split("|")[0] for k in keys_to_check}
        price_cache = {}
        for symbol in symbols_needed:
            price_cache[symbol] = fetch_price(symbol)

        for key in keys_to_check:
            symbol = key.split("|")[0]
            price = price_cache.get(symbol)
            if price is not None:
                update_trailing_stops(key, price)

        time.sleep(PRICE_POLL_SECONDS)


# ============================================================
# STARTUP
# ============================================================

def startup():
    global _startup_done
    with _startup_lock:
        if _startup_done:
            return
        if not check_single_instance():
            return
        _startup_done = True

    print("🚀 SCALPING BOTU (OKX, Multi-TF) BAŞLAYIR...")

    threading.Thread(target=candle_worker, daemon=True).start()
    threading.Thread(target=price_worker, daemon=True).start()

    if USE_POLLING:
        threading.Thread(target=telegram_polling_worker, daemon=True).start()

    if SEND_STARTUP_MESSAGE:
        limit_str = "Limitsiz (Test)" if MAX_TRADES_PER_DAY >= 9999 else str(MAX_TRADES_PER_DAY)
        send_telegram(
            "⚡ SCALPING BOTU AKTİVDİR! (OKX)\n\n"
            f"📡 {', '.join(display_symbol(s) for s in SYMBOLS)} izlənilir.\n"
            f"⏱️ Timeframe-lər (hər biri MÜSTƏQİL): {', '.join(TIMEFRAMES)}\n"
            f"📊 EMA{EMA_FAST_PERIOD}/EMA{EMA_SLOW_PERIOD} Krosover + RSI + Həcm Filtri\n"
            f"⚖️ Günlük Max Trade: {limit_str}\n"
            "💾 Nəticələr SQLite-də saxlanılır.\n\n"
            "💬 Bot əmrləri üçün Telegram-da /help yazın."
        )


# ============================================================
# ROUTES & WEBHOOK
# ============================================================

@app.route("/")
def home():
    stats = get_statistics()
    with lock:
        active_count = len(active_trades)
        cooldown_snapshot = cooldown_until
    return jsonify({
        "status": "online",
        "exchange": "OKX",
        "strategy": "EMA crossover + RSI + Volume (scalping)",
        "mode": "SIGNAL-ONLY (real sifariş yoxdur)",
        "symbols": SYMBOLS,
        "timeframes": TIMEFRAMES,
        "active_trades": active_count,
        "statistics": stats,
        "cooldown_until": cooldown_snapshot.isoformat() if cooldown_snapshot else None,
    })


@app.route("/health")
def health():
    return "OK", 200


@app.route("/stats")
def stats_route():
    return jsonify(get_statistics())


@app.route("/active")
def active_route():
    with lock:
        return jsonify(list(active_trades.values()))


@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    data = request.get_json()
    if data:
        process_telegram_update(data)
    return jsonify({"status": "ok"}), 200


# ============================================================
# MAIN
# ============================================================

startup()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, use_reloader=False)
