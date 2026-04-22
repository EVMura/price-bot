"""
Telegram-бот для мониторинга цен товаров.

Улучшения v2:
  1. SQLite вместо JSON — конкурентная запись, транзакции, индексы
  2. Парсеры под конкретные магазины (Kaspi, Wildberries, OZON, Amazon)
  3. Поддержка прокси через переменную среды PROXY_URL
  4. Режим уведомлений: только снижение (по умолчанию) или любое изменение

Запуск:
    pip install -r requirements.txt
    export BOT_TOKEN="токен"
    export PROXY_URL="socks5://user:pass@host:port"   # опционально
    export NOTIFY_MODE="any"                           # опционально
    python price_bot.py
"""

import os
import re
import json
import sqlite3
import logging
import asyncio
import contextlib
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ═══════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ═══════════════════════════════════════════════════════════════
BOT_TOKEN       = os.getenv("BOT_TOKEN", "8756640177:AAH7J9OXyQvmjqadURaMNWXk78p7BwOKUuk")
DB_PATH         = Path(os.getenv("DB_PATH", "prices.db"))
CHECK_INTERVAL  = int(os.getenv("CHECK_INTERVAL", 60 * 30))  # секунды
REQUEST_TIMEOUT = 15

# Прокси — PROXY_URL=socks5://user:pass@host:port  или  http://...
_proxy  = os.getenv("PROXY_URL")
PROXIES = {"http": _proxy, "https": _proxy} if _proxy else None

# NOTIFY_MODE: "drop" — уведомлять только при снижении цены
#              "any"  — при любом изменении (рост тоже)
NOTIFY_MODE = os.getenv("NOTIFY_MODE", "drop")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ — SQLite
# ═══════════════════════════════════════════════════════════════

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # параллельные читатели без блокировок
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS items (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                url         TEXT    NOT NULL,
                title       TEXT    NOT NULL DEFAULT '',
                last_price  REAL,
                min_price   REAL,
                added_at    TEXT    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_user ON items(user_id);
        """)
    log.info("БД готова: %s", DB_PATH)


def db_add_item(user_id: int, url: str, title: str, price: float | None) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO items (user_id, url, title, last_price, min_price, added_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, url, title, price, price, datetime.utcnow().isoformat()),
        )
        return cur.lastrowid


def db_remove_item(item_id: int, user_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM items WHERE id = ? AND user_id = ?", (item_id, user_id)
        )
        return cur.rowcount > 0


def db_get_items(user_id: int) -> list[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM items WHERE user_id = ? ORDER BY id", (user_id,)
        ).fetchall()


def db_get_all_items() -> list[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM items ORDER BY user_id, id"
        ).fetchall()


def db_update_price(item_id: int, new_price: float) -> None:
    with get_conn() as conn:
        conn.execute(
            """UPDATE items SET
                 last_price = ?,
                 min_price  = CASE WHEN min_price IS NULL OR ? < min_price
                                   THEN ? ELSE min_price END
               WHERE id = ?""",
            (new_price, new_price, new_price, item_id),
        )


# ═══════════════════════════════════════════════════════════════
#  ПАРСИНГ ЦЕН
# ═══════════════════════════════════════════════════════════════

def _fetch_soup(url: str) -> BeautifulSoup | None:
    """Загружает страницу и возвращает BeautifulSoup или None при ошибке."""
    try:
        resp = requests.get(
            url, headers=HEADERS, timeout=REQUEST_TIMEOUT, proxies=PROXIES
        )
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException as e:
        log.warning("Ошибка загрузки %s: %s", url, e)
        return None


def extract_price_from_text(text: str) -> float | None:
    """Ищет первое «ценообразное» число в строке."""
    # убираем неразрывные пробелы между цифрами: «1 234» → «1234»
    cleaned = re.sub(r"(?<=\d)[\s\u00A0\u202F](?=\d)", "", text.strip())
    m = re.search(r"(\d{2,})(?:[.,](\d{1,2}))?", cleaned)
    if not m:
        return None
    try:
        return float(f"{m.group(1)}.{m.group(2) or '0'}")
    except ValueError:
        return None


# ── Специализированные парсеры ───────────────────────────────────────────────

def _parse_kaspi(soup: BeautifulSoup) -> float | None:
    """kaspi.kz — JSON в __NEXT_DATA__ → span с классом цены."""
    # 1. Пробуем React-данные в <script id="__NEXT_DATA__">
    nd = soup.find("script", id="__NEXT_DATA__")
    if nd:
        with contextlib.suppress(Exception):
            data = json.loads(nd.string)
            price = (
                data["props"]["pageProps"]
                    .get("productData", {})
                    .get("offer", {})
                    .get("price")
            )
            if price:
                return float(price)
    # 2. Фолбэк по CSS-селектору
    for sel in (
        "span.item__price-once",
        "[data-zone-name='price'] span",
        ".price-block span",
    ):
        tag = soup.select_one(sel)
        if tag:
            p = extract_price_from_text(tag.get_text())
            if p:
                return p
    return None


def _parse_wildberries(soup: BeautifulSoup) -> float | None:
    """wildberries.ru / wildberries.kz"""
    for sel in (
        "ins.price-block__final-price",
        "span.price-block__final-price",
        "span.price-block__wallet-price",
        "[class*='priceWithSale']",
        "[class*='price-block']",
    ):
        tag = soup.select_one(sel)
        if tag:
            p = extract_price_from_text(tag.get_text())
            if p:
                return p
    return None


def _parse_ozon(soup: BeautifulSoup) -> float | None:
    """ozon.ru / ozon.kz — OZON активно использует JS, но JSON-LD иногда есть."""
    for script in soup.find_all("script", type="application/ld+json"):
        with contextlib.suppress(Exception):
            data = json.loads(script.string or "")
            if data.get("@type") == "Product":
                offers = data.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0]
                raw = offers.get("price") or offers.get("lowPrice")
                if raw is not None:
                    return float(str(raw).replace(",", "."))
    for sel in (
        "[class*='price'] span",
        "[data-widget='webPrice'] span",
    ):
        tag = soup.select_one(sel)
        if tag:
            p = extract_price_from_text(tag.get_text())
            if p:
                return p
    return None


def _parse_amazon(soup: BeautifulSoup) -> float | None:
    """amazon.com / amazon.co.uk / amazon.de и т.д."""
    for sel in (
        "span#priceblock_ourprice",
        "span#priceblock_dealprice",
        "span.a-price-whole",
        "span#price_inside_buybox",
        "span.a-offscreen",
    ):
        tag = soup.select_one(sel)
        if tag:
            p = extract_price_from_text(tag.get_text())
            if p:
                return p
    return None


# Реестр магазинов: часть домена → парсер
# Чтобы добавить свой магазин: впиши домен и напиши функцию выше
SHOP_PARSERS: dict[str, callable] = {
    "kaspi.kz":       _parse_kaspi,
    "wildberries.ru": _parse_wildberries,
    "wildberries.kz": _parse_wildberries,
    "ozon.ru":        _parse_ozon,
    "ozon.kz":        _parse_ozon,
    "amazon.":        _parse_amazon,  # покрывает .com, .de, .co.uk и т.д.
}


def _get_shop_parser(url: str):
    netloc = urlparse(url).netloc.lower()
    for domain, fn in SHOP_PARSERS.items():
        if domain in netloc:
            return fn
    return None


# ── Универсальный фолбэк ─────────────────────────────────────────────────────

def _parse_generic(soup: BeautifulSoup) -> float | None:
    """JSON-LD (Schema.org) → meta-теги → itemprop."""
    for script in soup.find_all("script", type="application/ld+json"):
        with contextlib.suppress(Exception):
            data = json.loads(script.string or "")
            for item in (data if isinstance(data, list) else [data]):
                if not isinstance(item, dict):
                    continue
                if item.get("@type") in ("Product", ["Product"]):
                    offers = item.get("offers", {})
                    if isinstance(offers, list):
                        offers = offers[0] if offers else {}
                    raw = offers.get("price") or offers.get("lowPrice")
                    if raw is not None:
                        return float(str(raw).replace(",", "."))

    for prop, attr in (
        ("product:price:amount", "property"),
        ("og:price:amount",      "property"),
        ("price",                "name"),
    ):
        tag = soup.find("meta", {attr: prop})
        if tag and tag.get("content"):
            p = extract_price_from_text(tag["content"])
            if p:
                return p

    tag = soup.find(attrs={"itemprop": "price"})
    if tag:
        return extract_price_from_text(tag.get("content") or tag.get_text())

    return None


# ── Публичный интерфейс ──────────────────────────────────────────────────────

def parse_price(url: str) -> tuple[float | None, str]:
    """
    Возвращает (цена, название).
    Порядок: специфичный парсер → универсальный фолбэк.
    """
    soup = _fetch_soup(url)
    if soup is None:
        return None, ""

    price: float | None = None

    shop_fn = _get_shop_parser(url)
    if shop_fn:
        with contextlib.suppress(Exception):
            price = shop_fn(soup)

    if price is None:
        price = _parse_generic(soup)

    # Название
    title = ""
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        title = og["content"]
    elif soup.title:
        title = soup.title.get_text(strip=True)
    title = (title or urlparse(url).netloc).strip()[:120]

    return price, title


# ═══════════════════════════════════════════════════════════════
#  ЛОГИКА УВЕДОМЛЕНИЙ
# ═══════════════════════════════════════════════════════════════

def _check_notify(old: float | None, new: float) -> str | None:
    """
    Возвращает направление ("drop" | "rise") если уведомление нужно,
    иначе None.
    """
    if old is None:
        return None
    if new < old:
        return "drop"
    if NOTIFY_MODE == "any" and new != old:
        return "rise"
    return None


def _notify_text(row: sqlite3.Row, old: float, new: float, direction: str) -> str:
    diff = abs(new - old)
    pct  = diff / old * 100
    if direction == "drop":
        header = "📉 *Цена снизилась!*"
        change = f"Экономия: {diff:.2f} ({pct:.1f}%)"
    else:
        header = "📈 *Цена выросла*"
        change = f"Подорожание: +{diff:.2f} (+{pct:.1f}%)"

    return (
        f"{header}\n\n"
        f"*{row['title']}*\n"
        f"Было:  {old:.2f}\n"
        f"Стало: *{new:.2f}*\n"
        f"{change}\n\n"
        f"[Открыть товар]({row['url']})"
    )


# ═══════════════════════════════════════════════════════════════
#  ОБРАБОТЧИКИ КОМАНД
# ═══════════════════════════════════════════════════════════════

async def cmd_start(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    mode_label = "снижения цены" if NOTIFY_MODE == "drop" else "любого изменения цены"
    await update.message.reply_text(
        "👋 Привет! Я слежу за ценами товаров.\n\n"
        "Пришли ссылку на товар — буду проверять каждые "
        f"{CHECK_INTERVAL // 60} мин. и уведомлю при {mode_label}.\n\n"
        "/list  — мои товары\n"
        "/check — проверить прямо сейчас\n"
        "/help  — справка"
    )


async def cmd_help(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    shops = ", ".join(SHOP_PARSERS.keys())
    await update.message.reply_text(
        "Отправь ссылку — добавлю товар в отслеживание.\n\n"
        f"Специальные парсеры: {shops}\n"
        "Все остальные сайты — универсальный парсер (JSON-LD / meta).\n\n"
        "/list  — список отслеживаемых товаров\n"
        "/check — ручная проверка всех цен\n\n"
        "Переменные среды:\n"
        "• BOT_TOKEN — токен от @BotFather\n"
        "• PROXY_URL — прокси (socks5://... или http://...)\n"
        "• NOTIFY_MODE — drop (только снижение) | any (любое изменение)\n"
        f"• CHECK_INTERVAL — интервал в секундах (сейчас {CHECK_INTERVAL})"
    )


async def cmd_list(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    items = await asyncio.to_thread(db_get_items, update.effective_user.id)
    if not items:
        await update.message.reply_text("Пока ничего не отслеживается.")
        return

    for row in items:
        price_str = f"{row['last_price']:.2f}" if row["last_price"] is not None else "—"
        min_str   = f"{row['min_price']:.2f}"  if row["min_price"]  is not None else "—"
        text = (
            f"*{row['title']}*\n"
            f"Сейчас: {price_str}  |  Минимум: {min_str}\n"
            f"[Открыть]({row['url']})"
        )
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Удалить", callback_data=f"del:{row['id']}")]]
        )
        await update.message.reply_text(
            text, reply_markup=kb,
            parse_mode="Markdown", disable_web_page_preview=True,
        )


async def on_delete(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    item_id = int(query.data.split(":")[1])
    ok = await asyncio.to_thread(db_remove_item, item_id, update.effective_user.id)
    await query.edit_message_text(
        "🗑 Товар удалён." if ok else "Товар уже был удалён."
    )


async def on_link(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text.strip()
    m = re.search(r"https?://\S+", text)
    if not m:
        await update.message.reply_text(
            "Не вижу ссылки. Пришли URL, начинающийся с http:// или https://"
        )
        return

    url = m.group(0)
    await update.message.reply_text("🔎 Загружаю страницу, ищу цену...")

    price, title = await asyncio.to_thread(parse_price, url)
    await asyncio.to_thread(db_add_item, update.effective_user.id, url, title, price)

    if price is None:
        await update.message.reply_text(
            f"⚠️ Добавил *{title or url}*, но цену не нашёл.\n"
            "Возможно, сайт блокирует ботов или рендерит цену через JavaScript.\n"
            "Попробую снова при следующей плановой проверке.",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text(
            f"✅ Добавлено: *{title}*\nЦена: *{price:.2f}*",
            parse_mode="Markdown", disable_web_page_preview=True,
        )


# ═══════════════════════════════════════════════════════════════
#  ПРОВЕРКА ЦЕН
# ═══════════════════════════════════════════════════════════════

async def check_all_prices(
    context: ContextTypes.DEFAULT_TYPE,
    notify_user_id: int | None = None,
) -> int:
    """Проверяет все товары и рассылает уведомления. Возвращает число уведомлений."""
    rows = await asyncio.to_thread(db_get_all_items)
    sent = 0

    for row in rows:
        uid = row["user_id"]
        if notify_user_id is not None and uid != notify_user_id:
            continue

        new_price, _ = await asyncio.to_thread(parse_price, row["url"])
        if new_price is None:
            continue

        old_price = row["last_price"]
        await asyncio.to_thread(db_update_price, row["id"], new_price)

        direction = _check_notify(old_price, new_price)
        if direction:
            sent += 1
            msg = _notify_text(row, old_price, new_price, direction)
            try:
                await context.bot.send_message(
                    chat_id=uid, text=msg,
                    parse_mode="Markdown", disable_web_page_preview=True,
                )
            except Exception as e:
                log.warning("Не смог уведомить user_id=%d: %s", uid, e)

    return sent


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("⏳ Проверяю цены, подожди...")
    sent = await check_all_prices(context, notify_user_id=update.effective_user.id)
    if sent == 0:
        await update.message.reply_text("Изменений нет — цены актуальны.")


async def scheduled_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    log.info("Плановая проверка цен...")
    sent = await check_all_prices(context)
    log.info("Готово, уведомлений: %d", sent)


# ═══════════════════════════════════════════════════════════════
#  ЗАПУСК
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    if BOT_TOKEN == "PASTE_YOUR_TOKEN_HERE":
        raise SystemExit("Задай переменную окружения BOT_TOKEN")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help",  cmd_help))
    app.add_handler(CommandHandler("list",  cmd_list))
    app.add_handler(CommandHandler("check", cmd_check))
    app.add_handler(CallbackQueryHandler(on_delete, pattern=r"^del:\d+$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_link))
    app.job_queue.run_repeating(scheduled_check, interval=CHECK_INTERVAL, first=60)

    log.info(
        "Бот запущен | интервал=%ds | режим=%s | прокси=%s",
        CHECK_INTERVAL, NOTIFY_MODE, bool(PROXIES),
    )
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
