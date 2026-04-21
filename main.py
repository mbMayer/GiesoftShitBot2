# bot_aiogram.py
# Aiogram 3.x + SQLite FSM storage (без Redis)
#
# Фикс: время всегда берётся по Екатеринбургу (Asia/Yekaterinburg), независимо от сервера.
# На Windows для zoneinfo рекомендуется установить:
#   pip install tzdata
#
# Логика:
# - При ОТКРЫТИИ смены вводим ФИО, номер смены, кассу (+ остатки если номер >= 3), комментарий, чеклист => отправляем отчёт
# - При ЗАКРЫТИИ смены НЕ спрашиваем ФИО/номер/кассу (берём из открытия), просим только комментарий (+ "Почищены клавиатуры" если смена ночная), чеклист => отправляем отчёт
# - Тип смены (дневная/ночная) определяется по времени ОТКРЫТИЯ + окно допуска SHIFT_TOLERANCE_MINUTES
# - "Остатки" в отчёте выводятся только если заполнено хотя бы одно поле
# - FSM и данные сохраняются в файл fsm.sqlite3 рядом с ботом

import os
import socket
import asyncio
import logging
import json
from datetime import datetime, time as dtime
from typing import Dict, Tuple, List, Any, Optional

from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import ClientTimeout, TCPConnector
import aiosqlite

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, ErrorEvent
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State as FSMState
from aiogram.fsm.state import State as AiogramState
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.storage.base import BaseStorage, StorageKey

from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession

from aiogram.exceptions import (
    TelegramRetryAfter,
    TelegramNetworkError,
    TelegramServerError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramAPIError,
)

# SOCKS5 (опционально): pip install aiohttp-socks
try:
    from aiohttp_socks import ProxyConnector
except Exception:
    ProxyConnector = None


# -------------------- CONFIG --------------------

# Таймзона бота (Екатеринбург)
BOT_TZ = ZoneInfo(os.getenv("BOT_TZ", "Asia/Yekaterinburg"))

ADMIN_IDS = [1134352505]
OTCHET_IDS = [-5152524381]
RECIPIENTS: List[int] = list(dict.fromkeys(ADMIN_IDS + OTCHET_IDS))

DAY_SHIFT_START = dtime(9, 0)
DAY_SHIFT_END = dtime(21, 0)

# окно допуска вокруг 09:00 и 21:00
SHIFT_TOLERANCE_MINUTES = int(os.getenv("SHIFT_TOLERANCE_MINUTES", "30"))

CHECK_ITEMS: Dict[str, str] = {
    "fridge": "Холодильник",
    "sandwiches": "Сэндвичи",
    "trashbags_task": "Мусорные пакеты",
    "napkins": "Салфетки, туалетная бумага",
    "soap_task": "Мыло",
    "wetwipes_task": "Влажные салфетки",
    "games": "Обновление игр",
}

STOCK_QUESTIONS: Tuple[Tuple[str, str], ...] = (
    ("paperself", "Введите наличие бумажных полотенец (упаковок):"),
    ("trashbags", "Введите наличие мусорных пакетов (упаковок):"),
    ("soap", "Введите наличие мыла (упаковок/бутылок):"),
    ("waterself", "Введите наличие влажных салфеток (коробок):"),
    ("tea", "Введите наличие чая (упаковок):"),
    ("sugar", "Введите наличие сахара (упаковок):"),
    ("teaspoons", "Введите наличие чайных ложек (упаковок):"),
    ("water19l", "Введите наличие воды 19л (бутылей):"),
    ("antiseptic", "Введите наличие антисептика (бутылок):"),
    ("floorwash", "Введите наличие средства для пола (бутылок):"),
)

STOCK_LABELS: Dict[str, str] = {
    "paperself": "Бумажные полотенца",
    "trashbags": "Пакеты для мусора",
    "soap": "Мыло",
    "waterself": "Влажные салфетки",
    "tea": "Чай",
    "sugar": "Сахар",
    "teaspoons": "Ложки для чая",
    "water19l": "Вода 19л",
    "antiseptic": "Антисептик",
    "floorwash": "Средство для пола",
}
STOCK_KEYS = list(STOCK_LABELS.keys())


# -------------------- LOGGING --------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("shift-bot-aiogram")


# -------------------- SQLite FSM STORAGE --------------------

class SQLiteFSMStorage(BaseStorage):
    """
    FSM storage в SQLite файле.
    ВАЖНО: aiogram может передавать state как объект State => сохраняем state.state (строку).
    """
    def __init__(self, path: str = "fsm.sqlite3"):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def _connect(self) -> aiosqlite.Connection:
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.path, timeout=30)
            await self._conn.execute("PRAGMA journal_mode=WAL;")
            await self._conn.execute("PRAGMA synchronous=NORMAL;")
            await self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fsm (
                    k TEXT PRIMARY KEY,
                    state TEXT,
                    data TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            await self._conn.commit()
        return self._conn

    @staticmethod
    def _k(key: StorageKey) -> str:
        bot_id = getattr(key, "bot_id", "")
        chat_id = getattr(key, "chat_id", "")
        user_id = getattr(key, "user_id", "")
        thread_id = getattr(key, "thread_id", "")
        destiny = getattr(key, "destiny", "")
        business_connection_id = getattr(key, "business_connection_id", "")
        return f"{bot_id}:{chat_id}:{user_id}:{thread_id}:{business_connection_id}:{destiny}"

    @staticmethod
    def _dump_state(state: Optional[Any]) -> Optional[str]:
        if state is None:
            return None
        if isinstance(state, AiogramState):
            return state.state
        st = getattr(state, "state", None)
        if isinstance(st, str):
            return st
        if isinstance(state, str):
            return state
        return str(state)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def wait_closed(self) -> None:
        return

    async def get_state(self, key: StorageKey) -> Optional[str]:
        conn = await self._connect()
        cur = await conn.execute("SELECT state FROM fsm WHERE k=?", (self._k(key),))
        row = await cur.fetchone()
        if not row:
            return None
        return row[0] if row[0] else None

    async def set_state(self, key: StorageKey, state: Optional[Any] = None) -> None:
        conn = await self._connect()
        k = self._k(key)
        state_str = self._dump_state(state)
        await conn.execute("INSERT OR IGNORE INTO fsm(k, state, data) VALUES(?, NULL, '{}')", (k,))
        await conn.execute("UPDATE fsm SET state=? WHERE k=?", (state_str, k))
        await conn.commit()

    async def get_data(self, key: StorageKey) -> Dict[str, Any]:
        conn = await self._connect()
        cur = await conn.execute("SELECT data FROM fsm WHERE k=?", (self._k(key),))
        row = await cur.fetchone()
        if not row or not row[0]:
            return {}
        try:
            obj = json.loads(row[0])
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    async def set_data(self, key: StorageKey, data: Dict[str, Any]) -> None:
        conn = await self._connect()
        k = self._k(key)
        await conn.execute("INSERT OR IGNORE INTO fsm(k, state, data) VALUES(?, NULL, '{}')", (k,))
        await conn.execute("UPDATE fsm SET data=? WHERE k=?", (json.dumps(data, ensure_ascii=False), k))
        await conn.commit()

    async def update_data(self, key: StorageKey, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        current = await self.get_data(key)
        current.update(data)
        current.update(kwargs)
        await self.set_data(key, current)
        return current

    async def clear(self, key: StorageKey) -> None:
        conn = await self._connect()
        await conn.execute("DELETE FROM fsm WHERE k=?", (self._k(key),))
        await conn.commit()


# -------------------- FSM STATES --------------------

class ShiftFSM(StatesGroup):
    choosing_action = FSMState()
    enter_name = FSMState()
    enter_shift_number = FSMState()
    enter_cash = FSMState()
    enter_stock = FSMState()
    enter_comment = FSMState()
    enter_keyboards_cleaned = FSMState()
    choosing_items = FSMState()


# -------------------- CALLBACK DATA --------------------

class ActionCb(CallbackData, prefix="act"):
    action: str  # open | close


class ItemCb(CallbackData, prefix="it"):
    item_id: str


class SubmitCb(CallbackData, prefix="sub"):
    ok: int


# -------------------- CUSTOM SESSION --------------------

class CustomAiohttpSession(AiohttpSession):
    """
    Нужен из-за различий версий aiogram/aiohttp: dp.start_polling ожидает bot.session.timeout как число.
    """
    def __init__(
        self,
        *,
        connector: Optional[aiohttp.BaseConnector] = None,
        aiohttp_timeout: Optional[ClientTimeout] = None,
        timeout: float = 90.0,
        proxy: Optional[str] = None,
    ):
        super().__init__(timeout=float(timeout), proxy=proxy)
        self._custom_connector = connector
        self._aiohttp_timeout = aiohttp_timeout

    async def create_session(self) -> aiohttp.ClientSession:
        sess = getattr(self, "_session", None)
        if sess is not None and not sess.closed:
            return sess

        at = self._aiohttp_timeout or ClientTimeout(total=float(self.timeout))
        sess = aiohttp.ClientSession(connector=self._custom_connector, timeout=at)
        setattr(self, "_session", sess)
        return sess

    async def _create_session(self) -> aiohttp.ClientSession:
        return await self.create_session()


# -------------------- DOMAIN HELPERS --------------------

def now_dt() -> datetime:
    # всегда Екатеринбург
    return datetime.now(BOT_TZ)

def now_str() -> str:
    return now_dt().strftime("%Y-%m-%d %H:%M:%S %z")

def shift_type_label(shift_type: str) -> str:
    return "Дневная" if shift_type == "day" else "Ночная"

def determine_shift_type_by_open(opened_at: datetime) -> str:
    """
    Дневная: 09:00..21:00
    Ночная: 21:00..09:00
    + окно допуска SHIFT_TOLERANCE_MINUTES вокруг 09:00 и 21:00.
    """
    tol_sec = max(0, SHIFT_TOLERANCE_MINUTES) * 60

    t_sec = opened_at.hour * 3600 + opened_at.minute * 60 + opened_at.second
    day_start_sec = DAY_SHIFT_START.hour * 3600 + DAY_SHIFT_START.minute * 60  # 09:00
    night_start_sec = DAY_SHIFT_END.hour * 3600 + DAY_SHIFT_END.minute * 60   # 21:00
    day_end_sec = night_start_sec

    def in_range(x: int, start: int, end: int) -> bool:
        if start <= end:
            return start <= x < end
        return x >= start or x < end

    night_from = (night_start_sec - tol_sec) % 86400
    night_to = (night_start_sec + tol_sec) % 86400
    day_from = (day_start_sec - tol_sec) % 86400
    day_to = (day_start_sec + tol_sec) % 86400

    if tol_sec and in_range(t_sec, night_from, night_to):
        return "night"
    if tol_sec and in_range(t_sec, day_from, day_to):
        return "day"

    return "day" if (day_start_sec <= t_sec < day_end_sec) else "night"

def init_shift_data() -> Dict[str, Any]:
    return {
        "name": "",
        "shift_number": None,  # int
        "cash": "",
        "comment": "",
        "items": {k: False for k in CHECK_ITEMS.keys()},
        "opened_at": "",
        "shift_type": "",
        "keyboards_cleaned": "",
        **{k: "" for k in STOCK_KEYS},
    }

def is_blank_value(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "-", "—", "нет", "не было")

def action_keyboard(shift_active: bool):
    kb = InlineKeyboardBuilder()
    if shift_active:
        kb.button(text="Закрыть смену", callback_data=ActionCb(action="close").pack())
    else:
        kb.button(text="Начать смену", callback_data=ActionCb(action="open").pack())
    kb.adjust(1)
    return kb.as_markup()

def items_keyboard(shift_data: Dict[str, Any]):
    kb = InlineKeyboardBuilder()
    items = shift_data.get("items", {})
    for item_id, label in CHECK_ITEMS.items():
        mark = "✅" if items.get(item_id) else "❌"
        kb.button(text=f"{label} {mark}", callback_data=ItemCb(item_id=item_id).pack())
    kb.button(text="Отправить отчет", callback_data=SubmitCb(ok=1).pack())
    kb.adjust(1)
    return kb.as_markup()

def render_stock_lines(shift_data: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for key, label in STOCK_LABELS.items():
        v = shift_data.get(key, "")
        if is_blank_value(v):
            continue
        out.append(f"{label}: {str(v).strip()}")
    return out

def format_report(shift_data: Dict[str, Any], user, action: str) -> str:
    action_text = "начал смену" if action == "open" else "закрыл смену"
    username = f"@{user.username}" if user.username else f"id:{user.id}"

    shift_type = shift_data.get("shift_type") or "day"
    opened_at = shift_data.get("opened_at") or "—"

    lines = [
        "<b>Отчет</b>",
        f"Пользователь: {username} ({user.full_name})",
        f"Смена (по открытию): <b>{shift_type_label(shift_type)}</b>",
        f"Открыта: {opened_at}",
        f"Действие: <b>{action_text}</b>",
        f"Время отчёта: {now_str()}",
        "",
        f"ФИО: {shift_data.get('name', '')}",
        f"Номер смены: {shift_data.get('shift_number', '')}",
        f"Касса: {shift_data.get('cash', '')} рублей",
        f"Комментарий: {shift_data.get('comment') or '—'}",
    ]

    if action == "close" and shift_type == "night":
        kc = shift_data.get("keyboards_cleaned", "")
        if not is_blank_value(kc):
            lines.append(f"Почищены клавиатуры: {str(kc).strip()}")

    stock_lines = render_stock_lines(shift_data)
    if stock_lines:
        lines += ["", "<b>Остатки:</b>", *stock_lines]

    lines += ["", "<b>Пункты:</b>"]
    for item_id, label in CHECK_ITEMS.items():
        status = "✅" if shift_data.get("items", {}).get(item_id) else "❌"
        lines.append(f" - {label}: {status}")

    return "\n".join(lines)

def get_next_stock_question(idx: int) -> Optional[Tuple[str, str]]:
    if 0 <= idx < len(STOCK_QUESTIONS):
        return STOCK_QUESTIONS[idx]
    return None

def snapshot_stock(shift_data: Dict[str, Any]) -> Dict[str, str]:
    snap: Dict[str, str] = {}
    for k in STOCK_KEYS:
        v = shift_data.get(k, "")
        snap[k] = "" if is_blank_value(v) else str(v).strip()
    return snap


# -------------------- NETWORK HELPERS --------------------

async def safe_send(bot: Bot, chat_id: int, text: str, *, retries: int = 6) -> None:
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            await bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True)
            return
        except TelegramRetryAfter as e:
            await asyncio.sleep(float(e.retry_after) + 0.5)
        except (TelegramNetworkError, TelegramServerError) as e:
            log.warning("Temporary error send to %s: %s (attempt %s/%s)", chat_id, e, attempt, retries)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 15)
        except TelegramForbiddenError as e:
            log.warning("Forbidden send to %s: %s", chat_id, e)
            return
        except TelegramBadRequest as e:
            log.warning("BadRequest send to %s: %s", chat_id, e)
            return
        except TelegramAPIError as e:
            log.error("TelegramAPIError send to %s: %s", chat_id, e)
            return

async def wait_for_telegram(bot: Bot) -> None:
    delay = 1.0
    while True:
        try:
            await bot.get_me()
            return
        except TelegramRetryAfter as e:
            await asyncio.sleep(float(e.retry_after) + 0.5)
        except (TelegramNetworkError, TelegramServerError, TelegramAPIError) as e:
            log.warning("Telegram недоступен (%s). Жду %.1fs и пробую снова...", repr(e), delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 20)


# -------------------- STATE HELPERS --------------------

async def get_shift_active(state: FSMContext) -> bool:
    data = await state.get_data()
    return bool(data.get("shift_active", False))

async def set_shift_active(state: FSMContext, value: bool) -> None:
    await state.update_data(shift_active=bool(value))

async def require_shift_data(state: FSMContext) -> Dict[str, Any]:
    data = await state.get_data()
    if not isinstance(data.get("shift_data"), dict):
        await state.update_data(shift_data=init_shift_data())
        data = await state.get_data()
    return data["shift_data"]

async def get_active_shift(state: FSMContext) -> Optional[dict]:
    data = await state.get_data()
    v = data.get("active_shift")
    return v if isinstance(v, dict) else None


# -------------------- HANDLERS --------------------

async def cmd_start(message: Message, state: FSMContext):
    shift_active = await get_shift_active(state)
    await message.answer("Привет! Нажмите кнопку для действия.", reply_markup=action_keyboard(shift_active))
    await state.set_state(ShiftFSM.choosing_action)

async def cmd_status(message: Message, state: FSMContext):
    shift_active = await get_shift_active(state)
    active_shift = await get_active_shift(state)

    extra = ""
    if shift_active and active_shift:
        extra = (
            f"\nСмена: {shift_type_label(active_shift.get('shift_type', 'day'))}"
            f"\nОткрыта: {active_shift.get('opened_at', '—')}"
            f"\nФИО: {active_shift.get('name', '—')}"
            f"\nНомер смены: {active_shift.get('shift_number', '—')}"
            f"\nКасса: {active_shift.get('cash', '—')}"
        )
    await message.answer(f"Статус смены: {'ОТКРЫТА' if shift_active else 'ЗАКРЫТА'}{extra}")

async def cmd_cancel(message: Message, state: FSMContext):
    data = await state.get_data()
    shift_active = bool(data.get("shift_active", False))
    active_shift = data.get("active_shift")

    await state.set_state(None)
    await state.update_data(
        shift_active=shift_active,
        active_shift=active_shift,
        current_action=None,
        shift_data=None,
        stock_idx=None,
    )
    await message.answer("Процесс отменён. Нажмите /start чтобы начать заново.", reply_markup=action_keyboard(shift_active))

async def on_action(call: CallbackQuery, callback_data: ActionCb, state: FSMContext):
    await call.answer()

    shift_active = await get_shift_active(state)
    action = callback_data.action

    if action == "open" and shift_active:
        await call.message.answer("Смена уже открыта.", reply_markup=action_keyboard(True))
        await state.set_state(ShiftFSM.choosing_action)
        return

    if action == "close" and not shift_active:
        await call.message.answer("Смена не открыта.", reply_markup=action_keyboard(False))
        await state.set_state(ShiftFSM.choosing_action)
        return

    if action == "open":
        shift_data = init_shift_data()
        opened = now_dt()
        shift_data["opened_at"] = opened.strftime("%Y-%m-%d %H:%M:%S %z")
        shift_data["shift_type"] = determine_shift_type_by_open(opened)

        await state.update_data(current_action="open", shift_data=shift_data, stock_idx=0)
        await call.message.answer("Введите ваше ФИО:")
        await state.set_state(ShiftFSM.enter_name)
        return

    # close: берём данные из active_shift
    active_shift = await get_active_shift(state)
    if not active_shift:
        await call.message.answer("Не удалось найти данные открытия смены. Нажмите /start и откройте смену заново.")
        await set_shift_active(state, False)
        await state.update_data(active_shift=None)
        await state.set_state(ShiftFSM.choosing_action)
        return

    shift_data = init_shift_data()
    shift_data["opened_at"] = str(active_shift.get("opened_at", "") or "")
    shift_data["shift_type"] = str(active_shift.get("shift_type", "") or "")
    shift_data["name"] = str(active_shift.get("name", "") or "")
    shift_data["shift_number"] = active_shift.get("shift_number", None)
    shift_data["cash"] = str(active_shift.get("cash", "") or "")

    stock_snap = active_shift.get("stock")
    if isinstance(stock_snap, dict):
        for k in STOCK_KEYS:
            v = stock_snap.get(k, "")
            shift_data[k] = "" if is_blank_value(v) else str(v).strip()

    await state.update_data(current_action="close", shift_data=shift_data, stock_idx=None)
    await call.message.answer("Введите комментарий к смене:")
    await state.set_state(ShiftFSM.enter_comment)

async def enter_name(message: Message, state: FSMContext):
    shift_data = await require_shift_data(state)
    shift_data["name"] = (message.text or "").strip()
    await state.update_data(shift_data=shift_data)

    await message.answer("Введите номер смены (числом):")
    await state.set_state(ShiftFSM.enter_shift_number)

async def enter_shift_number(message: Message, state: FSMContext):
    shift_data = await require_shift_data(state)
    text = (message.text or "").strip()
    try:
        shift_data["shift_number"] = int(text)
    except ValueError:
        await message.answer("Номер смены должен быть числом. Введите ещё раз:")
        return

    await state.update_data(shift_data=shift_data)
    await message.answer("Введите сумму кассы (в рублях):")
    await state.set_state(ShiftFSM.enter_cash)

async def enter_cash(message: Message, state: FSMContext):
    shift_data = await require_shift_data(state)
    shift_data["cash"] = (message.text or "").strip()
    await state.update_data(shift_data=shift_data)

    shift_number = int(shift_data.get("shift_number") or 0)
    if shift_number >= 3:
        await state.update_data(stock_idx=0)
        _key, q = STOCK_QUESTIONS[0]
        await message.answer(q)
        await state.set_state(ShiftFSM.enter_stock)
    else:
        await message.answer("Введите комментарий к смене:")
        await state.set_state(ShiftFSM.enter_comment)

async def enter_stock(message: Message, state: FSMContext):
    shift_data = await require_shift_data(state)
    data = await state.get_data()
    idx = int(data.get("stock_idx", 0))

    qa = get_next_stock_question(idx)
    if not qa:
        await message.answer("Введите комментарий к смене:")
        await state.set_state(ShiftFSM.enter_comment)
        return

    key, _q = qa
    answer = (message.text or "").strip()
    shift_data[key] = "" if is_blank_value(answer) else answer

    idx += 1
    await state.update_data(shift_data=shift_data, stock_idx=idx)

    next_qa = get_next_stock_question(idx)
    if not next_qa:
        await message.answer("Введите комментарий к смене:")
        await state.set_state(ShiftFSM.enter_comment)
        return

    _next_key, next_q = next_qa
    await message.answer(next_q)
    await state.set_state(ShiftFSM.enter_stock)

async def enter_comment(message: Message, state: FSMContext):
    data = await state.get_data()
    action = data.get("current_action")

    shift_data = await require_shift_data(state)
    shift_data["comment"] = (message.text or "").strip()
    await state.update_data(shift_data=shift_data)

    if action == "close" and shift_data.get("shift_type") == "night":
        await message.answer(
            "Почищены клавиатуры: введите номера (через запятую/пробел). "
            "Если не чистили — напишите '-'"
        )
        await state.set_state(ShiftFSM.enter_keyboards_cleaned)
        return

    await message.answer("Выберите пункты:", reply_markup=items_keyboard(shift_data))
    await state.set_state(ShiftFSM.choosing_items)

async def enter_keyboards_cleaned(message: Message, state: FSMContext):
    shift_data = await require_shift_data(state)
    text = (message.text or "").strip()
    shift_data["keyboards_cleaned"] = "" if is_blank_value(text) else text
    await state.update_data(shift_data=shift_data)

    await message.answer("Выберите пункты:", reply_markup=items_keyboard(shift_data))
    await state.set_state(ShiftFSM.choosing_items)

async def on_toggle_item(call: CallbackQuery, callback_data: ItemCb, state: FSMContext):
    await call.answer()
    shift_data = await require_shift_data(state)

    item_id = callback_data.item_id
    if item_id in shift_data["items"]:
        shift_data["items"][item_id] = not shift_data["items"][item_id]
        await state.update_data(shift_data=shift_data)

    try:
        if call.message:
            await call.message.edit_reply_markup(reply_markup=items_keyboard(shift_data))
    except TelegramBadRequest as e:
        log.info("edit_reply_markup skipped: %s", e)

async def on_submit(call: CallbackQuery, callback_data: SubmitCb, state: FSMContext, bot: Bot):
    await call.answer()
    data = await state.get_data()
    shift_data = data.get("shift_data")
    action = data.get("current_action")

    if not isinstance(shift_data, dict) or action not in ("open", "close"):
        shift_active = await get_shift_active(state)
        if call.message:
            await call.message.answer("Сессия заполнения отчёта потеряна. Нажмите /start.", reply_markup=action_keyboard(shift_active))
        await state.set_state(ShiftFSM.choosing_action)
        return

    report = format_report(shift_data, call.from_user, action)
    await asyncio.gather(*[safe_send(bot, cid, report) for cid in RECIPIENTS])

    if call.message:
        await call.message.answer("Отчет отправлен.")

    if action == "open":
        await set_shift_active(state, True)
        await state.update_data(
            active_shift={
                "opened_at": shift_data.get("opened_at"),
                "shift_type": shift_data.get("shift_type"),
                "name": shift_data.get("name"),
                "shift_number": shift_data.get("shift_number"),
                "cash": shift_data.get("cash"),
                "stock": snapshot_stock(shift_data),
            }
        )
    else:
        await set_shift_active(state, False)
        await state.update_data(active_shift=None)

    shift_active = await get_shift_active(state)
    await state.set_state(ShiftFSM.choosing_action)
    await state.update_data(current_action=None, shift_data=None, stock_idx=None)

    if call.message:
        await call.message.answer("Нажмите кнопку для действия.", reply_markup=action_keyboard(shift_active))

async def fallback_any_message(message: Message, state: FSMContext):
    shift_active = await get_shift_active(state)
    await message.answer("Нажмите /start чтобы начать.", reply_markup=action_keyboard(shift_active))


# -------------------- ERROR HANDLER --------------------

async def error_handler(event: ErrorEvent) -> bool:
    exc = event.exception
    log.error("Unhandled error while processing update: %r", exc, exc_info=(type(exc), exc, exc.__traceback__))
    return True


# -------------------- SESSION FACTORY --------------------

def build_session() -> AiohttpSession:
    proxy_url = os.getenv("PROXY_URL")
    force_ipv4 = os.getenv("FORCE_IPV4", "1") == "1"

    timeout_seconds = float(os.getenv("BOT_TIMEOUT", "90"))
    connect_timeout = float(os.getenv("BOT_CONNECT_TIMEOUT", "30"))
    sock_read_timeout = float(os.getenv("BOT_SOCK_READ_TIMEOUT", "60"))

    aiohttp_timeout = ClientTimeout(
        total=timeout_seconds,
        connect=min(connect_timeout, timeout_seconds),
        sock_read=min(sock_read_timeout, timeout_seconds),
    )

    if proxy_url and (proxy_url.startswith("socks5://") or proxy_url.startswith("socks4://")):
        if ProxyConnector is None:
            raise RuntimeError("Для SOCKS-прокси установите: pip install aiohttp-socks")
        connector = ProxyConnector.from_url(proxy_url)
        log.info("Using SOCKS proxy: %s", proxy_url)
        return CustomAiohttpSession(connector=connector, aiohttp_timeout=aiohttp_timeout, timeout=timeout_seconds)

    if proxy_url:
        log.info("Using HTTP(S) proxy: %s", proxy_url)
        return AiohttpSession(proxy=proxy_url, timeout=timeout_seconds)

    connector = TCPConnector(
        family=socket.AF_INET if force_ipv4 else 0,
        ttl_dns_cache=300,
    )
    return CustomAiohttpSession(connector=connector, aiohttp_timeout=aiohttp_timeout, timeout=timeout_seconds)


# -------------------- MAIN --------------------

async def main():
    # ВАЖНО: не храните токен в коде, лучше задайте переменную окружения BOT_TOKEN.
    token = "8153684081:AAGUbfR8Kqa1Oty1nUurQ8iDQlHMsxnwyf8"
    if not token:
        raise RuntimeError("BOT_TOKEN не задан в переменных окружения.")

    storage = SQLiteFSMStorage(os.getenv("FSM_DB_PATH", "fsm.sqlite3"))
    session = build_session()

    bot = Bot(token=token, session=session, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher(storage=storage)
    dp.errors.register(error_handler)

    dp.message.register(cmd_start, CommandStart())
    dp.message.register(cmd_status, Command("status"))
    dp.message.register(cmd_cancel, Command("cancel"))

    dp.callback_query.register(on_action, ActionCb.filter())
    dp.message.register(enter_name, ShiftFSM.enter_name, F.text)
    dp.message.register(enter_shift_number, ShiftFSM.enter_shift_number, F.text)
    dp.message.register(enter_cash, ShiftFSM.enter_cash, F.text)
    dp.message.register(enter_stock, ShiftFSM.enter_stock, F.text)
    dp.message.register(enter_comment, ShiftFSM.enter_comment, F.text)
    dp.message.register(enter_keyboards_cleaned, ShiftFSM.enter_keyboards_cleaned, F.text)

    dp.callback_query.register(on_toggle_item, ItemCb.filter())
    dp.callback_query.register(on_submit, SubmitCb.filter())

    dp.message.register(fallback_any_message, F.text)

    await wait_for_telegram(bot)
    await bot.delete_webhook(drop_pending_updates=True)

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await storage.close()


if __name__ == "__main__":
    asyncio.run(main())
