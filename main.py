#!/usr/bin/env python3
"""
Telegram рассыльщик с авторизацией по номеру телефона.
Управление через Telegram-бота.
Python 3.9+
"""

import asyncio
import contextlib
import json
import logging
import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from telethon import Button, TelegramClient, events
from telethon.errors import (
    ChatAdminRequiredError,
    ChannelInvalidError,
    ChannelPrivateError,
    FloodWaitError,
    InputUserDeactivatedError,
    MessageNotModifiedError,
    PeerIdInvalidError,
    RPCError,
    UserBannedInChannelError,
    UserIsBlockedError,
)
from telethon.network.connection.tcpfull import ConnectionTcpFull
from telethon.network.connection.tcpmtproxy import (
    ConnectionTcpMTProxyRandomizedIntermediate,
)
from telethon.tl.functions.messages import DeleteHistoryRequest
from telethon.tl.types import Channel, Chat

# =========================
# Конфигурация
# =========================
API_ID = 20784926
API_HASH = "2884cd0ca1ab0bbdef307767e2e2f1d0"
SESSION_NAME = "user_session"
CONTROL_BOT_TOKEN = "8292152730:AAEJOCpGqXG6U6xxV6qVyIMER0FbgYZiLLo"

ADMIN_USER_IDS = {8661926277}

EXCLUDED_CHAT_IDS: Set[int] = set()
SEND_TO_ALL_BY_DEFAULT = True
ALLOWED_CHAT_IDS: Set[int] = set()

STATE_FILE = Path("state.json")
LOG_LEVEL = "WARNING"

DEFAULT_MESSAGE = "Привет! Это автоматическое сообщение по расписанию."
DEFAULT_INTERVAL_SECONDS = 180

# Анти-бан/анти-флуд задержки
SEND_DELAY_RANGE = (45, 90)  # сек между успешными отправками
CYCLE_DELAY_JITTER = (0.85, 1.25)  # множитель для паузы между циклами
FLOOD_RETRY_BASE = 3
FLOOD_RETRY_CAP = 5

PROXY_ENABLED = True
PROXY_TYPE = "http"  # http, socks5, mtproto
PROXY_HOST = "194.147.115.50"
PROXY_PORT = 3128
PROXY_USER = ""
PROXY_PASS = ""
MT_PROXY_SECRET = ""

if PROXY_ENABLED and PROXY_TYPE == "mtproto":
    CONNECTION = ConnectionTcpMTProxyRandomizedIntermediate
else:
    CONNECTION = ConnectionTcpFull

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("telegram_scheduler")
logging.getLogger("telethon.client.updates").setLevel(logging.WARNING)


# =========================
# Модель состояния
# =========================
@dataclass
class Stats:
    sent_ok: int = 0
    send_errors: int = 0
    flood_hits: int = 0
    cycles_total: int = 0

    def to_json(self) -> Dict[str, int]:
        return {
            "sent_ok": self.sent_ok,
            "send_errors": self.send_errors,
            "flood_hits": self.flood_hits,
            "cycles_total": self.cycles_total,
        }

    @classmethod
    def from_json(cls, payload: Dict[str, Any]) -> "Stats":
        return cls(
            sent_ok=int(payload.get("sent_ok", 0)),
            send_errors=int(payload.get("send_errors", 0)),
            flood_hits=int(payload.get("flood_hits", 0)),
            cycles_total=int(payload.get("cycles_total", 0)),
        )


@dataclass
class AppState:
    message_text: str = DEFAULT_MESSAGE
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS
    sending_enabled: bool = False
    last_cycle_started_at: Optional[float] = None
    last_errors: Dict[str, str] = field(default_factory=dict)
    manual_chat_ids: Set[int] = field(default_factory=set)
    banned_chats: Set[int] = field(default_factory=set)
    stats: Stats = field(default_factory=Stats)

    def to_json(self) -> Dict[str, object]:
        return {
            "message_text": self.message_text,
            "interval_seconds": self.interval_seconds,
            "sending_enabled": self.sending_enabled,
            "last_cycle_started_at": self.last_cycle_started_at,
            "last_errors": self.last_errors,
            "manual_chat_ids": sorted(self.manual_chat_ids),
            "banned_chats": sorted(self.banned_chats),
            "stats": self.stats.to_json(),
        }

    @classmethod
    def from_json(cls, payload: Dict[str, object]) -> "AppState":
        return cls(
            message_text=str(payload.get("message_text") or DEFAULT_MESSAGE),
            interval_seconds=max(
                10,
                int(payload.get("interval_seconds") or DEFAULT_INTERVAL_SECONDS),
            ),
            sending_enabled=bool(payload.get("sending_enabled", False)),
            last_cycle_started_at=payload.get("last_cycle_started_at"),
            last_errors={
                str(k): str(v)
                for k, v in dict(payload.get("last_errors") or {}).items()
            },
            manual_chat_ids={int(x) for x in payload.get("manual_chat_ids", [])},
            banned_chats={int(x) for x in payload.get("banned_chats", [])},
            stats=Stats.from_json(dict(payload.get("stats") or {})),
        )


class StateStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = asyncio.Lock()
        self.state: AppState = AppState()

    async def load(self) -> AppState:
        async with self._lock:
            if not self.path.exists():
                self.state = AppState()
                self.state.store = self
                return self.state
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                self.state = AppState.from_json(data)
                self.state.store = self
                return self.state
            except Exception as exc:
                logger.exception("Ошибка чтения state.json: %s", exc)
                self.state = AppState()
                self.state.store = self
                return self.state

    async def save(self, state: AppState) -> None:
        async with self._lock:
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(state.to_json(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp.replace(self.path)


# =========================
# Вспомогательные функции
# =========================
def is_admin(sender_id: Optional[int]) -> bool:
    return sender_id is not None and sender_id in ADMIN_USER_IDS


async def safe_reply(event, text: str, buttons=None, parse_mode: Optional[str] = "html") -> None:
    try:
        await event.reply(text, buttons=buttons, parse_mode=parse_mode)
    except FloodWaitError as exc:
        await asyncio.sleep(exc.seconds + 1)
        await event.reply(text, buttons=buttons, parse_mode=parse_mode)
    except Exception as exc:
        logger.warning("safe_reply error: %s", exc)


async def safe_edit(event, text: str, buttons=None, parse_mode: Optional[str] = "html") -> None:
    try:
        await event.edit(text, buttons=buttons, parse_mode=parse_mode)
    except MessageNotModifiedError:
        return
    except FloodWaitError as exc:
        await asyncio.sleep(exc.seconds + 1)
        await event.edit(text, buttons=buttons, parse_mode=parse_mode)
    except Exception as exc:
        logger.warning("safe_edit error: %s", exc)


async def leave_chat(client: TelegramClient, chat_id: int, state: AppState) -> None:
    if chat_id in state.banned_chats:
        return
    try:
        entity = await client.get_entity(chat_id)
        await client(DeleteHistoryRequest(peer=entity, max_id=0, just_clear=False))
        state.banned_chats.add(chat_id)
        await state.store.save(state)
        logger.info("Покинул чат %s", chat_id)
    except Exception as exc:
        logger.warning("Не удалось покинуть чат %s: %s", chat_id, exc)


async def get_all_dialogs(client: TelegramClient, state: AppState) -> List[Tuple[int, Any]]:
    dialogs = await client.get_dialogs(limit=None)
    result: List[Tuple[int, Any]] = []
    for dialog in dialogs:
        entity = dialog.entity
        chat_id = getattr(entity, "id", None)
        if not chat_id or chat_id in EXCLUDED_CHAT_IDS or chat_id in state.banned_chats:
            continue

        if isinstance(entity, Chat):
            result.append((chat_id, entity))
            continue

        if isinstance(entity, Channel) and getattr(entity, "megagroup", False):
            result.append((chat_id, entity))
            continue

    return result


async def get_target_chat_ids(client: TelegramClient, state: AppState) -> List[int]:
    target: Set[int] = set()
    if SEND_TO_ALL_BY_DEFAULT:
        dialogs = await get_all_dialogs(client, state)
        target.update([chat_id for chat_id, _ in dialogs])
    else:
        target.update(ALLOWED_CHAT_IDS)

    target.update(state.manual_chat_ids)
    target -= state.banned_chats
    return sorted(target)


async def send_message_to_chat(
    client: TelegramClient,
    chat_id: int,
    text: str,
    state: AppState,
) -> Tuple[Optional[str], bool]:
    """
    Возвращает (ошибка, нужно_ли_ждать_обычную_задержку)
    """
    for attempt in range(1, FLOOD_RETRY_CAP + 1):
        try:
            await client.send_message(chat_id, text)
            return None, True
        except FloodWaitError as exc:
            state.stats.flood_hits += 1
            random_jitter = random.uniform(0.85, 1.20)
            backoff = min(
                exc.seconds,
                int((FLOOD_RETRY_BASE * (2 ** (attempt - 1))) * random_jitter),
            )
            backoff = max(1, backoff)
            logger.warning(
                "FloodWait в чате %s: %s сек, попытка %s/%s, пауза %s сек",
                chat_id,
                exc.seconds,
                attempt,
                FLOOD_RETRY_CAP,
                backoff,
            )
            await state.store.save(state)
            await asyncio.sleep(backoff)
            continue
        except (
            ChatAdminRequiredError,
            UserBannedInChannelError,
            UserIsBlockedError,
            InputUserDeactivatedError,
        ):
            await leave_chat(client, chat_id, state)
            state.stats.send_errors += 1
            return "Нет прав, чат покинут", False
        except (ChannelInvalidError, ChannelPrivateError, PeerIdInvalidError):
            await leave_chat(client, chat_id, state)
            state.stats.send_errors += 1
            return "Чат недоступен", False
        except RPCError as exc:
            state.stats.send_errors += 1
            if "you can't write" in str(exc).lower() or "banned" in str(exc).lower():
                await leave_chat(client, chat_id, state)
                return "Нет прав", False
            return f"RPC ошибка: {exc}", False
        except Exception as exc:
            state.stats.send_errors += 1
            return f"Ошибка: {exc}", False

    state.stats.send_errors += 1
    return f"FloodWait не устранён после {FLOOD_RETRY_CAP} попыток", False


def format_status(state: AppState) -> str:
    mode = "все чаты" if SEND_TO_ALL_BY_DEFAULT else f"только разрешённые: {sorted(ALLOWED_CHAT_IDS)}"
    banned = f"{len(state.banned_chats)}"
    recent_errors = "\n".join(
        [f"• {k}: {v}" for k, v in list(state.last_errors.items())[-10:]]
    ) or "нет"

    return (
        "<b>Текущее состояние</b>\n"
        f"Рассылка: <b>{'включена' if state.sending_enabled else 'выключена'}</b>\n"
        f"Интервал циклов: <b>{state.interval_seconds}</b> сек\n"
        f"Пауза между отправками: <b>{SEND_DELAY_RANGE[0]}-{SEND_DELAY_RANGE[1]}</b> сек\n"
        f"Режим: <b>{mode}</b>\n"
        f"Забаненные чаты: <b>{banned}</b>\n"
        f"Текст: <code>{state.message_text}</code>\n\n"
        "<b>Статистика</b>\n"
        f"✅ Успешно отправлено: <b>{state.stats.sent_ok}</b>\n"
        f"❌ Ошибок отправки: <b>{state.stats.send_errors}</b>\n"
        f"⏳ FloodWait событий: <b>{state.stats.flood_hits}</b>\n"
        f"🔁 Циклов рассылки: <b>{state.stats.cycles_total}</b>\n\n"
        f"<b>Последние ошибки</b>\n{recent_errors}"
    )


def main_menu_buttons() -> List[List[Button]]:
    return [
        [Button.inline("▶️ Старт", b"start_send"), Button.inline("⏹ Стоп", b"stop_send")],
        [Button.inline("📄 Статус", b"show_status")],
    ]


async def scheduler_loop(client: TelegramClient, store: StateStore) -> None:
    logger.info("Планировщик запущен")
    while True:
        state = await store.load()
        if not state.sending_enabled:
            await asyncio.sleep(2)
            continue

        state.last_cycle_started_at = time.time()
        state.stats.cycles_total += 1
        target_ids = await get_target_chat_ids(client, state)
        if not target_ids:
            logger.warning("Нет доступных чатов")
            state.sending_enabled = False
            state.last_errors["general"] = "Нет доступных чатов для рассылки"
            await store.save(state)
            await asyncio.sleep(2)
            continue

        logger.info("Рассылка по %s чатам", len(target_ids))
        for chat_id in target_ids:
            error, need_delay = await send_message_to_chat(
                client,
                chat_id,
                state.message_text,
                state,
            )
            state = await store.load()
            if error:
                state.last_errors[str(chat_id)] = error
                logger.warning("Ошибка в %s: %s", chat_id, error)
            else:
                state.last_errors.pop(str(chat_id), None)
                state.stats.sent_ok += 1
                logger.info("Отправлено в %s", chat_id)
            await store.save(state)

            if need_delay:
                await asyncio.sleep(random.randint(*SEND_DELAY_RANGE))

        state = await store.load()
        await store.save(state)
        cycle_pause = max(10, int(state.interval_seconds * random.uniform(*CYCLE_DELAY_JITTER)))
        await asyncio.sleep(cycle_pause)


async def handle_admin_message(event, client: TelegramClient, store: StateStore) -> None:
    if not is_admin(event.sender_id):
        await safe_reply(event, "Доступ запрещён.")
        return

    text = (event.raw_text or "").strip()
    state = await store.load()

    if text == "/start":
        await safe_reply(
            event,
            "Управление рассылкой.\n"
            "/status — состояние и статистика\n"
            "/run — включить\n"
            "/stop — выключить\n"
            "/text <текст> — изменить текст\n"
            "/setinterval <сек> — интервал цикла\n"
            "/addchat <id> — вручную добавить чат\n"
            "/listchats — список первых 100 чатов\n"
            "/delete_session — удалить сессии и выйти",
            buttons=main_menu_buttons(),
        )
        return

    if text == "/status":
        await safe_reply(event, format_status(state), buttons=main_menu_buttons())
        return

    if text == "/run":
        state.sending_enabled = True
        await store.save(state)
        await safe_reply(event, "Рассылка включена.")
        return

    if text == "/stop":
        state.sending_enabled = False
        await store.save(state)
        await safe_reply(event, "Рассылка остановлена.")
        return

    if text.startswith("/text "):
        new_text = text[len("/text ") :].strip()
        if new_text:
            state.message_text = new_text
            await store.save(state)
            await safe_reply(event, "Текст обновлён.")
        return

    if text.startswith("/setinterval "):
        try:
            seconds = int(text[len("/setinterval ") :].strip())
            if seconds >= 10:
                state.interval_seconds = seconds
                await store.save(state)
                await safe_reply(event, f"Интервал: {seconds} сек.")
            else:
                await safe_reply(event, "Интервал должен быть >= 10 сек.")
        except Exception:
            await safe_reply(event, "Некорректный интервал.")
        return

    if text.startswith("/addchat "):
        try:
            chat_id = int(text[len("/addchat ") :].strip())
            await client.get_entity(chat_id)
            state.manual_chat_ids.add(chat_id)
            await store.save(state)
            await safe_reply(event, f"Чат {chat_id} добавлен.")
        except Exception as exc:
            await safe_reply(event, f"Ошибка: {exc}")
        return

    if text == "/listchats":
        try:
            dialogs = await client.get_dialogs(limit=100)
            lines = []
            for d in dialogs:
                entity = d.entity
                chat_id = getattr(entity, "id", None)
                name = getattr(d, "name", "")
                if chat_id:
                    lines.append(f"{chat_id} ({name})")
            await safe_reply(event, "Диалоги (первые 100):\n" + "\n".join(lines))
        except Exception as exc:
            await safe_reply(event, f"Ошибка получения списка чатов: {exc}")
        return

    if text == "/delete_session":
        for pattern in (f"{SESSION_NAME}*", "control_bot_session*"):
            for file_path in Path(".").glob(pattern):
                with contextlib.suppress(Exception):
                    file_path.unlink()
        await safe_reply(event, "Сессия удалена. Перезапустите скрипт.")
        sys.exit(0)


async def handle_callback(event, store: StateStore) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return

    data = event.data.decode("utf-8")
    state = await store.load()

    if data == "start_send":
        state.sending_enabled = True
        await store.save(state)
        await safe_edit(event, "Рассылка включена.", buttons=main_menu_buttons())
    elif data == "stop_send":
        state.sending_enabled = False
        await store.save(state)
        await safe_edit(event, "Рассылка остановлена.", buttons=main_menu_buttons())
    elif data == "show_status":
        await safe_edit(
            event,
            format_status(state),
            buttons=main_menu_buttons(),
            parse_mode="html",
        )


def get_proxy() -> Optional[Dict[str, Any]]:
    if not PROXY_ENABLED:
        return None

    if PROXY_TYPE in {"socks5", "http"}:
        proxy: Dict[str, Any] = {
            "proxy_type": PROXY_TYPE,
            "addr": PROXY_HOST,
            "port": PROXY_PORT,
        }
        if PROXY_USER:
            proxy["username"] = PROXY_USER
            proxy["password"] = PROXY_PASS
        return proxy

    if PROXY_TYPE == "mtproto":
        secret = bytes.fromhex(MT_PROXY_SECRET) if MT_PROXY_SECRET else None
        return {
            "proxy_type": "mtproto",
            "addr": PROXY_HOST,
            "port": PROXY_PORT,
            "secret": secret,
        }

    raise ValueError(f"Неизвестный тип прокси: {PROXY_TYPE}")


async def main() -> None:
    proxy_config = get_proxy()

    client = TelegramClient(
        SESSION_NAME,
        API_ID,
        API_HASH,
        connection=CONNECTION,
        proxy=proxy_config,
    )
    await client.start()
    me = await client.get_me()
    logger.info("Авторизован user: %s (@%s)", me.first_name or me.username, me.username)

    control_bot = TelegramClient(
        "control_bot_session",
        API_ID,
        API_HASH,
        connection=CONNECTION,
        proxy=proxy_config,
    )
    await control_bot.start(bot_token=CONTROL_BOT_TOKEN)
    bot_me = await control_bot.get_me()
    logger.info("Бот @%s запущен", bot_me.username)

    store = StateStore(STATE_FILE)
    await store.load()
    await store.save(store.state)

    @control_bot.on(events.NewMessage)
    async def on_message(event):
        await handle_admin_message(event, client, store)

    @control_bot.on(events.CallbackQuery)
    async def on_callback(event):
        await handle_callback(event, store)

    scheduler_task = asyncio.create_task(scheduler_loop(client, store))

    try:
        await control_bot.run_until_disconnected()
    finally:
        scheduler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await scheduler_task
        await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Остановлено")
    except Exception as exc:
        logger.exception("Ошибка: %s", exc)
        sys.exit(1)
