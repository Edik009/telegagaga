#!/usr/bin/env python3
"""
Безопасная альтернатива личному user-account автоматизатору:
- Telethon используется в режиме бота (BotFather token), а не для управления user-аккаунтом.
- Рассылка выполняется только в явно разрешённые чаты (из конфига) или в чаты,
  которые были явно зарегистрированы через команду бота администратором.
- Намеренно НЕ поддерживаются:
  * вход в личный Telegram-аккаунт по номеру телефона/коду;
  * обход "всех активных чатов" user-аккаунта;
  * управление пользовательскими сессиями Telethon для массовых рассылок.

Это помогает избежать сценариев спама и проблем с безопасностью/правилами Telegram.

Python: 3.9+
Зависимость: telethon
"""

import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

from telethon import Button, TelegramClient, events
from telethon.errors import (
    ChatAdminRequiredError,
    ChannelInvalidError,
    ChannelPrivateError,
    FloodWaitError,
    InputUserDeactivatedError,
    PeerIdInvalidError,
    RPCError,
    SessionPasswordNeededError,
    UserBannedInChannelError,
    UserIsBlockedError,
)


# =========================
# Конфигурация
# =========================
API_ID = int(os.getenv("TG_API_ID", "123456"))
API_HASH = os.getenv("TG_API_HASH", "PUT_API_HASH_HERE")
BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "PUT_BOT_TOKEN_HERE")

# ID Telegram-пользователя, которому разрешено управлять ботом.
# Узнать свой ID можно у @userinfobot или аналогичных ботов.
ADMIN_USER_IDS: Set[int] = {
    int(value)
    for value in os.getenv("TG_ADMIN_USER_IDS", "").split(",")
    if value.strip()
}

# Если включено, рассылка идёт только в эти чаты.
# Формат: TG_ALLOWED_CHAT_IDS="-100123,-100456,777000"
STRICT_ALLOWLIST_MODE = os.getenv("TG_STRICT_ALLOWLIST_MODE", "true").lower() == "true"
ALLOWED_CHAT_IDS: Set[int] = {
    int(value)
    for value in os.getenv("TG_ALLOWED_CHAT_IDS", "").split(",")
    if value.strip()
}

# Файлы локального хранения.
SESSION_NAME = os.getenv("TG_SESSION_NAME", "bot_session")
STATE_FILE = Path(os.getenv("TG_STATE_FILE", "state.json"))
LOG_LEVEL = os.getenv("TG_LOG_LEVEL", "INFO").upper()

DEFAULT_MESSAGE = "Привет! Это автоматическое сообщение по расписанию."
DEFAULT_INTERVAL_SECONDS = 180


# =========================
# Логирование
# =========================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("telegram_scheduler_bot")


# =========================
# Модель состояния
# =========================
@dataclass
class AppState:
    message_text: str = DEFAULT_MESSAGE
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS
    sending_enabled: bool = False
    last_cycle_started_at: Optional[float] = None
    registered_chat_ids: Set[int] = field(default_factory=set)
    last_errors: Dict[str, str] = field(default_factory=dict)

    def to_json(self) -> Dict[str, object]:
        return {
            "message_text": self.message_text,
            "interval_seconds": self.interval_seconds,
            "sending_enabled": self.sending_enabled,
            "last_cycle_started_at": self.last_cycle_started_at,
            "registered_chat_ids": sorted(self.registered_chat_ids),
            "last_errors": self.last_errors,
        }

    @classmethod
    def from_json(cls, payload: Dict[str, object]) -> "AppState":
        return cls(
            message_text=str(payload.get("message_text") or DEFAULT_MESSAGE),
            interval_seconds=max(10, int(payload.get("interval_seconds") or DEFAULT_INTERVAL_SECONDS)),
            sending_enabled=bool(payload.get("sending_enabled", False)),
            last_cycle_started_at=payload.get("last_cycle_started_at"),
            registered_chat_ids={int(x) for x in payload.get("registered_chat_ids", [])},
            last_errors={str(k): str(v) for k, v in dict(payload.get("last_errors") or {}).items()},
        )


class StateStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = asyncio.Lock()

    async def load(self) -> AppState:
        async with self._lock:
            if not self.path.exists():
                return AppState()
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                return AppState.from_json(data)
            except Exception as exc:
                logger.exception("Не удалось прочитать %s: %s", self.path, exc)
                broken_path = self.path.with_suffix(".broken.json")
                try:
                    self.path.replace(broken_path)
                except OSError:
                    logger.warning("Не удалось переименовать повреждённый файл состояния")
                return AppState()

    async def save(self, state: AppState) -> None:
        async with self._lock:
            tmp_path = self.path.with_suffix(".tmp")
            tmp_path.write_text(
                json.dumps(state.to_json(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp_path.replace(self.path)


# =========================
# Вспомогательные функции
# =========================
def ensure_env() -> None:
    placeholders = {"PUT_API_HASH_HERE", "PUT_BOT_TOKEN_HERE"}
    if API_ID == 123456 or API_HASH in placeholders or BOT_TOKEN in placeholders:
        raise RuntimeError(
            "Заполните переменные окружения TG_API_ID, TG_API_HASH и TG_BOT_TOKEN."
        )
    if not ADMIN_USER_IDS:
        raise RuntimeError("Укажите хотя бы один TG_ADMIN_USER_IDS для управления ботом.")


def is_admin(sender_id: Optional[int]) -> bool:
    return sender_id is not None and sender_id in ADMIN_USER_IDS


async def safe_reply(event, text: str, buttons=None, parse_mode: Optional[str] = "html") -> None:
    try:
        await event.reply(text, buttons=buttons, parse_mode=parse_mode)
    except FloodWaitError as exc:
        logger.warning("FloodWait при ответе пользователю: %s сек.", exc.seconds)
        await asyncio.sleep(exc.seconds + 1)
        await event.reply(text, buttons=buttons, parse_mode=parse_mode)
    except Exception as exc:
        logger.exception("Не удалось отправить ответ: %s", exc)


async def resolve_target_chat_ids(client: TelegramClient, state: AppState) -> List[int]:
    target_ids = set(ALLOWED_CHAT_IDS) if STRICT_ALLOWLIST_MODE else set(state.registered_chat_ids)
    if not STRICT_ALLOWLIST_MODE:
        target_ids.update(ALLOWED_CHAT_IDS)

    valid_ids: List[int] = []
    for chat_id in sorted(target_ids):
        try:
            await client.get_entity(chat_id)
            valid_ids.append(chat_id)
        except (PeerIdInvalidError, ChannelInvalidError, ChannelPrivateError, ValueError) as exc:
            state.last_errors[str(chat_id)] = f"Чат недоступен или некорректен: {exc}"
            logger.warning("Исключаю chat_id=%s: %s", chat_id, exc)
        except FloodWaitError as exc:
            logger.warning("FloodWait при проверке chat_id=%s: %s", chat_id, exc.seconds)
            await asyncio.sleep(exc.seconds + 1)
            valid_ids.append(chat_id)
        except Exception as exc:
            state.last_errors[str(chat_id)] = f"Ошибка проверки чата: {exc}"
            logger.exception("Не удалось проверить chat_id=%s", chat_id)
    return valid_ids


async def send_message_to_chat(client: TelegramClient, chat_id: int, text: str) -> Optional[str]:
    try:
        await client.send_message(chat_id, text)
        return None
    except FloodWaitError as exc:
        logger.warning("FloodWait при отправке в %s: ждать %s сек.", chat_id, exc.seconds)
        await asyncio.sleep(exc.seconds + 1)
        try:
            await client.send_message(chat_id, text)
            return None
        except Exception as retry_exc:
            return f"Не удалось после FloodWait: {retry_exc}"
    except (
        ChatAdminRequiredError,
        UserBannedInChannelError,
        UserIsBlockedError,
        InputUserDeactivatedError,
        PeerIdInvalidError,
        ChannelInvalidError,
        ChannelPrivateError,
    ) as exc:
        return f"Нет прав или чат недоступен: {exc}"
    except (OSError, asyncio.TimeoutError) as exc:
        return f"Сетевая ошибка: {exc}"
    except RPCError as exc:
        return f"Telegram RPC ошибка: {exc}"
    except Exception as exc:
        logger.exception("Непредвиденная ошибка при отправке в %s", chat_id)
        return f"Непредвиденная ошибка: {exc}"


def format_status(state: AppState) -> str:
    mode = "allowlist" if STRICT_ALLOWLIST_MODE else "registered+allowlist"
    targets = sorted(ALLOWED_CHAT_IDS if STRICT_ALLOWLIST_MODE else (state.registered_chat_ids | ALLOWED_CHAT_IDS))
    errors = "\n".join(f"• {chat_id}: {msg}" for chat_id, msg in list(state.last_errors.items())[-10:]) or "нет"
    return (
        "<b>Текущее состояние</b>\n"
        f"Рассылка: <b>{'включена' if state.sending_enabled else 'выключена'}</b>\n"
        f"Интервал: <b>{state.interval_seconds}</b> сек.\n"
        f"Режим чатов: <b>{mode}</b>\n"
        f"Цели: <code>{targets}</code>\n"
        f"Текст: <code>{state.message_text}</code>\n"
        f"Ошибки: \n{errors}"
    )


def main_menu_buttons() -> List[List[Button]]:
    return [
        [Button.inline("▶️ Старт", b"start_send"), Button.inline("⏹ Стоп", b"stop_send")],
        [Button.inline("📄 Статус", b"show_status"), Button.inline("🗑 Очистить ошибки", b"clear_errors")],
        [Button.inline("🧹 Удалить state.json", b"delete_state")],
    ]


async def scheduler_loop(client: TelegramClient, store: StateStore) -> None:
    logger.info("Планировщик запущен")
    while True:
        state = await store.load()
        if not state.sending_enabled:
            await asyncio.sleep(2)
            continue

        state.last_cycle_started_at = time.time()
        target_ids = await resolve_target_chat_ids(client, state)
        if not target_ids:
            logger.warning("Нет доступных чатов для рассылки")
            state.last_errors["general"] = "Нет доступных чатов для рассылки"
            state.sending_enabled = False
            await store.save(state)
            await asyncio.sleep(2)
            continue

        logger.info("Начинаю цикл рассылки по %s чатам", len(target_ids))
        for chat_id in target_ids:
            error = await send_message_to_chat(client, chat_id, state.message_text)
            state = await store.load()
            if error:
                state.last_errors[str(chat_id)] = error
                logger.warning("Ошибка отправки в %s: %s", chat_id, error)
            else:
                state.last_errors.pop(str(chat_id), None)
                logger.info("Отправлено в chat_id=%s", chat_id)
            await store.save(state)
            await asyncio.sleep(1)

        state = await store.load()
        await store.save(state)
        await asyncio.sleep(max(10, state.interval_seconds))


async def handle_admin_message(event, client: TelegramClient, store: StateStore) -> None:
    if not is_admin(event.sender_id):
        await safe_reply(event, "Доступ запрещён.")
        return

    text = (event.raw_text or "").strip()
    state = await store.load()

    if text == "/start":
        await safe_reply(
            event,
            "Управление рассылкой. Доступны команды:\n"
            "/status\n"
            "/run\n"
            "/stop\n"
            "/settext <текст>\n"
            "/setinterval <секунды>\n"
            "/register_chat <chat_id>\n"
            "/unregister_chat <chat_id>\n"
            "/delete_state\n"
            "/delete_session\n",
            buttons=main_menu_buttons(),
        )
        return

    if text == "/status":
        await safe_reply(event, format_status(state), buttons=main_menu_buttons())
        return

    if text == "/run":
        state.sending_enabled = True
        await store.save(state)
        await safe_reply(event, "Рассылка включена.", buttons=main_menu_buttons())
        return

    if text == "/stop":
        state.sending_enabled = False
        await store.save(state)
        await safe_reply(event, "Рассылка остановлена.", buttons=main_menu_buttons())
        return

    if text.startswith("/settext "):
        new_text = text[len("/settext "):].strip()
        if not new_text:
            await safe_reply(event, "Текст не должен быть пустым.")
            return
        state.message_text = new_text
        await store.save(state)
        await safe_reply(event, "Текст обновлён.")
        return

    if text.startswith("/setinterval "):
        raw = text[len("/setinterval "):].strip()
        try:
            seconds = int(raw)
            if seconds < 10:
                raise ValueError
        except ValueError:
            await safe_reply(event, "Интервал должен быть целым числом не меньше 10 секунд.")
            return
        state.interval_seconds = seconds
        await store.save(state)
        await safe_reply(event, f"Интервал обновлён: {seconds} сек.")
        return

    if text.startswith("/register_chat "):
        if STRICT_ALLOWLIST_MODE:
            await safe_reply(event, "Сейчас включён STRICT_ALLOWLIST_MODE=true, регистрация отключена.")
            return
        raw = text[len("/register_chat "):].strip()
        try:
            chat_id = int(raw)
            await client.get_entity(chat_id)
        except Exception as exc:
            await safe_reply(event, f"Не удалось проверить chat_id: {exc}")
            return
        state.registered_chat_ids.add(chat_id)
        await store.save(state)
        await safe_reply(event, f"Чат {chat_id} зарегистрирован.")
        return

    if text.startswith("/unregister_chat "):
        raw = text[len("/unregister_chat "):].strip()
        try:
            chat_id = int(raw)
        except ValueError:
            await safe_reply(event, "Укажите корректный chat_id.")
            return
        state.registered_chat_ids.discard(chat_id)
        await store.save(state)
        await safe_reply(event, f"Чат {chat_id} удалён из регистрации.")
        return

    if text == "/delete_state":
        try:
            if STATE_FILE.exists():
                STATE_FILE.unlink()
            await safe_reply(event, "Файл состояния удалён. При следующем действии будет создан заново.")
        except OSError as exc:
            await safe_reply(event, f"Не удалось удалить state.json: {exc}")
        return

    if text == "/delete_session":
        await client.disconnect()
        session_path = Path(f"{SESSION_NAME}.session")
        session_journal = Path(f"{SESSION_NAME}.session-journal")
        errors = []
        for path in (session_path, session_journal):
            try:
                if path.exists():
                    path.unlink()
            except OSError as exc:
                errors.append(f"{path}: {exc}")
        if errors:
            print("\n".join(errors), file=sys.stderr)
        sys.exit(0)

    await safe_reply(event, "Неизвестная команда. Нажмите /start для справки.")


async def handle_callback(event, store: StateStore) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return

    data = event.data.decode("utf-8")
    state = await store.load()

    if data == "start_send":
        state.sending_enabled = True
        await store.save(state)
        await event.edit("Рассылка включена.", buttons=main_menu_buttons())
        return

    if data == "stop_send":
        state.sending_enabled = False
        await store.save(state)
        await event.edit("Рассылка остановлена.", buttons=main_menu_buttons())
        return

    if data == "show_status":
        await event.edit(format_status(state), buttons=main_menu_buttons(), parse_mode="html")
        return

    if data == "clear_errors":
        state.last_errors.clear()
        await store.save(state)
        await event.edit("Ошибки очищены.", buttons=main_menu_buttons())
        return

    if data == "delete_state":
        try:
            if STATE_FILE.exists():
                STATE_FILE.unlink()
            await event.edit("state.json удалён.", buttons=main_menu_buttons())
        except OSError as exc:
            await event.edit(f"Ошибка удаления state.json: {exc}", buttons=main_menu_buttons())
        return

    await event.answer("Неизвестное действие", alert=True)


async def run() -> None:
    ensure_env()
    store = StateStore(STATE_FILE)
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    try:
        await client.start(bot_token=BOT_TOKEN)
    except SessionPasswordNeededError:
        raise RuntimeError("Для bot_token не должна требоваться 2FA-пароль.")
    except Exception as exc:
        raise RuntimeError(f"Не удалось запустить Telethon client: {exc}") from exc

    me = await client.get_me()
    logger.info("Бот авторизован: @%s (%s)", getattr(me, "username", None), me.id)

    @client.on(events.NewMessage(pattern=None))
    async def on_new_message(event):
        try:
            await handle_admin_message(event, client, store)
        except Exception as exc:
            logger.exception("Ошибка в обработчике сообщений: %s", exc)
            await safe_reply(event, f"Внутренняя ошибка: {exc}")

    @client.on(events.CallbackQuery())
    async def on_callback(event):
        try:
            await handle_callback(event, store)
        except Exception as exc:
            logger.exception("Ошибка в callback-обработчике: %s", exc)
            await event.answer(f"Внутренняя ошибка: {exc}", alert=True)

    scheduler_task = asyncio.create_task(scheduler_loop(client, store))
    try:
        await client.run_until_disconnected()
    finally:
        scheduler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await scheduler_task


if __name__ == "__main__":
    import contextlib

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем")
    except Exception as exc:
        logger.exception("Критическая ошибка: %s", exc)
        sys.exit(1)
