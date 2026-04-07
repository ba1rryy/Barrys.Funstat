import logging
import random
import csv
from io import StringIO, BytesIO
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram import BaseMiddleware
import asyncio
import subprocess
from datetime import datetime, timedelta
import time
import aiosqlite
from collections import Counter
from aiocryptopay import AioCryptoPay, Networks
from dotenv import load_dotenv
import os

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в .env файле!")

bot = Bot(token=BOT_TOKEN)

# if .env dont work, u also can use config.json

crypto = AioCryptoPay(token=os.getenv("CRYPTO_PAY_TOKEN"), network=Networks.MAIN_NET)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

OWNER_ID = 1234567890 #OwnerID
ADMIN_IDS = []
bot = Bot(token="") #bot token
dp = Dispatcher()

crypto = AioCryptoPay(token='', network=Networks.MAIN_NET)  #cryptobot token

class BanMiddleware(BaseMiddleware):
    def __init__(self, db):
        self.db = db

    async def __call__(self, handler, event, data):
        if isinstance(event, types.Message):
            user_id = event.from_user.id
            logger.info(f"Проверка бана для пользователя {user_id}")
            is_banned = self.db.is_banned(user_id)
            if is_banned:
                logger.info(f"Пользователь {user_id} забанен")
                await event.answer("🚫 Вы забанены и не можете использовать бота.")
                return
        return await handler(event, data)

def get_main_menu(user_id):
    buttons = [
        [KeyboardButton(text="🔍 Пробив"), KeyboardButton(text="💎 Баланс")],
        [KeyboardButton(text="🏆 Топ пользователей"), KeyboardButton(text="📊 Моя статистика")],
        [KeyboardButton(text="🎮 Угадать число"), KeyboardButton(text="👥 Рефералы")],
        [KeyboardButton(text="💰 Купить через CryptoBot"), KeyboardButton(text="📞 Поддержка")]
    ]
    if user_id == OWNER_ID or user_id in ADMIN_IDS:
        buttons.append([KeyboardButton(text="📥 Парсинг"), KeyboardButton(text="📤 Импорт")])
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    return keyboard

def get_pagination_keyboard(current_page, total_pages, user_id, prefix="page", timestamp=None):
    timestamp = timestamp or str(time.time())
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    buttons = []
    if current_page > 1:
        buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{prefix}_{user_id}_{current_page-1}_{timestamp}"))
    if current_page < total_pages:
        buttons.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"{prefix}_{user_id}_{current_page+1}_{timestamp}"))
    if buttons:
        keyboard.inline_keyboard.append(buttons)
    return keyboard

def get_cancel_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ])
    return keyboard

class GuessNumberState(StatesGroup):
    number = State()

class ChatParseState(StatesGroup):
    chat_link = State()
    chat_limit = State()

class SearchState(StatesGroup):
    query = State()

class ImportState(StatesGroup):
    file = State()

class AnnounceState(StatesGroup):
    chat_id = State()

class CaptchaState(StatesGroup):
    captcha_answer = State()

class CryptoPaymentState(StatesGroup):
    transaction_id = State()

def init_dispatcher(db):
    dp.message.middleware(BanMiddleware(db))

    @dp.callback_query(lambda c: c.data == "cancel_action")
    async def process_cancel_callback(callback: types.CallbackQuery, state: FSMContext):
        user_id = callback.from_user.id
        logger.info(f"Действие отменено пользователем {user_id}")
        await state.clear()
        await callback.message.edit_text("❌ Действие отменено!", reply_markup=None)
        await callback.message.answer("🌟 Главное меню:", reply_markup=get_main_menu(user_id))
        await callback.answer()

    @dp.message(Command(commands=["cancel"]))
    async def cmd_cancel(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Команда /cancel от пользователя {user_id}")
        await state.clear()
        await message.answer("❌ Действие отменено!", reply_markup=get_main_menu(user_id))

    @dp.message(Command(commands=["start"]))
    async def cmd_start(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Команда /start от пользователя {user_id}")
        db.increment_usage(user_id)
        user_exists = db.user_exists(user_id)
        if not user_exists:
            captcha = random.randint(1000, 9999)
            await state.update_data(captcha=captcha, attempts=3) 
            await state.set_state(CaptchaState.captcha_answer)
            await message.answer(f"⚠️ Подтвердите, что вы не бот!\n🔑 Код: **{captcha}**\nВведите код (3 попытки) или используйте /cancel:")
        else:
            crystals = db.get_crystals(user_id)
            referrer_id = db.get_referrer(user_id)
            db.add_user(user_id=user_id, first_name=message.from_user.first_name, last_name=message.from_user.last_name, username=message.from_user.username)
            if referrer_id and not db.is_referrer_rewarded(referrer_id, user_id):
                db.add_crystals(referrer_id, 4)
                db.mark_referrer_rewarded(referrer_id, user_id)
            welcome_message = (
                f"🌟 **Добро пожаловать в Testing Bot!** 🌟\n\n"
                f"👤 **Ваш ID:** {user_id}\n"
                f"💎 **Баланс:** {crystals} кристаллов\n"
                f"👥 **Пригласивший:** {referrer_id or 'Нет'}\n\n"
                f"Выберите действие в меню ниже 👇"
            )
            await message.answer(welcome_message, reply_markup=get_main_menu(user_id))
        await state.clear()

    @dp.message(CaptchaState.captcha_answer)
    async def process_captcha(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        data = await state.get_data()
        captcha = data.get("captcha")
        attempts = data.get("attempts", 3)
        user_input = message.text.strip()

        if user_input.isdigit() and int(user_input) == captcha:
            db.add_user(user_id=user_id, first_name=message.from_user.first_name, last_name=message.from_user.last_name, username=message.from_user.username)
            db.add_crystals(user_id, 100) 
            welcome_message = (
                f"🎉 **Капча пройдена!** 🎉\n\n"
                f"👤 **Добро пожаловать, {message.from_user.first_name}!**\n"
                f"💎 **Награда за вход:** +100 кристаллов!\n\n"
                f"Выберите действие в меню ниже 👇"
            )
            await message.answer(welcome_message, reply_markup=get_main_menu(user_id))
            await state.clear()
        else:
            attempts -= 1
            if attempts > 0:
                new_captcha = random.randint(1000, 9999)
                await state.update_data(captcha=new_captcha, attempts=attempts)
                await message.answer(f"❌ Неверный код! Осталось попыток: {attempts}\n🔑 Новый код: **{new_captcha}**\nВведите код или используйте /cancel:")
            else:
                db.ban_user(user_id, None) 
                await message.answer("🚫 Вы исчерпали все попытки. Вы забанены.")
                await state.clear()

    @dp.message(lambda message: message.text == "🔍 Пробив")
    async def cmd_search(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Запрос пробива от пользователя {user_id}")
        crystals = db.get_crystals(user_id)
        if crystals < 2:
            await message.answer("❌ Недостаточно кристаллов для пробива!\n💎 Требуется: 2 кристалла\nПополните баланс или пригласите друзей!")
            return
        db.add_crystals(user_id, -2)
        await state.set_state(SearchState.query)
        await message.answer("🔍 Введите user_id или username пользователя для пробива:", reply_markup=get_cancel_keyboard())

    @dp.message(SearchState.query, lambda message: not message.text.startswith('/'))
    async def process_search(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        query = message.text.strip()
        logger.info(f"Обработка запроса пробива '{query}' от пользователя {user_id}")
        if not query:
            await message.answer("❌ Введите user_id или username!", reply_markup=get_cancel_keyboard())
            return

        try:
            async with aiosqlite.connect("tgparser.db", timeout=30) as db_conn:
                
                user_info = None
                try:
                    user_id_query = int(query)
                    async with db_conn.execute('SELECT user_id, first_name, last_name, username, phone FROM users WHERE user_id = ?', (user_id_query,)) as cursor:
                        user_info = await cursor.fetchone()
                except ValueError:
                    async with db_conn.execute('SELECT user_id, first_name, last_name, username, phone FROM users WHERE username = ?', (query,)) as cursor:
                        user_info = await cursor.fetchone()
                    if user_info:
                        user_id_query = user_info[0]

                if not user_info:
                    await message.answer("❌ Пользователь не найден в базе данных.", reply_markup=get_main_menu(user_id))
                    await state.clear()
                    return

                user_id_query, first_name, last_name, username, phone = user_info
                logger.info(f"Найден пользователь: {username or 'без username'} (ID: {user_id_query})")

                
                async with db_conn.execute('SELECT COUNT(*) FROM messages WHERE user_id = ?', (user_id_query,)) as cursor:
                    total_messages = (await cursor.fetchone())[0]

                if total_messages == 0:
                    response = f"✅ Пользователь: {first_name} {last_name or ''} (@{username or 'без username'})\n📊 Сообщений: 0"
                    await message.answer(response, reply_markup=get_main_menu(user_id))
                    await state.clear()
                    return

                
                messages = []
                async with db_conn.execute('SELECT chat_id, content FROM messages WHERE user_id = ?', (user_id_query,)) as cursor:
                    async for row in cursor:
                        messages.append(row)

                message_contents = [msg[1] for msg in messages if msg[1]]
                message_counter = Counter(message_contents)
                most_common_messages = message_counter.most_common(5)

                chats_messages = {}
                for chat_id, content in messages:
                    if chat_id not in chats_messages:
                        async with db_conn.execute('SELECT title, username FROM chats WHERE id = ?', (chat_id,)) as cursor:
                            chat_info = await cursor.fetchone()
                            if chat_info:
                                chat_title, chat_username = chat_info
                                chats_messages[chat_id] = {"title": chat_title, "username": chat_username, "messages": []}
                            else:
                                chats_messages[chat_id] = {"title": f"Чат {chat_id}", "username": None, "messages": []}
                    if content:
                        chats_messages[chat_id]["messages"].append(content)

                lines = [f"✅ Пользователь: {first_name} {last_name or ''} (@{username or 'без username'})",
                         f"📊 Всего сообщений: {total_messages}"]
                if phone:
                    lines.append(f"📞 Телефон: {phone}")
                
                if most_common_messages:
                    lines.append("\n📝 Самые частые сообщения:")
                    for msg, count in most_common_messages:
                        if len(msg) > 50:
                            msg = msg[:47] + "..."
                        lines.append(f"- '{msg}' ({count} раз)")

                lines.append("\n💬 Сообщения по чатам:")
                for chat_id, chat_data in chats_messages.items():
                    chat_title = chat_data["title"]
                    chat_username = chat_data["username"]
                    chat_messages = chat_data["messages"]
                    chat_link = f"https://t.me/{chat_username}" if chat_username else f"Чат {chat_id}"
                    lines.append(f"Чат: [{chat_title}]({chat_link})")
                    lines.append(f"Сообщений: {len(chat_messages)}")
                    lines.append("Примеры сообщений:")
                    for msg in chat_messages[-5:]:
                        if len(msg) > 50:
                            msg = msg[:47] + "..."
                        lines.append(f"- {msg}")
                    lines.append("")

                response = "\n".join(lines)
                await message.answer(response, reply_markup=get_main_menu(user_id), parse_mode="Markdown")

        except Exception as e:
            logger.error(f"[Ошибка поиска]: {e}")
            await message.answer(f"❌ Ошибка при поиске: {e}", reply_markup=get_main_menu(user_id))

        await state.clear()

    @dp.message(lambda message: message.text == "💎 Баланс")
    async def cmd_balance(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Запрос баланса от пользователя {user_id}")
        crystals = db.get_crystals(user_id)
        balance_message = (
            f"💎 **Ваш баланс:** {crystals} кристаллов\n\n"
            f"Заработайте больше, приглашая друзей или играя! 👇"
        )
        await message.answer(balance_message, reply_markup=get_main_menu(user_id))
        await state.clear()

    @dp.message(lambda message: message.text == "🏆 Топ пользователей")
    async def cmd_top_users(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Запрос топа от пользователя {user_id}")
        users = db.get_all_users_by_crystals()
        if users:
            page = 1
            users_per_page = 5
            total_pages = (len(users) + users_per_page - 1) // users_per_page
            page_users = users[(page-1)*users_per_page:page*users_per_page]
            response = f"🏆 **Топ пользователей** (страница {page}/{total_pages}):\n\n"
            for i, u in enumerate(page_users, (page-1)*users_per_page):
                name = u[1] or u[3] or str(u[0])
                response += f"**{i+1}.** {name} — 💎 {u[4]}\n"
            await message.answer(response, reply_markup=get_pagination_keyboard(page, total_pages, user_id, prefix="top_page"))
        else:
            await message.answer("🌟 Топ пользователей пока пуст.", reply_markup=get_main_menu(user_id))
        await state.clear()

    @dp.callback_query(lambda c: c.data.startswith("top_page_"))
    async def process_top_pagination(callback: types.CallbackQuery):
        user_id = callback.from_user.id
        data = callback.data.split("_")
        page = int(data[2])
        logger.info(f"Пагинация топа для пользователя {user_id}, страница {page}")
        users = db.get_all_users_by_crystals()
        if users:
            users_per_page = 5
            total_pages = (len(users) + users_per_page - 1) // users_per_page
            page_users = users[(page-1)*users_per_page:page*users_per_page]
            response = f"🏆 **Топ пользователей** (страница {page}/{total_pages}):\n\n"
            for i, u in enumerate(page_users, (page-1)*users_per_page):
                name = u[1] or u[3] or str(u[0])
                response += f"**{i+1}.** {name} — 💎 {u[4]}\n"
            if callback.message.text != response:
                await callback.message.edit_text(response, reply_markup=get_pagination_keyboard(page, total_pages, user_id, prefix="top_page"))
        await callback.answer()

    @dp.message(lambda message: message.text == "📊 Моя статистика")
    async def cmd_stats(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Запрос статистики от пользователя {user_id}")
        crystals = db.get_crystals(user_id)
        messages = db.count_user_messages(user_id)
        usage = db.get_user_activity(user_id)
        response = (
            f"📊 **Статистика {message.from_user.first_name}:**\n\n"
            f"💎 **Баланс:** {crystals} кристаллов\n"
            f"📩 **Сообщений:** {messages}\n"
            f"⏱ **Запусков бота:** {usage}\n"
        )
        await message.answer(response, reply_markup=get_main_menu(user_id))
        await state.clear()

    @dp.message(lambda message: message.text == "🎮 Угадать число")
    async def cmd_play(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Запуск игры от пользователя {user_id}")
        number = random.randint(1, 100)
        attempts = 10
        await state.update_data(number=number, attempts=attempts)
        await state.set_state(GuessNumberState.number)
        await message.answer(f"🎮 **Угадай число от 1 до 100!**\nУ вас {attempts} попыток.\nВведите число:", reply_markup=get_cancel_keyboard())

    @dp.message(GuessNumberState.number, lambda message: not message.text.startswith('/'))
    async def process_guess(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        data = await state.get_data()
        number = data.get("number")
        attempts = data.get("attempts") - 1
        try:
            guess = int(message.text.strip())
            if 1 <= guess <= 100:
                if guess == number:
                    db.add_crystals(user_id, 5)
                    await message.answer(f"🎉 **Поздравляем!** Вы угадали число {number} за {10 - attempts} попыток!\n💎 **Награда:** +5 кристаллов", reply_markup=get_main_menu(user_id))
                    await state.clear()
                elif attempts > 0:
                    hint = "⬆ Число больше" if guess < number else "⬇ Число меньше"
                    await message.answer(f"{hint}\nОсталось {attempts} попыток.\nВведите новое число:", reply_markup=get_cancel_keyboard())
                    await state.update_data(attempts=attempts)
                else:
                    await message.answer(f"😞 Игра окончена!\nЗагаданное число: {number}\nПопробуйте снова!", reply_markup=get_main_menu(user_id))
                    await state.clear()
            else:
                await message.answer("⚠️ Введите число от 1 до 100!")
        except ValueError:
            await message.answer("⚠️ Введите корректное число!")

    @dp.message(lambda message: message.text == "👥 Рефералы")
    async def cmd_invite(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Запрос рефералов от пользователя {user_id}")
        today = db.count_referrals(user_id, datetime.now() - timedelta(days=1))
        yesterday = db.count_referrals(user_id, datetime.now() - timedelta(days=2))
        week = db.count_referrals(user_id, datetime.now() - timedelta(days=7))
        month = db.count_referrals(user_id, datetime.now() - timedelta(days=30))
        year = db.count_referrals(user_id, datetime.now() - timedelta(days=365))
        total = db.count_referrals(user_id)
        invite_link = f"https://t.me/{(await bot.get_me()).username}?start={user_id}"
        response = (
            f"👥 **Реферальная система**\n\n"
            f"🔗 **Ваша ссылка:** {invite_link}\n"
            f"📊 **Статистика приглашений:**\n"
            f"🌞 Сегодня: {today}\n"
            f"🌙 Вчера: {yesterday}\n"
            f"📅 Неделя: {week}\n"
            f"🌙 Месяц: {month}\n"
            f"📅 Год: {year}\n"
            f"∞ Всего: {total}\n\n"
            f"Приглашайте друзей и получайте 💎!"
        )
        await message.answer(response, reply_markup=get_main_menu(user_id))
        await state.clear()

    @dp.message(lambda message: message.text == "📞 Поддержка")
    async def cmd_support(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Запрос поддержки от пользователя {user_id}")
        await message.answer("📞 **Техподдержка**\nОбратитесь к @SupportBot для помощи.", reply_markup=get_main_menu(user_id))
        await state.clear()

    @dp.message(lambda message: message.text == "📥 Парсинг")
    async def cmd_parse_chat(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Команда 'Парсинг' от пользователя {user_id}")
        if user_id != OWNER_ID and user_id not in ADMIN_IDS:
            await message.answer("🚫 Доступ только для администраторов!")
            return
        await state.set_state(ChatParseState.chat_link)
        await message.answer("📥 Введите ссылки на чаты (через пробел или новую строку):", reply_markup=get_cancel_keyboard())

    @dp.message(ChatParseState.chat_link, lambda message: not message.text.startswith('/'))
    async def process_parse_chat(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        chat_links = [link.strip() for link in message.text.replace('\n', ' ').split() if link.strip()]
        if not chat_links:
            await message.answer("❌ Ссылки на чаты не введены!")
            await state.clear()
            return
        await message.answer("📥 Укажите количество сообщений для парсинга с каждого чата (0 - все):")
        await state.update_data(chat_links=chat_links)
        await state.set_state(ChatParseState.chat_limit)

    @dp.message(ChatParseState.chat_limit, lambda message: not message.text.startswith('/'))
    async def process_parse_limit(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        try:
            messages_limit = int(message.text.strip())
            if messages_limit < 0:
                raise ValueError("Отрицательное значение")
            data = await state.get_data()
            chat_links = data.get("chat_links")
            if not chat_links:
                await message.answer("❌ Ошибка данных парсинга!")
                await state.clear()
                return
            await message.answer("📥 Инициирован процесс парсинга...")
            with open("chat_links.txt", "w") as f:
                f.write(" ".join(chat_links))
            process = await asyncio.create_subprocess_exec(
                "python3", "telethon_parser.py",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            if process.returncode == 0:
                await message.answer(f"🎉 Парсинг успешно завершен!\n{stdout.decode()}")
            else:
                await message.answer(f"❌ Ошибка при парсинге:\n{stderr.decode()}")
        except ValueError:
            await message.answer("❌ Введите корректное число!")
            return
        except Exception as e:
            logger.error(f"Ошибка парсинга: {str(e)}")
            await message.answer(f"❌ Произошла ошибка: {str(e)}")
        await state.clear()
        await message.answer("🌟 Главное меню:", reply_markup=get_main_menu(user_id))

    @dp.message(lambda message: message.text == "📤 Импорт")
    async def cmd_import_users(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Команда 'Импорт' от пользователя {user_id}")
        if user_id != OWNER_ID and user_id not in ADMIN_IDS:
            await message.answer("🚫 Доступ только для администраторов!")
            return
        await message.answer("📤 Отправьте CSV-файл (user_id,first_name,last_name,username):", reply_markup=get_cancel_keyboard())
        await state.set_state(ImportState.file)

    @dp.message(ImportState.file, lambda message: message.document)
    async def process_import_file(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Импорт файла от пользователя {user_id}")
        try:
            file = await bot.get_file(message.document.file_id)
            file_content = await bot.download_file(file.file_path)
            content = file_content.read().decode('utf-8')
            csv_reader = csv.reader(StringIO(content))
            next(csv_reader, None) 
            participants = [dict(zip(["user_id", "first_name", "last_name", "username"], row)) for row in csv_reader if len(row) == 4]
            for participant in participants:
                db.add_user(**{k: (int(v) if k == "user_id" else v) for k, v in participant.items()})
            await message.answer(f"🎉 Импорт завершён!\nИмпортировано: {len(participants)} участников")
        except Exception as e:
            logger.error(f"Ошибка импорта: {str(e)}")
            await message.answer(f"❌ Ошибка при импорте: {str(e)}")
        await state.clear()
        await message.answer("🌟 Главное меню:", reply_markup=get_main_menu(user_id))

    @dp.chat_member()
    async def handle_chat_member_update(update: types.ChatMemberUpdated):
        if update.new_chat_member.status == "member" and not update.new_chat_member.user.is_bot:
            user = update.new_chat_member.user
            referrer_id = None
            if update.invite_link and hasattr(update.invite_link, 'name') and update.invite_link.name.startswith("start="):
                referrer_id = int(update.invite_link.name.split("=")[1])
            db.add_user(user_id=user.id, first_name=user.first_name, last_name=user.last_name, username=user.username, referrer_id=referrer_id)

    @dp.message(Command(commands=["announce"]))
    async def cmd_announce(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Команда 'announce' от пользователя {user_id}")
        if user_id != OWNER_ID and user_id not in ADMIN_IDS:
            await message.answer("🚫 Доступ только для администраторов!")
            return
        await message.answer("📢 Введите ID или ссылку чата для объявления:", reply_markup=get_cancel_keyboard())
        await state.set_state(AnnounceState.chat_id)

    @dp.message(AnnounceState.chat_id, lambda message: not message.text.startswith('/'))
    async def process_announce_chat(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        chat_id = message.text.strip().replace("https://t.me/", "@") if message.text.startswith("https://t.me/") else int(message.text) if message.text.startswith("-100") else None
        if chat_id:
            try:
                await bot.send_message(chat_id, "📢 Напишите /start, чтобы получить 1 💎!")
                await message.answer("🎉 Объявление отправлено!")
            except Exception as e:
                logger.error(f"Ошибка объявления: {str(e)}")
                await message.answer(f"❌ Ошибка отправки: {str(e)}")
        await state.clear()
        await message.answer("🌟 Главное меню:", reply_markup=get_main_menu(user_id))

    @dp.message(Command(commands=["set_crystals"]))
    async def cmd_set_crystal(message: types.Message):
        user_id = message.from_user.id
        logger.info(f"Команда 'set_crystals' от пользователя {user_id}")
        if user_id != OWNER_ID and user_id not in ADMIN_IDS:
            await message.answer("🚫 Доступ только для администраторов!")
            return
        args = message.text.split()
        if len(args) == 3:
            try:
                db.set_crystals(int(args[1]), int(args[2]))
                await message.answer(f"✅ Баланс пользователя {args[1]} установлен на {args[2]} 💎", reply_markup=get_main_menu(user_id))
            except Exception as e:
                logger.error(f"Ошибка set_crystals: {str(e)}")
                await message.answer(f"❌ Ошибка: {str(e)}")
        else:
            await message.answer("❌ Формат: /set_crystals <user_id> <crystals>")

    @dp.message(Command(commands=["givecrystal"]))
    async def cmd_give_crystal(message: types.Message):
        user_id = message.from_user.id
        logger.info(f"Команда 'givecrystal' от пользователя {user_id}")
        if user_id != OWNER_ID and user_id not in ADMIN_IDS:
            await message.answer("🚫 Доступ только для администраторов!")
            return
        args = message.text.split()
        if len(args) == 3:
            try:
                target_id = int(args[1])
                amount = int(args[2])
                db.add_crystals(target_id, amount)
                await message.answer(f"✅ Пользователю {target_id} начислено {amount} 💎", reply_markup=get_main_menu(user_id))
            except Exception as e:
                logger.error(f"Ошибка givecrystal: {str(e)}")
                await message.answer(f"❌ Ошибка: {str(e)}")
        else:
            await message.answer("❌ Формат: /givecrystal <user_id> <crystals>")

    @dp.message(Command(commands=["ban"]))
    async def cmd_ban(message: types.Message):
        user_id = message.from_user.id
        logger.info(f"Команда 'ban' от пользователя {user_id}")
        if user_id != OWNER_ID and user_id not in ADMIN_IDS:
            await message.answer("🚫 Доступ только для администраторов!")
            return
        args = message.text.split()
        if len(args) >= 2:
            try:
                db.ban_user(int(args[1]), int(args[2]) if len(args) > 2 else None)
                await message.answer(f"🚫 Пользователь {args[1]} заблокирован!")
            except Exception as e:
                logger.error(f"Ошибка ban: {str(e)}")
                await message.answer(f"❌ Ошибка: {str(e)}")
        else:
            await message.answer("❌ Формат: /ban <user_id> [duration]")

    @dp.message(Command(commands=["unban"]))
    async def cmd_unban(message: types.Message):
        user_id = message.from_user.id
        logger.info(f"Команда 'unban' от пользователя {user_id}")
        if user_id != OWNER_ID and user_id not in ADMIN_IDS:
            await message.answer("🚫 Доступ только для администраторов!")
            return
        args = message.text.split()
        if len(args) == 2:
            try:
                db.unban_user(int(args[1]))
                await message.answer(f"✅ Пользователь {args[1]} разблокирован!")
            except Exception as e:
                logger.error(f"Ошибка unban: {str(e)}")
                await message.answer(f"❌ Ошибка: {str(e)}")
        else:
            await message.answer("❌ Формат: /unban <user_id>")

    @dp.message(Command(commands=["mute"]))
    async def cmd_mute(message: types.Message):
        user_id = message.from_user.id
        logger.info(f"Команда 'mute' от пользователя {user_id}")
        if user_id != OWNER_ID and user_id not in ADMIN_IDS:
            await message.answer("🚫 Доступ только для администраторов!")
            return
        args = message.text.split()
        if len(args) == 3:
            try:
                db.mute_user(int(args[1]), int(args[2]))
                await message.answer(f"🔇 Пользователь {args[1]} замьючен на {args[2]} часов!")
            except Exception as e:
                logger.error(f"Ошибка mute: {str(e)}")
                await message.answer(f"❌ Ошибка: {str(e)}")
        else:
            await message.answer("❌ Формат: /mute <user_id> <hours>")

    @dp.message(Command(commands=["unmute"]))
    async def cmd_unmute(message: types.Message):
        user_id = message.from_user.id
        logger.info(f"Команда 'unmute' от пользователя {user_id}")
        if user_id != OWNER_ID and user_id not in ADMIN_IDS:
            await message.answer("🚫 Доступ только для администраторов!")
            return
        args = message.text.split()
        if len(args) == 2:
            try:
                db.unmute_user(int(args[1]))
                await message.answer(f"✅ Пользователь {args[1]} размьючен!")
            except Exception as e:
                logger.error(f"Ошибка unmute: {str(e)}")
                await message.answer(f"❌ Ошибка: {str(e)}")
        else:
            await message.answer("❌ Формат: /unmute <user_id>")

    @dp.message(Command(commands=["wipe_user"]))
    async def cmd_wipe_user(message: types.Message):
        user_id = message.from_user.id
        logger.info(f"Команда 'wipe_user' от пользователя {user_id}")
        if user_id != OWNER_ID and user_id not in ADMIN_IDS:
            await message.answer("🚫 Доступ только для администраторов!")
            return
        args = message.text.split()
        if len(args) == 2:
            try:
                db.wipe_user(int(args[1]))
                await message.answer(f"🗑️ Данные пользователя {args[1]} удалены!")
            except Exception as e:
                logger.error(f"Ошибка wipe_user: {str(e)}")
                await message.answer(f"❌ Ошибка: {str(e)}")
        else:
            await message.answer("❌ Формат: /wipe_user <user_id>")

    @dp.message(Command(commands=["export_users"]))
    async def cmd_export_users(message: types.Message):
        user_id = message.from_user.id
        logger.info(f"Команда 'export_users' от пользователя {user_id}")
        if user_id != OWNER_ID and user_id not in ADMIN_IDS:
            await message.answer("🚫 Доступ только для администраторов!")
            return
        try:
            users = db.export_users()
            if users:
                output = "ID,First Name,Last Name,Username,Crystals,Referrer ID\n" + "\n".join([f"{u[0]},{u[1] or ''},{u[2] or ''},{u[3] or ''},{u[4]},{u[5] or ''}" for u in users])
                file = BytesIO(output.encode())
                file.name = "users_export.csv"
                await message.answer_document(types.InputFile(file), caption="📄 Экспорт пользователей выполнен!")
            else:
                await message.answer("🌟 Нет данных для экспорта!")
        except Exception as e:
            logger.error(f"Ошибка export_users: {str(e)}")
            await message.answer(f"❌ Ошибка: {str(e)}")

    @dp.message(Command(commands=["export_messages"]))
    async def cmd_export_messages(message: types.Message):
        user_id = message.from_user.id
        logger.info(f"Команда 'export_messages' от пользователя {user_id}")
        if user_id != OWNER_ID and user_id not in ADMIN_IDS:
            await message.answer("🚫 Доступ только для администраторов!")
            return
        args = message.text.split()
        if len(args) == 2:
            try:
                messages = db.export_user_messages(int(args[1]))
                if messages:
                    output = "Message ID,User ID,Chat ID,Chat Name,Message Text,Message Link,Message Date\n" + "\n".join([f"{m[0]},{m[1]},{m[2]},{m[3] or ''},\"{m[4] or ''}\",{m[5] or ''},{m[6]}" for m in messages])
                    file = BytesIO(output.encode())
                    file.name = f"messages_{args[1]}_export.csv"
                    await message.answer_document(types.InputFile(file), caption="📄 Экспорт сообщений выполнен!")
                else:
                    await message.answer(f"🌟 Нет сообщений для пользователя {args[1]}!")
            except Exception as e:
                logger.error(f"Ошибка export_messages: {str(e)}")
                await message.answer(f"❌ Ошибка: {str(e)}")
        else:
            await message.answer("❌ Формат: /export_messages <user_id>")

    @dp.message(Command(commands=["stats_crystals"]))
    async def cmd_crystals_stats(message: types.Message):
        user_id = message.from_user.id
        logger.info(f"Команда 'stats_crystals' от пользователя {user_id}")
        if user_id != OWNER_ID and user_id not in ADMIN_IDS:
            await message.answer("🚫 Доступ только для администраторов!")
            return
        try:
            total, avg = db.get_crystals_stats()
            await message.answer(f"💎 **Статистика кристаллов:**\nВсего: {total}\nСреднее: {avg:.2f}")
        except Exception as e:
            logger.error(f"Ошибка stats_crystals: {str(e)}")
            await message.answer(f"❌ Ошибка: {str(e)}")

    @dp.message(lambda message: message.text and not message.text.startswith('/'))
    async def handle_message(message: types.Message):
        user_id = message.from_user.id
        logger.info(f"Сообщение от пользователя {user_id}: {message.text}")
        try:
            db.add_user(user_id=user_id, first_name=message.from_user.first_name, last_name=message.from_user.last_name, username=message.from_user.username)
            if message.chat.type != "private":
                message_link = f"https://t.me/c/{str(message.chat.id).replace('-100', '')}/{message.message_id}" if message.chat.id < 0 else ''
                db.add_message(user_id=user_id, chat_id=message.chat.id, chat_name=message.chat.title, message_text=message.text, message_link=message_link, message_date=message.date.isoformat())
                message_count = db.count_user_messages(user_id)
                if message_count % 10 == 0:
                    db.add_crystals(user_id, 1)
                    await message.answer(f"🎉 Поздравляем! +1 💎 за {message_count} сообщений!")
        except Exception as e:
            logger.error(f"Ошибка обработки: {str(e)}")
            await message.answer(f"❌ Произошла ошибка: {str(e)}")

    @dp.callback_query(lambda c: c.data.startswith("page_"))
    async def process_pagination(callback: types.CallbackQuery):
        user_id = callback.from_user.id
        data = callback.data.split("_")
        target_user_id, page = int(data[1]), int(data[2])
        logger.info(f"Пагинация для пользователя {user_id}, страница {page}")
        try:
            messages, total = db.get_user_messages(target_user_id, page)
            total_pages = (total + 4) // 5
            response = f"💬 Сообщения пользователя {target_user_id} (страница {page}/{total_pages}):\n\n" + "\n".join([f"📅 {m[6]} в {m[3]}: {m[4]}" for m in messages])
            await callback.message.edit_text(response, reply_markup=get_pagination_keyboard(page, total_pages, target_user_id))
            await callback.answer()
        except Exception as e:
            logger.error(f"Ошибка пагинации: {str(e)}")
            await callback.message.edit_text(f"❌ Ошибка: {str(e)}")
            await callback.answer()

    @dp.message(lambda message: message.text == "💰 Купить через CryptoBot")
    async def cmd_buy_crypto(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        logger.info(f"Запрос покупки через CryptoBot от пользователя {user_id}")
        try:
           
            invoice = await crypto.create_invoice(
                amount=100,  
                currency="USDT",
                description=f"Покупка 100 кристаллов для пользователя {user_id}"
            )
            invoice_id = invoice.invoice_id
            pay_url = invoice.bot_invoice_url  
            await message.answer(
                f"💰 **Покупка кристаллов через CryptoBot**\n\n"
                f"1 кристалл = 1 ₽ (~0.0105 USD по текущему курсу)\n"
                f"Сумма: 100 USDT = 9500 кристаллов\n"
                f"Перейдите по ссылке для оплаты: [Оплатить]({pay_url})\n"
                f"После оплаты отправьте ID транзакции `{invoice_id}` сюда для начисления кристаллов.",
                reply_markup=get_cancel_keyboard(),
                parse_mode="Markdown"
            )
            await state.set_state(CryptoPaymentState.transaction_id)
        except Exception as e:
            logger.error(f"Ошибка создания инвойса: {str(e)}")
            await message.answer(
                f"❌ Ошибка при создании инвойса: {str(e)}\n"
                f"Попробуйте снова позже.",
                reply_markup=get_main_menu(user_id)
            )
            await state.clear()

    @dp.message(CryptoPaymentState.transaction_id, lambda message: not message.text.startswith('/'))
    async def process_crypto_payment(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        transaction_id = message.text.strip()
        logger.info(f"Получен ID транзакции {transaction_id} от пользователя {user_id}")
        try:
            invoices = await crypto.get_invoices(invoice_ids=int(transaction_id))
            if invoices and invoices[0].status == "paid":
                amount = float(invoices[0].amount) 
                
                crystals = int(amount * 95)  
                db.add_crystals(user_id, crystals)
                await message.answer(
                    f"🎉 Оплата подтверждена! Вам начислено {crystals} кристаллов.\n"
                    f"Новый баланс: {db.get_crystals(user_id)} 💎\n"
                    f"Спасибо за покупку!",
                    reply_markup=get_main_menu(user_id)
                )
            else:
                await message.answer(
                    "❌ Транзакция не подтверждена. Убедитесь, что ID верный, или попробуйте снова.",
                    reply_markup=get_main_menu(user_id)
                )
        except Exception as e:
            logger.error(f"Ошибка обработки оплаты: {str(e)}")
            await message.answer(
                f"❌ Ошибка при обработке транзакции: {str(e)}\n"
                f"Пожалуйста, свяжитесь с поддержкой.",
                reply_markup=get_main_menu(user_id)
            )
        await state.clear()

    return dp

if __name__ == '__main__':
    executor = Executor(dp)
    executor.start_polling(skip_updates=True)