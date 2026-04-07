import os
import json
import asyncio
import time
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
import aiosqlite
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("parser.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CONFIG_FILE = "config.json"
def load_config():
    try:
        with open(CONFIG_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"[Ошибка загрузки config.json]: {e}")
        return {}

config = load_config()

SESSION_DIR = "sessions"
try:
    if not os.path.exists(SESSION_DIR):
        os.makedirs(SESSION_DIR)
        logger.info(f"Создана директория {SESSION_DIR}")
    if not os.access(SESSION_DIR, os.W_OK):
        logger.error(f"Нет прав на запись в директорию {SESSION_DIR}")
        raise PermissionError(f"Нет прав на запись в директорию {SESSION_DIR}")
except Exception as e:
    logger.error(f"[Ошибка при создании директории {SESSION_DIR}]: {e}")
    raise

def load_chat_links():
    try:
        with open("chat_links.txt", "r", encoding="utf-8") as f:
            return [link.strip() for link in f.read().splitlines() if link.strip()]
    except Exception as e:
        logger.error(f"[Ошибка загрузки chat_links.txt]: {e}")
        return []

async def init_db():
    async with aiosqlite.connect("tgparser.db") as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                phone TEXT,
                is_owner INTEGER DEFAULT 0,
                is_admin INTEGER DEFAULT 0
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS chats (
                id INTEGER PRIMARY KEY,
                title TEXT,
                username TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER,
                chat_id INTEGER,
                user_id INTEGER,
                date TEXT,
                link TEXT,
                content TEXT,
                UNIQUE(message_id, chat_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')

        async with db.execute('PRAGMA table_info(users)') as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
        if 'phone' not in columns:
            await db.execute('ALTER TABLE users ADD COLUMN phone TEXT')
            logger.info("Добавлен столбец phone в таблицу users")
        if 'is_owner' not in columns:
            await db.execute('ALTER TABLE users ADD COLUMN is_owner INTEGER DEFAULT 0')
            logger.info("Добавлен столбец is_owner в таблицу users")
        if 'is_admin' not in columns:
            await db.execute('ALTER TABLE users ADD COLUMN is_admin INTEGER DEFAULT 0')
            logger.info("Добавлен столбец is_admin в таблицу users")

        async with db.execute('PRAGMA table_info(messages)') as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
        if 'message_id' not in columns:
            await db.execute('ALTER TABLE messages ADD COLUMN message_id INTEGER')
            logger.info("Добавлен столбец message_id в таблицу messages")
        if 'chat_id' not in columns:
            await db.execute('ALTER TABLE messages ADD COLUMN chat_id INTEGER')
            logger.info("Добавлен столбец chat_id в таблицу messages")
        if 'user_id' not in columns:
            await db.execute('ALTER TABLE messages ADD COLUMN user_id INTEGER')
            logger.info("Добавлен столбец user_id в таблицу messages")
        if 'date' not in columns:
            await db.execute('ALTER TABLE messages ADD COLUMN date TEXT')
            logger.info("Добавлен столбец date в таблицу messages")
        if 'link' not in columns:
            await db.execute('ALTER TABLE messages ADD COLUMN link TEXT')
            logger.info("Добавлен столбец link в таблицу messages")
        if 'content' not in columns:
            await db.execute('ALTER TABLE messages ADD COLUMN content TEXT')
            logger.info("Добавлен столбец content в таблицу messages")

        await db.commit()

async def insert_users_buffered(users_buffer):
    if not users_buffer:
        return
    async with aiosqlite.connect("tgparser.db") as db:
        await db.executemany('''
            INSERT OR REPLACE INTO users (user_id, first_name, last_name, username, phone, is_owner, is_admin)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', users_buffer)
        await db.commit()
    logger.info(f"Записано {len(users_buffer)} пользователей в базу данных")

async def insert_messages_buffered(messages_buffer):
    if not messages_buffer:
        return
    async with aiosqlite.connect("tgparser.db") as db:
        try:
            await db.executemany('''
                INSERT OR IGNORE INTO messages (message_id, chat_id, user_id, date, link, content)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', messages_buffer)
            await db.commit()
            logger.info(f"Записано {len(messages_buffer)} сообщений в базу данных")
        except Exception as e:
            logger.error(f"[Ошибка вставки сообщений]: {e}")

async def insert_chat(chat_id, title, username):
    async with aiosqlite.connect("tgparser.db") as db:
        await db.execute('''
            INSERT OR IGNORE INTO chats (id, title, username)
            VALUES (?, ?, ?)
        ''', (chat_id, title, username))
        await db.commit()

async def parse_chat(chat_username, messages_limit):
    session_name = f"{SESSION_DIR}/auth_session"
    logger.info(f"Используем сессию: {session_name}")
    
    if not os.path.exists(session_name + '.session'):
        logger.error(f"Сессия {session_name} не найдена. Сначала авторизуйся с помощью `authorize.py`")
        return False, None, None, None, 0, 0, 0, 0
    
    try:
        client = TelegramClient(session_name, config.get('api_id'), config.get('api_hash'))
    except Exception as e:
        logger.error(f"[Ошибка создания TelegramClient для {session_name}]: {e}")
        raise
    
    async with client:
        try:
            await asyncio.wait_for(client.connect(), timeout=30)
            logger.info(f"Подключение к {session_name} успешно")
            
            authorized = await asyncio.wait_for(client.is_user_authorized(), timeout=10)
            if not authorized:
                logger.error(f"Сессия {session_name} не авторизована. Запусти `authorize.py` для авторизации")
                return False, None, None, None, 0, 0, 0, 0
            
            logger.info(f"Сессия {session_name} авторизована, продолжаем парсинг")
            
            entity = await asyncio.wait_for(client.get_entity(chat_username), timeout=30)
            await insert_chat(entity.id, getattr(entity, 'title', ''), getattr(entity, 'username', None))
            logger.info(f"Найден чат: {entity.title} (ID: {entity.id})")
            
            admins = []
            users_buffer = []
            start_time = time.time()
            async for user in client.iter_participants(entity, filter=ChannelParticipantsSearch('')):
                if not user.bot:
                    is_owner = getattr(user, 'is_owner', False)
                    is_admin = getattr(user, 'is_admin', False)
                    users_buffer.append((
                        user.id, user.first_name, user.last_name, user.username,
                        getattr(user, 'phone', None), 1 if is_owner else 0, 1 if is_admin else 0
                    ))
                    if is_owner or is_admin:
                        admins.append(user.id)
                    if len(users_buffer) >= 100:
                        await insert_users_buffered(users_buffer)
                        users_buffer = []
                    logger.debug(f"Обработан пользователь {user.id}")
                    await asyncio.sleep(0.1) 
                if time.time() - start_time > 300: 
                    logger.warning(f"Парсинг участников чата {chat_username} превысил 5 минут, прерываем")
                    break
            if users_buffer:
                await insert_users_buffered(users_buffer)
            logger.info(f"Обработано {len(admins)} владельцев/админов и {entity.participants_count} пользователей в чате {entity.title}")
            
            count = 0
            added = 0
            messages_buffer = []
            start_time = time.time()
            async for message in client.iter_messages(entity, limit=messages_limit if messages_limit > 0 else None):
                user = message.sender
                if hasattr(user, 'id') and hasattr(user, 'bot'):
                    if not user.bot: 
                        users_buffer.append((
                            user.id, user.first_name, user.last_name, user.username,
                            getattr(user, 'phone', None), 0, 0
                        ))
                        msg_link = f"https://t.me/{getattr(entity, 'username', 'c')}/{message.id}" if getattr(entity, 'username', None) else f"https://t.me/c/{str(entity.id)[4:]}/{message.id}"
                        messages_buffer.append((
                            message.id, entity.id, user.id, message.date.isoformat(),
                            msg_link, message.text or ''
                        ))
                        count += 1
                        added += 1
                        if len(messages_buffer) >= 100:
                            await insert_messages_buffered(messages_buffer)
                            await insert_users_buffered(users_buffer)
                            messages_buffer = []
                            users_buffer = []
                        if count % 10 == 0: 
                            logger.info(f"Обработано {count} сообщений в чате {entity.title}")
                        await asyncio.sleep(0.1)  
                elif hasattr(user, 'id'): 
                    logger.debug(f"Пропущено сообщение от канала/группы с ID {user.id}")
                else:
                    logger.warning(f"Неизвестный тип sender для сообщения {message.id}")
                if time.time() - start_time > 600: 
                    logger.warning(f"Парсинг сообщений чата {chat_username} превысил 10 минут, прерываем")
                    break
            if messages_buffer:
                await insert_messages_buffered(messages_buffer)
                await insert_users_buffered(users_buffer)
            end_time = time.time()
            return True, entity.id, getattr(entity, 'title', ''), getattr(entity, 'username', ''), len(admins), count, added, end_time - start_time
        except asyncio.TimeoutError:
            logger.error(f"[Таймаут при парсинге {chat_username}]")
            return False, None, None, None, 0, 0, 0, 0
        except Exception as e:
            logger.error(f"[Ошибка парсинга {chat_username}]: {e}")
            return False, None, None, None, 0, 0, 0, 0
        finally:
            await client.disconnect()

async def parse_chats_sequential(chat_links, messages_limit):
    if not chat_links:
        return "❌ Нет ссылок на чаты для парсинга."
    
    stats = []
    total_users = 0
    total_msgs = 0
    total_added = 0
    total_time = 0
    parse_start = time.time()
    
    for chat_link in chat_links:
        result = await parse_chat(chat_link, messages_limit)
        ok, chat_id, chat_title, chat_uname, admin_count, msg_count, msg_added, elapsed = result
        stats.append((chat_link, ok, chat_title, chat_uname, admin_count, msg_count, msg_added, elapsed))
        total_users += admin_count
        total_msgs += msg_count
        total_added += msg_added
        total_time += elapsed
    
    parse_end = time.time()
    async with aiosqlite.connect("tgparser.db") as db:
        async with db.execute('SELECT COUNT(*) FROM messages') as cursor:
            messages_in_db = (await cursor.fetchone())[0]
        async with db.execute('SELECT COUNT(*) FROM users') as cursor:
            users_in_db = (await cursor.fetchone())[0]
    
    lines = []
    for chat_link, ok, chat_title, chat_uname, admin_count, msg_count, msg_added, elapsed in stats:
        if ok:
            link = f"https://t.me/{chat_uname}" if chat_uname else chat_link
            lines.append(f"✅ [{chat_title}]({link}): добавлено {admin_count} владельцев/админов, обработано {msg_count} сообщений, добавлено {msg_added}, время {elapsed:.1f} сек.")
        else:
            lines.append(f"❌ {chat_link}: ошибка парсинга.")
    
    duration = parse_end - parse_start
    speed_min = (total_msgs / duration * 60) if duration > 0 else 0
    speed_sec = (total_msgs / duration) if duration > 0 else 0
    lines.append(f"\n📊 Всего пользователей в базе: {users_in_db}")
    lines.append(f"📊 Всего сообщений в базе: {messages_in_db}")
    lines.append(f"📊 Всего новых владельцев/админов: {total_users}")
    lines.append(f"📊 Всего новых сообщений: {total_added}")
    lines.append(f"📊 Время выполнения: {int(duration // 60)}м {int(duration % 60)}с")
    lines.append(f"📊 Скорость: {speed_min:.1f} сообщ./мин, {speed_sec:.2f} сообщ./сек")
    return "\n".join(lines)

if __name__ == "__main__":
    asyncio.run(init_db()) 
    chat_links = load_chat_links()
    if chat_links:
        result = asyncio.run(parse_chats_sequential(chat_links, 0))
        print(result)
    else:
        logger.error("[Ошибка]: Файл chat_links.txt пуст или отсутствует.")