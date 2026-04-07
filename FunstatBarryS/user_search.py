import asyncio
import aiosqlite
from collections import Counter
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("search.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def search_user(search_term):
    try:
        async with aiosqlite.connect("tgparser.db", timeout=30) as db:
            user_id = None
            user_info = None

            try:
                user_id = int(search_term)
                async with db.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)) as cursor:
                    user_info = await cursor.fetchone()
            except ValueError:
                async with db.execute('SELECT * FROM users WHERE username = ?', (search_term,)) as cursor:
                    user_info = await cursor.fetchone()
                if user_info:
                    user_id = user_info[0]

            if not user_info:
                return f"❌ Пользователь '{search_term}' не найден в базе данных."

            user_id, first_name, last_name, username, phone, is_owner, is_admin = user_info
            logger.info(f"Найден пользователь: {username or 'без username'} (ID: {user_id})")

            async with db.execute('SELECT COUNT(*) FROM messages WHERE user_id = ?', (user_id,)) as cursor:
                total_messages = (await cursor.fetchone())[0]

            if total_messages == 0:
                return f"✅ Пользователь: {first_name} {last_name or ''} (@{username or 'без username'})\n📊 Сообщений: 0"

            messages = []
            async with db.execute('SELECT chat_id, content FROM messages WHERE user_id = ?', (user_id,)) as cursor:
                async for row in cursor:
                    messages.append(row)

            message_contents = [msg[1] for msg in messages if msg[1]]
            message_counter = Counter(message_contents)
            most_common_messages = message_counter.most_common(5)

            chats_messages = {}
            for chat_id, content in messages:
                if chat_id not in chats_messages:
                    async with db.execute('SELECT title, username FROM chats WHERE id = ?', (chat_id,)) as cursor:
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
            lines.append(f"👑 Владелец: {'Да' if is_owner else 'Нет'}")
            lines.append(f"🛠 Админ: {'Да' if is_admin else 'Нет'}")
            
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

            return "\n".join(lines)

    except Exception as e:
        logger.error(f"[Ошибка поиска]: {e}")
        return f"❌ Ошибка при поиске: {e}"

async def main_search():
    while True:
        search_term = input("Введите user_id или username пользователя для поиска (или 'exit' для выхода): ").strip()
        if search_term.lower() == 'exit':
            break
        if search_term:
            result = await search_user(search_term)
            print(result)
        else:
            print("❌ Введите user_id или username!")

if __name__ == "__main__":
    asyncio.run(main_search())