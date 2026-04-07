from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

API_ID = 29477403
API_HASH = "9cae8399f5f9ee05b9bbfafac0c0b640"

async def main():
    client = TelegramClient("sessions/my_new_session", API_ID, API_HASH)
    await client.start(phone=lambda: input("Введите ваш номер телефона: "))
    print("Сессия создана! Файл сохранён как sessions/my_new_session.session")

import asyncio
asyncio.run(main())