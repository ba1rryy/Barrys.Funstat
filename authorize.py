import asyncio
from telethon import TelegramClient

async def authorize():
    client = TelegramClient('sessions/auth_session', 29477403, '9cae8399f5f9ee05b9bbfafac0c0b640')
    await client.connect()
    if not await client.is_user_authorized():
        sent_code = await client.send_code_request('+447405448995')
        code = input("Введите код авторизации: ")
        await client.sign_in(phone='+447405448995', code=code, phone_code_hash=sent_code.phone_code_hash)
    print("Сессия авторизована!")
    await client.disconnect()

asyncio.run(authorize())