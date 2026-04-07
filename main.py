import asyncio
from bot import init_dispatcher, bot
from database import Database

async def main():
    db = Database()
    await db.init_db()  
    dp = init_dispatcher(db)
    await dp.start_polling(bot)  

if __name__ == '__main__':
    asyncio.run(main())