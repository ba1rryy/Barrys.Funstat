import logging
from pyrogram import Client
from database import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("parser.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

async def parse_chats(chat_links, db: Database, api_id: int, api_hash: str):
    logger.info(f"Starting parse_chats with API_ID: {api_id}, API_HASH: {api_hash[:5]}...")
    logger.info(f"Chat links to parse: {chat_links}")

    app = Client(
        "parser_session",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True
    )
    
    try:
        await app.start()
        logger.info("Pyrogram client started successfully.")
    except Exception as e:
        logger.error(f"Failed to start Pyrogram client: {e}")
        return

    for chat_link in chat_links:
        try:
            logger.info(f"Attempting to get chat: {chat_link}")
            chat = await app.get_chat(chat_link)
            logger.info(f"Processing chat: {chat.title} ({chat.id})")
            async for member in app.get_chat_members(chat.id):
                user = member.user
                await db.add_user(user.id, user.first_name, user.last_name, user.username)
                logger.debug(f"Added user {user.id} from chat {chat.title}")
        except Exception as e:
            logger.error(f"Error processing chat {chat_link}: {e}")
            continue
    
    await app.stop()
    logger.info("Pyrogram client stopped.")
