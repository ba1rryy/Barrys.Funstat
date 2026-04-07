import sqlite3
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_name="tgparser.db"):
        self.db_name = db_name

    async def init_db(self):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                crystals INTEGER DEFAULT 0,
                referrer_id INTEGER,
                usage_count INTEGER DEFAULT 0,
                FOREIGN KEY (referrer_id) REFERENCES users(user_id)
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                referrer_id INTEGER,
                referred_id INTEGER,
                rewarded INTEGER DEFAULT 0,
                timestamp TEXT,
                PRIMARY KEY (referrer_id, referred_id),
                FOREIGN KEY (referrer_id) REFERENCES users(user_id),
                FOREIGN KEY (referred_id) REFERENCES users(user_id)
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                chat_name TEXT,
                message_text TEXT,
                message_link TEXT,
                message_date TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS bans (
                user_id INTEGER PRIMARY KEY,
                ban_until TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS mutes (
                user_id INTEGER PRIMARY KEY,
                mute_until TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully.")

    def user_exists(self, user_id):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
        result = cur.fetchone()
        conn.close()
        return result is not None

    def add_user(self, user_id, first_name, last_name, username, referrer_id=None):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute('''
            INSERT OR REPLACE INTO users (user_id, first_name, last_name, username, crystals, referrer_id, usage_count)
            VALUES (?, ?, ?, ?, COALESCE((SELECT crystals FROM users WHERE user_id = ?), 0), ?, COALESCE((SELECT usage_count FROM users WHERE user_id = ?), 0))
        ''', (user_id, first_name, last_name, username, user_id, referrer_id, user_id))
        if referrer_id:
            cur.execute("INSERT OR IGNORE INTO referrals (referrer_id, referred_id, timestamp) VALUES (?, ?, ?)",
                        (referrer_id, user_id, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        logger.info(f"User {user_id} added or updated in database.")

    def get_crystals(self, user_id):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("SELECT crystals FROM users WHERE user_id = ?", (user_id,))
        result = cur.fetchone()
        conn.close()
        return result[0] if result else 0

    def add_crystals(self, user_id, amount):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("UPDATE users SET crystals = crystals + ? WHERE user_id = ?", (amount, user_id))
        if cur.rowcount == 0:
            cur.execute("INSERT INTO users (user_id, crystals) VALUES (?, ?)", (user_id, amount))
        conn.commit()
        conn.close()

    def set_crystals(self, user_id, amount):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("UPDATE users SET crystals = ? WHERE user_id = ?", (amount, user_id))
        if cur.rowcount == 0:
            cur.execute("INSERT INTO users (user_id, crystals) VALUES (?, ?)", (user_id, amount))
        conn.commit()
        conn.close()

    def get_referrer(self, user_id):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,))
        result = cur.fetchone()
        conn.close()
        return result[0] if result else None

    def is_referrer_rewarded(self, referrer_id, referred_id):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("SELECT rewarded FROM referrals WHERE referrer_id = ? AND referred_id = ?", (referrer_id, referred_id))
        result = cur.fetchone()
        conn.close()
        return result and result[0] == 1

    def mark_referrer_rewarded(self, referrer_id, referred_id):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("UPDATE referrals SET rewarded = 1 WHERE referrer_id = ? AND referred_id = ?", (referrer_id, referred_id))
        conn.commit()
        conn.close()

    def search_users(self, query):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        query = f"%{query}%"
        cur.execute("""
            SELECT user_id, first_name, last_name, username, crystals
            FROM users
            WHERE user_id LIKE ? OR first_name LIKE ? OR last_name LIKE ? OR username LIKE ?
        """, (query, query, query, query))
        users = cur.fetchall()
        conn.close()
        return users

    def get_all_users_by_crystals(self):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("SELECT user_id, first_name, last_name, username, crystals FROM users ORDER BY crystals DESC")
        users = cur.fetchall()
        conn.close()
        return users

    def count_user_messages(self, user_id):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM messages WHERE user_id = ?", (user_id,))
        count = cur.fetchone()[0]
        conn.close()
        return count

    def get_user_activity(self, user_id):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("SELECT usage_count FROM users WHERE user_id = ?", (user_id,))
        result = cur.fetchone()
        conn.close()
        return result[0] if result else 0

    def increment_usage(self, user_id):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("UPDATE users SET usage_count = usage_count + 1 WHERE user_id = ?", (user_id,))
        if cur.rowcount == 0:
            cur.execute("INSERT INTO users (user_id, usage_count) VALUES (?, 1)", (user_id,))
        conn.commit()
        conn.close()

    def count_referrals(self, user_id, since=None):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        if since:
            cur.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ? AND timestamp >= ?", (user_id, since.isoformat()))
        else:
            cur.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,))
        count = cur.fetchone()[0]
        conn.close()
        return count

    def add_message(self, user_id, chat_id, chat_name, message_text, message_link, message_date):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO messages (user_id, chat_id, chat_name, message_text, message_link, message_date)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, chat_id, chat_name, message_text, message_link, message_date))
        conn.commit()
        conn.close()

    def get_user_messages(self, user_id, page=1, per_page=5):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        offset = (page - 1) * per_page
        cur.execute("SELECT COUNT(*) FROM messages WHERE user_id = ?", (user_id,))
        total = cur.fetchone()[0]
        cur.execute("SELECT * FROM messages WHERE user_id = ? LIMIT ? OFFSET ?", (user_id, per_page, offset))
        messages = cur.fetchall()
        conn.close()
        return messages, total

    def export_users(self):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("SELECT user_id, first_name, last_name, username, crystals, referrer_id FROM users")
        users = cur.fetchall()
        conn.close()
        return users

    def export_user_messages(self, user_id):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("SELECT id, user_id, chat_id, chat_name, message_text, message_link, message_date FROM messages WHERE user_id = ?", (user_id,))
        messages = cur.fetchall()
        conn.close()
        return messages

    def get_crystals_stats(self):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("SELECT SUM(crystals), AVG(crystals) FROM users")
        total, avg = cur.fetchone()
        conn.close()
        return total or 0, avg or 0

    def is_banned(self, user_id):
        try:
            conn = sqlite3.connect(self.db_name)
            cur = conn.cursor()
            cur.execute("SELECT ban_until FROM bans WHERE user_id = ?", (user_id,))
            result = cur.fetchone()
            conn.close()
            if result:
                ban_until = result[0]
                if ban_until is None:
                    return True
                ban_until = datetime.fromisoformat(ban_until)
                return datetime.now() < ban_until
            return False
        except Exception as e:
            logger.error(f"Error checking ban status for user {user_id}: {str(e)}")
            return False

    def ban_user(self, user_id, duration_hours=None):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        ban_until = None
        if duration_hours:
            ban_until = (datetime.now() + timedelta(hours=duration_hours)).isoformat()
        cur.execute("INSERT OR REPLACE INTO bans (user_id, ban_until) VALUES (?, ?)", (user_id, ban_until))
        conn.commit()
        conn.close()

    def unban_user(self, user_id):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("DELETE FROM bans WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()

    def mute_user(self, user_id, duration_hours):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        mute_until = (datetime.now() + timedelta(hours=duration_hours)).isoformat()
        cur.execute("INSERT OR REPLACE INTO mutes (user_id, mute_until) VALUES (?, ?)", (user_id, mute_until))
        conn.commit()
        conn.close()

    def unmute_user(self, user_id):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("DELETE FROM mutes WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()

    def wipe_user(self, user_id):
        conn = sqlite3.connect(self.db_name)
        cur = conn.cursor()
        cur.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        cur.execute("DELETE FROM messages WHERE user_id = ?", (user_id,))
        cur.execute("DELETE FROM referrals WHERE referrer_id = ? OR referred_id = ?", (user_id, user_id))
        cur.execute("DELETE FROM bans WHERE user_id = ?", (user_id,))
        cur.execute("DELETE FROM mutes WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()