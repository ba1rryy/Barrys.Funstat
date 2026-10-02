# Telegram Funstat Parser Bot + Search

**[Русский](README.md) | [English](README_EN.md)**

**Multifunctional Telegram bot** for parsing chats and searching for users by ID or username.

---

##  Features

-  **Search** — user search + their entire message history across chats
- **Parsing** — collecting participants and messages from Telegram chats
-  Crystal system + referral system
-  Mini-game "Guess the Number"
-  Personal statistics and user leaderboard
-  Admin panel (parsing, importing, bans, granting crystals)

---

## Quick Start

### Step 1: Download the project
```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPOSITORY.git
cd YOUR_REPOSITORY
```

### Step 2: Install dependencies
```bash
pip install -r requirements.txt
```

### Step 3: Configure tokens
```bash
Bashcp .env.example .env
cp config.example.json config.json
```
#### Open the .env file and enter your data (BOT_TOKEN, API_ID, API_HASH, etc.).

### Step 4: Telegram Authorization
```bash
python authorize.py
```

Enter the verification code that will be sent to your phone.
```

### Step 5: Start the bot
```bash
Bashpython main.py
```
### if you have any questions, write to an LLM, and if there is someone who can help, write to them;
### unfortunately, I currently do not have any contacts
