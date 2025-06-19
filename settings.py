from dotenv import load_dotenv
import os

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")

# Безопасная обработка ADMIN_IDS
admin_ids_str = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = {int(id.strip()) for id in admin_ids_str.split(",") if id.strip()}