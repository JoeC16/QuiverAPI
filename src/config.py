import os
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

with open(os.path.join(BASE_DIR, "config.yaml")) as f:
    CONFIG = yaml.safe_load(f)

QUIVER_API_KEY = os.getenv("QUIVER_API_KEY", "PUT_KEY_HERE")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "PUT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "PUT_CHAT_ID_HERE")
