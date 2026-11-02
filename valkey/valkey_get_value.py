import os
import valkey
from valkey.connection import BlockingConnectionPool
from dotenv import load_dotenv

# ==========================================================
# LOAD ENV
# ==========================================================
load_dotenv()

# ==========================================================
# ENVIRONMENT
# ==========================================================
ENV = os.getenv("ENVIRON", "DR").upper()

print(f"Running Environment : {ENV}")

# ==========================================================
# VALKEY CONFIG
# ==========================================================
VALKEY_HOST = os.getenv(f"{ENV}_VALKEY_HOST")
VALKEY_PORT = int(os.getenv(f"{ENV}_VALKEY_PORT"))

# Username/Password available only for PROD and DR
VALKEY_USERNAME = os.getenv(f"{ENV}_VALKEY_USERNAME")
VALKEY_PASSWORD = os.getenv(f"{ENV}_VALKEY_PASSWORD")

# ==========================================================
# CREATE VALKEY CLIENT
# ==========================================================
def get_valkey_client():
    try:
        pool = BlockingConnectionPool(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            username=VALKEY_USERNAME or None,
            password=VALKEY_PASSWORD or None,
            db=0,
            max_connections=100,
            timeout=5,
            decode_responses=True
        )

        return valkey.Valkey(connection_pool=pool)

    except Exception as e:
        print(f"❌ Valkey connection failed: {e}")
        return None


# ==========================================================
# READ CUID COUNTER
# ==========================================================
def get_cuid_counter():
    client = get_valkey_client()

    if not client:
        print("❌ Unable to connect to Valkey")
        return None

    try:
        old_value = client.get("cuid_counter")

        print(f"Environment : {ENV}")
        print(f"Host        : {VALKEY_HOST}")
        print(f"Port        : {VALKEY_PORT}")
        print(f"Old Value   : {old_value}")

        return {
            "environment": ENV,
            "host": VALKEY_HOST,
            "port": VALKEY_PORT,
            "old_value": old_value
        }

    except Exception as e:
        print(f"❌ Failed to read Valkey counter: {e}")
        return None


# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    result = get_cuid_counter()