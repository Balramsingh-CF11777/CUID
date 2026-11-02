import os
import valkey
import clickhouse_connect
from valkey.connection import BlockingConnectionPool
from dotenv import load_dotenv

# ==========================================================
# LOAD ENV
# ==========================================================
load_dotenv()

# ==========================================================
# ENVIRONMENT
# ==========================================================
ENV = os.getenv("ENVIRON", "UAT").upper()

# ==========================================================
# VALKEY CONFIG
# ==========================================================
VALKEY_HOST = os.getenv(f"{ENV}_VALKEY_HOST")
VALKEY_PORT = int(os.getenv(f"{ENV}_VALKEY_PORT"))
VALKEY_USERNAME = os.getenv(f"{ENV}_VALKEY_USERNAME")
VALKEY_PASSWORD = os.getenv(f"{ENV}_VALKEY_PASSWORD")

# ==========================================================
# CLICKHOUSE CONFIG
# ==========================================================
CH_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
CH_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
CH_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
CH_PASS = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE")

# ==========================================================
# VALIDATE REQUIRED ENV
# ==========================================================
def validate_env():
    required = {
        f"{ENV}_VALKEY_HOST": VALKEY_HOST,
        f"{ENV}_VALKEY_PORT": VALKEY_PORT,
        f"{ENV}_CUID_CLICKHOUSE_HOST": CH_HOST,
        f"{ENV}_CUID_CLICKHOUSE_PORT": CH_PORT,
        f"{ENV}_CUID_CLICKHOUSE_USER": CH_USER,
        f"{ENV}_CUID_CLICKHOUSE_DATABASE": CH_DB,
    }

    missing = [key for key, value in required.items() if value in (None, "")]

    if missing:
        raise ValueError(f"Missing required env variables: {', '.join(missing)}")


# ==========================================================
# VALKEY CLIENT
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
# CLICKHOUSE CLIENT
# ==========================================================
def get_clickhouse_client():
    try:
        return clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASS,
            database=CH_DB
        )

    except Exception as e:
        print(f"❌ ClickHouse connection failed: {e}")
        return None


# ==========================================================
# GET MAX CUID
# ==========================================================
def get_max_cuid():
    client = get_clickhouse_client()

    if not client:
        return None

    try:
        result = client.query("""
            SELECT max(toUInt64OrZero(CUID)) AS max_cuid
            FROM CUID.CUID_REPOSITORY
        """)

        max_cuid = result.result_rows[0][0] or 0

        print(f"📊 Environment            : {ENV}")
        print(f"📊 ClickHouse Host        : {CH_HOST}")
        print(f"📊 Max CUID in Repository : {max_cuid}")

        return int(max_cuid)

    except Exception as e:
        print(f"❌ Failed to fetch max CUID: {e}")
        return None

    finally:
        try:
            client.close()
        except Exception:
            pass


# ==========================================================
# UPDATE VALKEY COUNTER
# ==========================================================
def update_cuid_counter():
    max_cuid = get_max_cuid()

    if max_cuid is None:
        print("❌ Unable to fetch max CUID")
        return None

    client = get_valkey_client()

    if not client:
        print("❌ Unable to connect to Valkey")
        return None

    try:
        old_value = client.get("cuid_counter")

        # Set current max CUID.
        # Next INCR() will generate max_cuid + 1.
        client.set("cuid_counter", str(max_cuid))

        new_value = client.get("cuid_counter")

        print("\n✅ Valkey Counter Updated Successfully")
        print(f"Environment : {ENV}")
        print(f"Valkey Host : {VALKEY_HOST}")
        print(f"Old Value   : {old_value}")
        print(f"New Value   : {new_value}")

        return {
            "environment": ENV,
            "old_value": old_value,
            "new_value": new_value
        }

    except Exception as e:
        print(f"❌ Failed to update Valkey counter: {e}")
        return None


# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    try:
        validate_env()

        print(f"🚀 Running CUID Counter Sync for ENV: {ENV}")

        result = update_cuid_counter()

        if result:
            print("\n🎯 Synchronization Completed Successfully")
        else:
            print("\n❌ Synchronization Failed")

    except Exception as e:
        print(f"\n❌ Fatal Error: {e}")