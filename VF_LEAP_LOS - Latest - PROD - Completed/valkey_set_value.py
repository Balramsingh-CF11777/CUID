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
# VALKEY CONFIG
# ==========================================================
VALKEY_HOST = os.getenv("VALKEY_HOST")
VALKEY_PORT = int(os.getenv("VALKEY_PORT"))
VALKEY_USERNAME = os.getenv("VALKEY_USERNAME")
VALKEY_PASSWORD = os.getenv("VALKEY_PASSWORD")

# ==========================================================
# CLICKHOUSE CONFIG
# ==========================================================
CH_HOST = os.getenv("DEST_CLICKHOUSE_HOST")
CH_PORT = int(os.getenv("DEST_CLICKHOUSE_PORT"))
CH_USER = os.getenv("DEST_CLICKHOUSE_USER")
CH_PASS = os.getenv("DEST_CLICKHOUSE_PASSWORD")
CH_DB = os.getenv("DEST_CLICKHOUSE_DATABASE")

# ==========================================================
# VALKEY CLIENT
# ==========================================================
def get_valkey_client():
    try:
        pool = BlockingConnectionPool(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            username=VALKEY_USERNAME,
            password=VALKEY_PASSWORD,
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

        max_cuid = result.result_rows[0][0]

        if max_cuid is None:
            max_cuid = 0

        print(f"📊 Max CUID in Repository : {max_cuid}")

        return int(max_cuid)

    except Exception as e:
        print(f"❌ Failed to fetch max CUID: {e}")
        return None

    finally:
        try:
            client.close()
        except:
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

        # Set current max CUID
        # Next INCR() call will generate max_cuid + 1
        client.set("cuid_counter", str(max_cuid))

        new_value = client.get("cuid_counter")

        print("\n✅ Valkey Counter Updated Successfully")
        print(f"Old Value : {old_value}")
        print(f"New Value : {new_value}")

        return {
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

    result = update_cuid_counter()

    if result:
        print("\n🎯 Synchronization Completed Successfully")
    else:
        print("\n❌ Synchronization Failed")