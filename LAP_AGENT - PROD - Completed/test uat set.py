import os
import valkey
from dotenv import load_dotenv
from valkey.connection import BlockingConnectionPool

load_dotenv()

ENV = os.getenv("ENVIRON", "UAT").upper()

VALKEY_HOST = os.getenv(f"{ENV}_VALKEY_HOST")
VALKEY_PORT = int(os.getenv(f"{ENV}_VALKEY_PORT"))

VALKEY_USERNAME = os.getenv(
    f"{ENV}_VALKEY_USERNAME",
    ""
)

VALKEY_PASSWORD = os.getenv(
    f"{ENV}_VALKEY_PASSWORD",
    ""
)

VALKEY_KEY = "cuid_counter"

# Old value before reservation
OLD_COUNTER = 1

try:

    if VALKEY_USERNAME and VALKEY_PASSWORD:

        pool = BlockingConnectionPool(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            username=VALKEY_USERNAME,
            password=VALKEY_PASSWORD,
            db=0,
            max_connections=10,
            timeout=5,
            decode_responses=True
        )

        client = valkey.Valkey(
            connection_pool=pool
        )

    else:

        client = valkey.Valkey(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            decode_responses=True
        )

    current_value = client.get(VALKEY_KEY)

    print(f"Current Counter : {current_value}")

    client.set(
        VALKEY_KEY,
        OLD_COUNTER
    )

    new_value = client.get(VALKEY_KEY)

    print(f"Counter Reset To : {new_value}")

except Exception as e:

    print(f"Failed : {e}")