import valkey
from valkey.connection import BlockingConnectionPool
import os
from dotenv import load_dotenv

load_dotenv()

valkey_host = os.getenv("VALKEY_HOST")
valkey_port = os.getenv("VALKEY_PORT")
valkey_username = os.getenv("VALKEY_USERNAME")
valkey_password = os.getenv("VALKEY_PASSWORD")

def get_valkey_client():
    try:
        # pool = BlockingConnectionPool(
        #     host=valkey_host,
        #     port=int(valkey_port),
        #     db=0,
        #     max_connections=100,
        #     timeout=5,
        #     decode_responses=True
        # )


        pool = BlockingConnectionPool(
            host=valkey_host,
            port=int(valkey_port),
            username=valkey_username,   # Optional for Redis 6+/Valkey ACL users
            password=valkey_password,   # Password
            db=0,
            max_connections=100,
            timeout=5,
            decode_responses=True
        )

        

        client = valkey.Valkey(connection_pool=pool)
        # client.ping()
        return client

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None


def update_cuid_counter(new_value):
    """
    Updates the cuid_counter in Valkey.

    Args:
        new_value (int/str): Value to set

    Returns:
        dict: old and new values
    """
    client = get_valkey_client()


    if not client:
        return None

    try:
        old_value = client.get("cuid_counter")

        print(old_value)

    #     client.set("cuid_counter", new_value)

    #     updated_value = client.get("cuid_counter")
        

    #     return {
    #         "old_value": old_value,
    #         "new_value": updated_value
    #     }

    except Exception as e:
        print(f"❌ Update failed: {e}")
        return None


# Example usage
if __name__ == "__main__":

    result = update_cuid_counter(22773149)
    print(result)