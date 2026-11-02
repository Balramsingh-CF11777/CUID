import valkey

HOST = "10.9.52.72"
PORT = 30378
USERNAME = "valkeywrtusr"
PASSWORD = "Trtdun78"

try:
    client = valkey.Valkey(
        host=HOST,
        port=PORT,
        username=USERNAME,
        password=PASSWORD,
        decode_responses=True,
        socket_connect_timeout=5
    )

    print("Attempting GET...")

    value = client.get("cuid_counter")

    print("✅ SUCCESS")
    print("cuid_counter =", value)

except Exception as e:
    print("❌ FAILED")
    print("Exception Type:", type(e).__name__)
    print("Exception:", e)