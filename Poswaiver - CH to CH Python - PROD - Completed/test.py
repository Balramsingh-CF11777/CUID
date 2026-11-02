import valkey

try:
    client = valkey.Valkey(
        host="10.9.19.41",
        port=30379,
        decode_responses=True,
        socket_connect_timeout=5
    )

    print("Attempting GET...")

    value = client.get("cuid_counter")

    print("SUCCESS")
    print("cuid_counter =", value)

except Exception as e:
    print("FAILED")
    print(type(e).__name__)
    print(e)
 