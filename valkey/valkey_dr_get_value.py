import os
import sys

import clickhouse_connect
import valkey
from dotenv import load_dotenv
from valkey.connection import BlockingConnectionPool


# ==========================================================
# LOAD ENVIRONMENT VARIABLES
# ==========================================================
load_dotenv()


# ==========================================================
# SUPPORTED ENVIRONMENTS
# ==========================================================
SUPPORTED_ENVIRONMENTS = {"UAT", "PROD", "DR"}

ENV = os.getenv("ENVIRON", "UAT").strip().upper()

if ENV not in SUPPORTED_ENVIRONMENTS:
    raise ValueError(
        f"Invalid ENVIRON value: {ENV}. "
        f"Supported environments: {', '.join(sorted(SUPPORTED_ENVIRONMENTS))}"
    )

print("=" * 60)
print(f"Running Environment: {ENV}")
print("=" * 60)


# ==========================================================
# HELPER FUNCTIONS
# ==========================================================
def get_required_env(variable_name):
    """
    Read a mandatory environment variable.

    Raises:
        RuntimeError: If the variable is missing or empty.
    """
    value = os.getenv(variable_name)

    if value is None or not value.strip():
        raise RuntimeError(
            f"Required environment variable is missing: {variable_name}"
        )

    return value.strip()


def get_required_int_env(variable_name):
    """
    Read and validate an integer environment variable.
    """
    value = get_required_env(variable_name)

    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(
            f"Environment variable {variable_name} must be an integer. "
            f"Current value: {value}"
        ) from exc


# ==========================================================
# VALKEY CONFIGURATION
# ==========================================================
VALKEY_HOST = get_required_env(f"{ENV}_VALKEY_HOST")
VALKEY_PORT = get_required_int_env(f"{ENV}_VALKEY_PORT")

# Credentials are optional for environments such as UAT.
VALKEY_USERNAME = os.getenv(f"{ENV}_VALKEY_USERNAME")
VALKEY_PASSWORD = os.getenv(f"{ENV}_VALKEY_PASSWORD")

VALKEY_USERNAME = (
    VALKEY_USERNAME.strip()
    if VALKEY_USERNAME and VALKEY_USERNAME.strip()
    else None
)

VALKEY_PASSWORD = (
    VALKEY_PASSWORD.strip()
    if VALKEY_PASSWORD and VALKEY_PASSWORD.strip()
    else None
)


# ==========================================================
# CLICKHOUSE CONFIGURATION
# ==========================================================
CLICKHOUSE_HOST = get_required_env(
    f"{ENV}_CUID_CLICKHOUSE_HOST"
)

CLICKHOUSE_PORT = get_required_int_env(
    f"{ENV}_CUID_CLICKHOUSE_PORT"
)

CLICKHOUSE_USER = get_required_env(
    f"{ENV}_CUID_CLICKHOUSE_USER"
)

CLICKHOUSE_PASSWORD = get_required_env(
    f"{ENV}_CUID_CLICKHOUSE_PASSWORD"
)

CLICKHOUSE_DATABASE = get_required_env(
    f"{ENV}_CUID_CLICKHOUSE_DATABASE"
)


# ==========================================================
# CREATE VALKEY CLIENT
# ==========================================================
def get_valkey_client():
    """
    Create and validate a Valkey client.

    Returns:
        valkey.Valkey | None
    """
    try:
        pool_arguments = {
            "host": VALKEY_HOST,
            "port": VALKEY_PORT,
            "db": 0,
            "max_connections": 100,
            "timeout": 5,
            "decode_responses": True,
        }

        # Add credentials only when configured.
        if VALKEY_USERNAME:
            pool_arguments["username"] = VALKEY_USERNAME

        if VALKEY_PASSWORD:
            pool_arguments["password"] = VALKEY_PASSWORD

        pool = BlockingConnectionPool(**pool_arguments)

        client = valkey.Valkey(connection_pool=pool)

        # Verify the connection immediately.
        # client.ping()

        return client

    except Exception as exc:
        print(f"❌ Valkey connection failed: {exc}")
        return None


# ==========================================================
# CREATE CLICKHOUSE CLIENT
# ==========================================================
def get_clickhouse_client():
    """
    Create and validate a ClickHouse client.

    Returns:
        clickhouse_connect.driver.Client | None
    """
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
            connect_timeout=10,
            send_receive_timeout=30,
        )

        # Verify the connection.
        client.command("SELECT 1")

        return client

    except Exception as exc:
        print(f"❌ ClickHouse connection failed: {exc}")
        return None


# ==========================================================
# READ CUID COUNTER
# ==========================================================
def get_cuid_counter():
    """
    Read the current CUID counter from Valkey.
    """
    client = get_valkey_client()

    if client is None:
        print("❌ Unable to connect to Valkey")
        return None

    try:
        old_value = client.get("cuid_counter")

        print("\nValkey Connection")
        print("-" * 60)
        print(f"Environment : {ENV}")
        print(f"Host        : {VALKEY_HOST}")
        print(f"Port        : {VALKEY_PORT}")
        print(f"Username    : {VALKEY_USERNAME or 'Not configured'}")
        print(f"Old Value   : {old_value}")

        return {
            "environment": ENV,
            "host": VALKEY_HOST,
            "port": VALKEY_PORT,
            "old_value": old_value,
        }

    except Exception as exc:
        print(f"❌ Failed to read Valkey counter: {exc}")
        return None

    finally:
        try:
            client.close()
        except Exception:
            pass


# ==========================================================
# TEST CLICKHOUSE CONNECTION
# ==========================================================
def test_clickhouse_connection():
    """
    Test the selected ClickHouse environment and return
    basic server information.
    """
    client = get_clickhouse_client()

    if client is None:
        print("❌ Unable to connect to ClickHouse")
        return None

    try:
        result = client.query(
            """
            SELECT
                version() AS server_version,
                currentDatabase() AS current_database
            """
        )

        row = result.first_row

        server_version = row[0] if row else None
        current_database = row[1] if row else None

        print("\nClickHouse Connection")
        print("-" * 60)
        print(f"Environment      : {ENV}")
        print(f"Host             : {CLICKHOUSE_HOST}")
        print(f"Port             : {CLICKHOUSE_PORT}")
        print(f"Username         : {CLICKHOUSE_USER}")
        print(f"Configured DB    : {CLICKHOUSE_DATABASE}")
        print(f"Current DB       : {current_database}")
        print(f"Server Version   : {server_version}")

        return {
            "environment": ENV,
            "host": CLICKHOUSE_HOST,
            "port": CLICKHOUSE_PORT,
            "username": CLICKHOUSE_USER,
            "database": current_database,
            "server_version": server_version,
        }

    except Exception as exc:
        print(f"❌ Failed to query ClickHouse: {exc}")
        return None

    finally:
        try:
            client.close()
        except Exception:
            pass


# ==========================================================
# DISPLAY SELECTED CONFIGURATION
# ==========================================================
def display_selected_configuration():
    """
    Display non-sensitive selected configuration.
    Passwords are intentionally not printed.
    """
    print("\nSelected Configuration")
    print("-" * 60)
    print(f"Environment       : {ENV}")
    print(f"Valkey Host       : {VALKEY_HOST}")
    print(f"Valkey Port       : {VALKEY_PORT}")
    print(f"Valkey Auth       : {'Enabled' if VALKEY_PASSWORD else 'Disabled'}")
    print(f"ClickHouse Host   : {CLICKHOUSE_HOST}")
    print(f"ClickHouse Port   : {CLICKHOUSE_PORT}")
    print(f"ClickHouse User   : {CLICKHOUSE_USER}")
    print(f"ClickHouse DB     : {CLICKHOUSE_DATABASE}")


# ==========================================================
# MAIN
# ==========================================================
def main():
    display_selected_configuration()

    valkey_result = get_cuid_counter()
    clickhouse_result = test_clickhouse_connection()

    print("\nConnection Summary")
    print("-" * 60)
    print(
        "Valkey      : "
        f"{'✅ Connected' if valkey_result is not None else '❌ Failed'}"
    )
    print(
        "ClickHouse  : "
        f"{'✅ Connected' if clickhouse_result is not None else '❌ Failed'}"
    )

    if valkey_result is None or clickhouse_result is None:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())