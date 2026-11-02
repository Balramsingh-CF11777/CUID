import os
import logging
import clickhouse_connect
import valkey

from dotenv import load_dotenv
from valkey.connection import BlockingConnectionPool

# ============================================================
# LOAD ENV
# ============================================================

load_dotenv()

# ============================================================
# LOGGING CONFIG
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# ============================================================
# ENV
# ============================================================

ENV = os.getenv("ENVIRON", "UAT").upper()

# ============================================================
# CLICKHOUSE CONFIG
# ============================================================

CLICKHOUSE_HOST = os.getenv(
    f"{ENV}_CUID_CLICKHOUSE_HOST"
)

CLICKHOUSE_PORT = int(
    os.getenv(
        f"{ENV}_CUID_CLICKHOUSE_PORT",
        8123
    )
)

CLICKHOUSE_USER = os.getenv(
    f"{ENV}_CUID_CLICKHOUSE_USER",
    ""
)

CLICKHOUSE_PASSWORD = os.getenv(
    f"{ENV}_CUID_CLICKHOUSE_PASSWORD",
    ""
)

CLICKHOUSE_DATABASE = os.getenv(
    f"{ENV}_CUID_CLICKHOUSE_DATABASE",
    "CUID"
)

# ============================================================
# VALKEY CONFIG
# ============================================================

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

# ============================================================
# VALKEY CONNECTION
# ============================================================

def get_valkey_client():

    if ENV == "UAT":

        return valkey.Valkey(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            decode_responses=True
        )

    pool_kwargs = {
        "host": VALKEY_HOST,
        "port": VALKEY_PORT,
        "db": 0,
        "max_connections": 100,
        "timeout": 5,
        "decode_responses": True
    }

    if VALKEY_USERNAME:
        pool_kwargs["username"] = VALKEY_USERNAME

    if VALKEY_PASSWORD:
        pool_kwargs["password"] = VALKEY_PASSWORD

    pool = BlockingConnectionPool(**pool_kwargs)

    return valkey.Valkey(
        connection_pool=pool
    )

# ============================================================
# RESERVE CUID RANGE
# ============================================================

def reserve_cuid_range(count):

    client = get_valkey_client()

    current_counter = client.get(VALKEY_KEY)

    logger.info(
        f"Current Valkey Counter : {current_counter}"
    )

    end_id = int(
        client.incrby(
            VALKEY_KEY,
            count
        )
    )

    start_id = end_id - count + 1

    logger.info(
        f"Reserved Range : {start_id} -> {end_id}"
    )

    return start_id, end_id

# ============================================================
# CLICKHOUSE CONNECTION
# ============================================================

client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE
)

# ============================================================
# COUNT QUERY
# ============================================================

COUNT_QUERY = """
SELECT count()
FROM
(
    SELECT DISTINCT
        upper(trim(split_pan)) AS PAN_NO

    FROM CUID.CUID_MASTER_TABLE

    ARRAY JOIN arrayConcat(
        splitByRegexp(
            '[,|/;\\\\s]+',
            assumeNotNull(ifNull(COAPPLICANT_PAN_NO, ''))
        ),

        splitByRegexp(
            '[,|/;\\\\s]+',
            assumeNotNull(ifNull(GUARANTOR_PAN_NO, ''))
        )
    ) AS split_pan

    WHERE length(trim(split_pan)) = 10

    AND match(
        upper(trim(split_pan)),
        '^[A-Z]{5}[0-9]{4}[A-Z]$'
    )

    AND upper(trim(split_pan)) NOT IN
    (
        SELECT PAN_NO
        FROM CUID.CUID_REPOSITORY
    )
)
"""

# ============================================================
# MAIN EXECUTION
# ============================================================

try:

    logger.info("🔌 Connecting ClickHouse...")
    logger.info(f"🏢 Database : {CLICKHOUSE_DATABASE}")
    logger.info(f"🌍 Environment : {ENV}")

    # --------------------------------------------------------
    # CHECK NEW PAN COUNT
    # --------------------------------------------------------

    logger.info("📊 Checking New PAN Count...")

    result = client.query(COUNT_QUERY)

    new_pan_count = result.result_rows[0][0]

    logger.info(
        f"✅ New PAN Count : {new_pan_count}"
    )

    if new_pan_count == 0:

        logger.info(
            "⚠️ No New PANs Found. Skipping Insert."
        )

    else:

        # ----------------------------------------------------
        # RESERVE VALKEY RANGE
        # ----------------------------------------------------

        start_id, end_id = reserve_cuid_range(
            new_pan_count
        )

        logger.info(
            f"🔒 Reserved CUID Range : "
            f"{start_id} -> {end_id}"
        )

        # ----------------------------------------------------
        # INSERT REPOSITORY
        # ----------------------------------------------------

        INSERT_QUERY = f"""
        INSERT INTO CUID.CUID_REPOSITORY
        (
            PAN_NO,
            CUID,
            CUID_CREATION_DATE
        )
        SELECT
            PAN_NO,

            lpad(
                toString(
                    row_number()
                    OVER (ORDER BY PAN_NO)
                    + {start_id} - 1
                ),
                16,
                '0'
            ) AS CUID,

            toString(now()) AS CUID_CREATION_DATE

        FROM
        (
            SELECT DISTINCT
                upper(trim(split_pan)) AS PAN_NO

            FROM CUID.CUID_MASTER_TABLE

            ARRAY JOIN arrayConcat(
                splitByRegexp(
                    '[,|/;\\\\s]+',
                    assumeNotNull(
                        ifNull(
                            COAPPLICANT_PAN_NO,
                            ''
                        )
                    )
                ),

                splitByRegexp(
                    '[,|/;\\\\s]+',
                    assumeNotNull(
                        ifNull(
                            GUARANTOR_PAN_NO,
                            ''
                        )
                    )
                )
            ) AS split_pan

            WHERE length(trim(split_pan)) = 10

            AND match(
                upper(trim(split_pan)),
                '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
            )

            AND upper(trim(split_pan)) NOT IN
            (
                SELECT PAN_NO
                FROM CUID.CUID_REPOSITORY
            )
        ) t
        """

        logger.info(
            "🚀 Starting CUID Generation Process..."
        )

        client.command(INSERT_QUERY)

        logger.info(
            "✅ Repository Insert Completed"
        )

        # ----------------------------------------------------
        # VALIDATION
        # ----------------------------------------------------

        inserted_repository = client.query(f"""
        SELECT count()
        FROM CUID.CUID_REPOSITORY
        WHERE toUInt64(CUID)
            BETWEEN {start_id} AND {end_id}
        """).result_rows[0][0]

        gap_count = new_pan_count - inserted_repository

        logger.info(
            f"Pending={new_pan_count}, "
            f"Inserted={inserted_repository}, "
            f"Gap={gap_count}"
        )

        if gap_count > 0:

            logger.warning(
                f"⚠ {gap_count} PANs reserved "
                f"but not inserted."
            )

except Exception as e:

    logger.exception(
        f"❌ Process Failed : {str(e)}"
    )

finally:

    client.close()

    logger.info(
        "🔌 ClickHouse Connection Closed"
    )