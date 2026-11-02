import os
import psycopg2
import clickhouse_connect
from dotenv import load_dotenv
import logging
import time
from datetime import datetime
from retrying import retry
import uuid
import valkey
from valkey.connection import BlockingConnectionPool

# ============================================================
# ENV LOAD
# ============================================================
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ENV = os.getenv("ENVIRON", "UAT").upper()

# # ---------------- POSTGRES ----------------
PG_HOST = os.getenv(f"{ENV}_POSIDEX_HOST")
PG_PORT = int(os.getenv(f"{ENV}_POSIDEX_PORT"))
PG_DB = os.getenv(f"{ENV}_POSIDEX_DATABASE")
PG_USER = os.getenv(f"{ENV}_POSIDEX_USERNAME")
PG_PASS = os.getenv(f"{ENV}_POSIDEX_PASSWORD")

# ---------------- POSTGRES ----------------
# PG_HOST = os.getenv(f"DR_POSIDEX_HOST")
# PG_PORT = int(os.getenv(f"DR_POSIDEX_PORT"))
# PG_DB = os.getenv(f"DR_POSIDEX_DATABASE")
# PG_USER = os.getenv(f"DR_POSIDEX_USERNAME")
# PG_PASS = os.getenv(f"DR_POSIDEX_PASSWORD")

# ---------------- CLICKHOUSE --------------
CH_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
CH_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
CH_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
CH_PASS = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD")
CH_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE")


TEMP_TABLE = "idnty_dg_blk_trg_sec_temp"
FINAL_TABLE = "idnty_dg_blk_trg_sec"

BATCH_SIZE = 500000

# ============================================================
# VALKEY CONFIG
# ============================================================

VALKEY_HOST = os.getenv(
    f"{ENV}_VALKEY_HOST"
)

VALKEY_PORT = int(
    os.getenv(f"{ENV}_VALKEY_PORT")
)

VALKEY_USERNAME = os.getenv(
    f"{ENV}_VALKEY_USERNAME",
    ""
)

VALKEY_PASSWORD = os.getenv(
    f"{ENV}_VALKEY_PASSWORD",
    ""
)

VALKEY_KEY = "cuid_counter"


def get_valkey_client():

    if ENV == "UAT":

        return valkey.Valkey(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            decode_responses=True
        )

    elif ENV == "PROD" or ENV == "DR":

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

        return valkey.Valkey(
            connection_pool=pool
        )

    raise ValueError(
        f"Unsupported ENVIRON value: {ENV}"
    )

def reserve_cuid_range(count):

    client = get_valkey_client()

    current_counter = client.get(
        VALKEY_KEY
    )

    logger.info(
        f"Current Valkey Counter: "
        f"{current_counter}"
    )

    end_id = int(
        client.incrby(
            VALKEY_KEY,
            count
        )
    )

    start_id = (
        end_id - count + 1
    )

    logger.info(
        f"Reserved Range: "
        f"{start_id} -> {end_id}"
    )

    return start_id, end_id

# ============================================================
# RETRY DECORATOR
# ============================================================
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def execute_with_retry(client, query):
    client.command(query)

# ============================================================
# MAIN ETL
# ============================================================
def run_etl():
    start_time = time.time()
    job_start = datetime.now()
    job_id = f"POSIDEX_{uuid.uuid4()}"

    source_count = 0
    temp_count = 0
    inserted_master = 0
    inserted_repository = 0

    try:
        logger.info("🔌 Connecting to PostgreSQL...")
        pg_conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASS
        )

        logger.info("🔌 Connecting to ClickHouse...")
        ch = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASS,
            database=CH_DB
        )

        # =====================================================
        # STEP 1: TRUNCATE TEMP
        # =====================================================
        logger.info("🧹 Truncating temp table...")
        execute_with_retry(ch, f"TRUNCATE TABLE {CH_DB}.{TEMP_TABLE}")

        # =====================================================
        # STEP 2: COUNT SOURCE
        # =====================================================
        count_cursor = pg_conn.cursor()
        count_query = """
            SELECT count(*)
            FROM (
                SELECT
                    "UCIC_NUMBER"        AS CIF_NO,
                    "PAN"                AS PAN_NO,
                    "CUSTOMER_NAME"      AS NAME,
                    "AADHAAR_NO",
                    "ckycid"             AS CKYCID,
                    "TIN_NO",
                    "VOTER_ID",
                    "DRIVING_LICN_NO",
                    "RATIONCARD_NO",
                    "PASSPORT",
                    "REGISTRATION_CERTIFICATE",
                    "DATE_OF_INCORPORATION",
                    "DOB"                AS DATE_OF_BIRTH,
                    "CUST_UNQ_ID",
                    "CUSTID"
                FROM public.idnty_dg_blk_trg_sec)
        """
        count_cursor.execute(count_query)
        source_count = count_cursor.fetchone()[0]
        count_cursor.close()

        logger.info(f"📊 Source valid PAN count: {source_count}")

        # =====================================================
        # STEP 3: STREAM DATA
        # =====================================================
        pg_cursor = pg_conn.cursor(name="POSIDEX_stream")

        select_query = """
            SELECT
                "UCIC_NUMBER",
                "CUST_UNQ_ID",
                "CUSTID",
                "CUSTOMER_NAME",
                "DOB",
                "DATE_OF_INCORPORATION",
                "PAN",
                "AADHAAR_NO",
                "ckycid",
                "TIN_NO",
                "VOTER_ID",
                "DRIVING_LICN_NO",
                "RATIONCARD_NO",
                "PASSPORT",
                "REGISTRATION_CERTIFICATE"
            FROM public.idnty_dg_blk_trg_sec
        """

        pg_cursor.execute(select_query)

        columns = [
            "UCIC_NUMBER",
            "CUST_UNQ_ID",
            "CUSTID",
            "CUSTOMER_NAME",
            "DOB",
            "DATE_OF_INCORPORATION",
            "PAN",
            "AADHAAR_NO",
            "ckycid",
            "TIN_NO",
            "VOTER_ID",
            "DRIVING_LICN_NO",
            "RATIONCARD_NO",
            "PASSPORT",
            "REGISTRATION_CERTIFICATE"
        ]

        total = 0

        while True:
            rows = pg_cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break

            cleaned = [
                [str(col) if col is not None else None for col in row]
                for row in rows
            ]

            ch.insert(
                table=TEMP_TABLE,
                data=cleaned,
                column_names=columns
            )

            total += len(cleaned)
            logger.info(f"➡ Inserted to temp: {total}")

        pg_cursor.close()
        pg_conn.close()

        logger.info("✅ Temp load completed")

        temp_count = ch.query(
            f"SELECT count() FROM {CH_DB}.{TEMP_TABLE}"
        ).result_rows[0][0]

        # =====================================================
        # STEP 4: INSERT INTO FINAL TABLE
        # =====================================================
        logger.info("🔄 Loading into final table...")

        # Check if FINAL table has data
        final_count = ch.query(
            f"SELECT count() FROM {CH_DB}.{FINAL_TABLE}"
        ).result_rows[0][0]

        if final_count == 0:
            logger.info("🆕 Initial load detected - performing full insert...")

            execute_with_retry(ch, f"""
                INSERT INTO {CH_DB}.{FINAL_TABLE}
                (
                    UCIC_NUMBER,
                    CUST_UNQ_ID,
                    CUSTID,
                    CUSTOMER_NAME,
                    DOB,
                    DATE_OF_INCORPORATION,
                    PAN,
                    AADHAAR_NO,
                    ckycid,
                    TIN_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    RATIONCARD_NO,
                    PASSPORT,
                    REGISTRATION_CERTIFICATE,
                    checksum1,
                    checksum2
                )
                SELECT
                    UCIC_NUMBER,
                    CUST_UNQ_ID,
                    CUSTID,
                    CUSTOMER_NAME,
                    DOB,
                    DATE_OF_INCORPORATION,
                    PAN,
                    AADHAAR_NO,
                    ckycid,
                    TIN_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    RATIONCARD_NO,
                    PASSPORT,
                    REGISTRATION_CERTIFICATE,
                    checksum1,
                    checksum2
                FROM {CH_DB}.{TEMP_TABLE}
            """)

        else:
            logger.info("♻ Incremental load detected - inserting new/changed records...")

            execute_with_retry(ch, f"""
                INSERT INTO {CH_DB}.{FINAL_TABLE}
                    (
                        UCIC_NUMBER,
                        CUST_UNQ_ID,
                        CUSTID,
                        CUSTOMER_NAME,
                        DOB,
                        DATE_OF_INCORPORATION,
                        PAN,
                        AADHAAR_NO,
                        ckycid,
                        TIN_NO,
                        VOTER_ID,
                        DRIVING_LICN_NO,
                        RATIONCARD_NO,
                        PASSPORT,
                        REGISTRATION_CERTIFICATE,
                        checksum1,
                        checksum2
                    )
                    SELECT
                        t.UCIC_NUMBER,
                        t.CUST_UNQ_ID,
                        t.CUSTID,
                        t.CUSTOMER_NAME,
                        t.DOB,
                        t.DATE_OF_INCORPORATION,
                        t.PAN,
                        t.AADHAAR_NO,
                        t.ckycid,
                        t.TIN_NO,
                        t.VOTER_ID,
                        t.DRIVING_LICN_NO,
                        t.RATIONCARD_NO,
                        t.PASSPORT,
                        t.REGISTRATION_CERTIFICATE,
                        t.checksum1,
                        t.checksum2
                    FROM {CH_DB}.{TEMP_TABLE} t
                    LEFT JOIN
                    (
                        SELECT
                            checksum1,
                            argMax(checksum2, version) AS checksum2
                        FROM {CH_DB}.{FINAL_TABLE}
                        GROUP BY checksum1
                    ) m
                    ON t.checksum1 = m.checksum1
                    WHERE
                        m.checksum1 IS NULL
                        OR t.checksum2 != m.checksum2
            """)

        # execute_with_retry(ch, f"OPTIMIZE TABLE {CH_DB}.{FINAL_TABLE} FINAL")

        inserted_master = ch.query(f"""
            SELECT countDistinct(checksum1)
            FROM {CH_DB}.{FINAL_TABLE}
            WHERE LastUpdated >= toDateTime('{job_start.strftime("%Y-%m-%d %H:%M:%S")}')
        """).result_rows[0][0]

        start_id = None
        end_id = None
        pending_repository = 0

        # =====================================================
        # STEP 5: CUID REPOSITORY
        # =====================================================
        logger.info("🆔 Preparing PAN staging table...")

        execute_with_retry(
            ch,
            "TRUNCATE TABLE CUID.CUID_NEW_PAN_STAGE"
        )

        execute_with_retry(ch, f"""
        INSERT INTO CUID.CUID_NEW_PAN_STAGE
        SELECT DISTINCT
            upper(trimBoth(PAN))
        FROM {CH_DB}.{FINAL_TABLE}
        FINAL
        WHERE
            PAN IS NOT NULL
            AND trimBoth(PAN) != ''
            AND match(
                upper(trimBoth(PAN)),
                '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
            )
            AND upper(trimBoth(PAN)) NOT IN
            (
                SELECT PAN_NO
                FROM CUID.CUID_REPOSITORY
            )
        """)

        pending_repository = ch.query("""
        SELECT count()
        FROM CUID.CUID_NEW_PAN_STAGE
        """).result_rows[0][0]

        logger.info(
            f"Pending PANs : {pending_repository}"
        )

        if pending_repository > 0:

            start_id, end_id = reserve_cuid_range(
                pending_repository
            )

            logger.info(
                f"Reserved Range : "
                f"{start_id} -> {end_id}"
            )

            repository_sql = f"""
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

                    now() AS CUID_CREATION_DATE

                FROM
                (
                    SELECT DISTINCT s.PAN_NO
                    FROM CUID.CUID_NEW_PAN_STAGE s
                    LEFT ANTI JOIN CUID.CUID_REPOSITORY r
                        ON s.PAN_NO = r.PAN_NO
                ) t
                """

            # logger.info(repository_sql)

            result = ch.command(repository_sql)

            # logger.info(f"Repository insert result: {result}")

            verify_count = ch.query(f"""
                SELECT count()
                FROM CUID.CUID_REPOSITORY
                WHERE toUInt64(CUID)
                BETWEEN {start_id} AND {end_id}
                """).result_rows[0][0]

            logger.info(
                f"Verification Count After Insert : "
                f"{verify_count}"
            )

            inserted_repository = verify_count

        else:

            inserted_repository = 0

        logger.info(
            f"Repository Inserts : "
            f"{inserted_repository}"
        )

        gap_count = (
            pending_repository
            - inserted_repository
        )

        logger.info(
            f"Pending={pending_repository}, "
            f"Inserted={inserted_repository}, "
            f"Gap={gap_count}"
        )

        if gap_count > 0:

            logger.warning(
                f"{gap_count} PANs reserved "
                f"but not inserted."
            )
        # =====================================================
        # STEP 6: MASTER INSERT
        # =====================================================
        logger.info("📦 Inserting into CUID_MASTER_TABLE...")

        execute_with_retry(ch, f"""
            INSERT INTO CUID.CUID_MASTER_TABLE
            (
                TYPE,
                SOURCE,
                PAN_NO,
                NAME,
                GST_NO,
                AADHAAR_NO,
                CKYCID,
                TIN_NO,
                VOTER_ID,
                DRIVING_LICN_NO,
                RATIONCARD_NO,
                PASSPORT,
                MOBILE_NO,
                EMAIL_ID,
                APPLICATIONNO,
                COAPPLICANT_PAN_NO,
                GUARANTOR_PAN_NO,
                CIF_NO,
                AGREEMENTNO,
                CUST_UNQ_ID,
                CUSTID,
                VENDOR_ID,
                SOURCE_BASED_STATUS,
                REGISTRATION_CERTIFICATE,
                NPA_STAGEID,
                LEAD_ID,
                LEAD_STATUS,
                CREATED_DATE,
                SOURCE_BASED_LAST_UPDATED,
                ASSET_VALUE,
                DATE_OF_INCORPORATION,
                USER_EMPLOYEE_NO,
                DATE_OF_BIRTH,
                DATE_OF_JOINING,
                ALLOCATION_REGION_CODE,
                ALLOCATION_REGION_TYPE,
                CUID,
                CUID_CREATION_DATE,
                REMARKS
            )

            SELECT

                'CUSTOMER',
                'POSIDEX',

                upper(trimBoth(p.PAN)),

                ifNull(p.CUSTOMER_NAME, ''),
                '',                                     -- GST_NO

                ifNull(p.AADHAAR_NO, ''),
                ifNull(p.ckycid, ''),
                ifNull(p.TIN_NO, ''),
                ifNull(p.VOTER_ID, ''),
                ifNull(p.DRIVING_LICN_NO, ''),
                ifNull(p.RATIONCARD_NO, ''),
                ifNull(p.PASSPORT, ''),

                '',                                     -- MOBILE_NO
                '',                                     -- EMAIL_ID
                '',                                     -- APPLICATIONNO
                '',                                     -- COAPPLICANT_PAN_NO
                '',                                     -- GUARANTOR_PAN_NO

                ifNull(p.UCIC_NUMBER, ''),              -- CIF_NO

                '',                                     -- AGREEMENTNO

                ifNull(p.CUST_UNQ_ID, ''),
                ifNull(p.CUSTID, ''),

                '',                                     -- VENDOR_ID
                '',                                     -- SOURCE_BASED_STATUS

                ifNull(p.REGISTRATION_CERTIFICATE, ''),

                '',                                     -- NPA_STAGEID
                '',                                     -- LEAD_ID
                '',                                     -- LEAD_STATUS
                '',                                     -- CREATED_DATE
                '',                                     -- SOURCE_BASED_LAST_UPDATED
                '',                                     -- ASSET_VALUE
                ifNull(p.DATE_OF_INCORPORATION, ''),
                '',                                     -- USER_EMPLOYEE_NO
                ifNull(p.DOB, ''),
                '',                                     -- DATE_OF_JOINING
                '',                                     -- ALLOCATION_REGION_CODE
                '',                                     -- ALLOCATION_REGION_TYPE
                r.CUID,
                r.CUID_CREATION_DATE,
                'Loaded from POSIDEX'

            FROM {CH_DB}.{FINAL_TABLE} p
            INNER JOIN CUID.CUID_REPOSITORY r
                ON upper(trimBoth(p.PAN))
                = upper(trimBoth(r.PAN_NO))
            WHERE
                p.PAN IS NOT NULL
                AND trim(p.PAN) != ''
                AND match(
                    upper(trim(p.PAN)),
                    '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                )
                AND p.LastUpdated >= today()
                AND p.LastUpdated < today() + 1
        """)

        # =====================================================
        # STEP 7: AUDIT LOG
        # =====================================================
        duration_minutes = round((time.time() - start_time) / 60, 2)

        execute_with_retry(ch, f"""
            INSERT INTO CUID.ETL_AUDIT_LOG
            VALUES
            (
                '{job_id}',
                'POSIDEX',
                toDateTime('{job_start.strftime("%Y-%m-%d %H:%M:%S")}'),
                now(),
                {int(source_count)},
                {int(temp_count)},
                {int(inserted_master)},
                {int(inserted_repository)},
                'SUCCESS',
                ''
            )
        """)

        # execute_with_retry(ch, """OPTIMIZE TABLE CUID.CUID_MASTER_TABLE FINAL;""")

        logger.info(f"🎯 ETL COMPLETED SUCCESSFULLY in {duration_minutes} minutes")

    except Exception as e:
        logger.error(f"❌ ETL FAILED: {e}")

# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    run_etl()
