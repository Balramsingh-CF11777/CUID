import os
import time
import uuid
import logging
import traceback
import sys
from datetime import datetime

import clickhouse_connect
import valkey
from valkey.connection import BlockingConnectionPool
from dotenv import load_dotenv
from retrying import retry


# ============================================================
# LOAD ENV
# ============================================================
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# SOURCE CLICKHOUSE
# ============================================================
SRC_HOST = os.getenv("SRC_CLICKHOUSE_HOST")
SRC_PORT = int(os.getenv("SRC_CLICKHOUSE_PORT"))
SRC_USER = os.getenv("SRC_CLICKHOUSE_USER")
SRC_PASSWORD = os.getenv("SRC_CLICKHOUSE_PASSWORD", "")
SRC_DB = os.getenv("SRC_CLICKHOUSE_DATABASE")
SRC_TABLE = os.getenv("SRC_CLICKHOUSE_TABLE")

# ============================================================
# ENVIRONMENT
# ============================================================
ENV = os.getenv("ENVIRON", "UAT").upper()

# ============================================================
# DESTINATION CLICKHOUSE
# ============================================================
DEST_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
DEST_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
DEST_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
DEST_PASSWORD = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD", "")
DEST_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE")

TEMP_TABLE = os.getenv("TEMP_TABLE", "CSEL_D2C_LOS_TEMP")
FINAL_TABLE = os.getenv("FINAL_TABLE", "CSEL_D2C_LOS")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100000))
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
TEST_ROW_LIMIT = int(os.getenv("TEST_ROW_LIMIT", 100000))

# ============================================================
# VALKEY CONFIG
# ============================================================
VALKEY_HOST = os.getenv(f"{ENV}_VALKEY_HOST")
VALKEY_PORT = int(os.getenv(f"{ENV}_VALKEY_PORT"))
VALKEY_USERNAME = os.getenv(f"{ENV}_VALKEY_USERNAME", "")
VALKEY_PASSWORD = os.getenv(f"{ENV}_VALKEY_PASSWORD", "")
VALKEY_KEY = os.getenv("VALKEY_KEY", "cuid_counter")


# ============================================================
# VALKEY CLIENT
# ============================================================
def get_valkey_client():
    if ENV == "UAT":
        return valkey.Valkey(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            decode_responses=True
        )

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


def reserve_cuid_range(count):
    client = get_valkey_client()

    current_counter = client.get(VALKEY_KEY)
    logger.info(f"Current Valkey Counter : {current_counter}")

    end_id = int(client.incrby(VALKEY_KEY, count))
    start_id = end_id - count + 1

    logger.info(f"Reserved Range : {start_id} -> {end_id}")

    return start_id, end_id


# ============================================================
# RETRY WRAPPERS
# ============================================================
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def execute_with_retry(client, query):
    client.command(query)


@retry(stop_max_attempt_number=3, wait_fixed=5000)
def insert_with_retry(client, table_name, rows, column_names):
    client.insert(
        table_name,
        rows,
        column_names=column_names
    )


# ============================================================
# MAIN ETL
# ============================================================
def run_etl():

    start_time = time.time()
    job_start = datetime.now()

    job_id = f"CSEL_D2C_LOS_{uuid.uuid4()}"
    job_start_str = job_start.strftime("%Y-%m-%d %H:%M:%S")

    source_count = 0
    temp_count = 0
    inserted_repository = 0

    try:
        logger.info(f"🚀 Job Started : {job_id}")
        logger.info(f"🕒 Start Time : {job_start_str}")
        logger.info(f"🌐 ENVIRON : {ENV}")

        # =====================================================
        # CONNECT SOURCE
        # =====================================================
        logger.info("🔌 Connecting SOURCE ClickHouse...")

        src = clickhouse_connect.get_client(
            host=SRC_HOST,
            port=SRC_PORT,
            username=SRC_USER,
            password=SRC_PASSWORD,
            database=SRC_DB
        )

        # =====================================================
        # CONNECT DESTINATION
        # =====================================================
        logger.info("🔌 Connecting DESTINATION ClickHouse...")

        dest = clickhouse_connect.get_client(
            host=DEST_HOST,
            port=DEST_PORT,
            username=DEST_USER,
            password=DEST_PASSWORD,
            database=DEST_DB
        )

        # =====================================================
        # STEP 1 : TRUNCATE TEMP
        # =====================================================
        logger.info("🧹 Truncating TEMP table...")

        execute_with_retry(
            dest,
            f"TRUNCATE TABLE {DEST_DB}.{TEMP_TABLE}"
        )

        # =====================================================
        # STEP 2 : SOURCE COUNT
        # =====================================================
        count_query = f"""
            SELECT count()
            FROM {SRC_DB}.{SRC_TABLE}
        """

        source_count = src.query(count_query).result_rows[0][0]

        if TEST_MODE:
            source_count = min(source_count, TEST_ROW_LIMIT)
            logger.info(f"🧪 TEST MODE ENABLED : {source_count}")

        logger.info(f"📊 Source Count : {source_count}")

        # =====================================================
        # STEP 3 : LOAD TEMP
        # =====================================================
        offset = 0
        total_loaded = 0

        temp_columns = [
            "PAN_NO",
            "AADHAR_NO",
            "MOBILE_NO",
            "EMAIL_ID",
            "APPLICATIONNO",
            "CIF_NO",
            "CUSTID",
            "LEAD_ID",
            "LEAD_STATUS",
            "CREATED_DATE",
            "SOURCE_BASED_LAST_UPDATED",
            "DATE_OF_BIRTH"
        ]

        while True:

            if TEST_MODE and total_loaded >= TEST_ROW_LIMIT:
                break

            limit_clause = BATCH_SIZE

            if TEST_MODE:
                remaining = TEST_ROW_LIMIT - total_loaded
                limit_clause = min(BATCH_SIZE, remaining)

            query = f"""
                SELECT
                    upper(trimBoth(toString(pan_no))) AS PAN_NO,
                    trimBoth(toString(aadhaar_no)) AS AADHAR_NO,
                    trimBoth(toString(mobile)) AS MOBILE_NO,
                    lower(trimBoth(toString(personal_email))) AS EMAIL_ID,
                    trimBoth(toString(lead_id)) AS APPLICATIONNO,
                    trimBoth(toString(cif_no)) AS CIF_NO,
                    trimBoth(toString(customer_id)) AS CUSTID,
                    trimBoth(toString(lead_id)) AS LEAD_ID,
                    trimBoth(toString(status)) AS LEAD_STATUS,

                    ifNull(
                        formatDateTime(
                            parseDateTimeBestEffortOrNull(toString(created_date)),
                            '%Y-%m-%d %H:%i:%S'
                        ),
                        ''
                    ) AS CREATED_DATE,

                    ifNull(
                        formatDateTime(
                            parseDateTimeBestEffortOrNull(toString(modified_date)),
                            '%Y-%m-%d %H:%i:%S'
                        ),
                        ''
                    ) AS SOURCE_BASED_LAST_UPDATED,

                    trimBoth(toString(dob)) AS DATE_OF_BIRTH

                FROM {SRC_DB}.{SRC_TABLE}
                LIMIT {limit_clause}
                OFFSET {offset}
            """

            rows = src.query(query).result_rows

            if not rows:
                break

            clean_rows = [
                tuple("" if v is None else str(v) for v in row)
                for row in rows
            ]

            insert_with_retry(
                dest,
                TEMP_TABLE,
                clean_rows,
                temp_columns
            )

            batch_count = len(clean_rows)
            total_loaded += batch_count
            offset += batch_count

            logger.info(
                f"➡ Loaded Batch : {batch_count} | "
                f"Total Loaded : {total_loaded}"
            )

        logger.info("✅ TEMP Load Completed")

        # =====================================================
        # STEP 4 : TEMP COUNT
        # =====================================================
        temp_count = dest.query(
            f"SELECT count() FROM {DEST_DB}.{TEMP_TABLE}"
        ).result_rows[0][0]

        logger.info(f"📦 TEMP Count : {temp_count}")

        # =====================================================
        # STEP 5 : INSERT INTO FINAL (new rows and updated rows)
        # =====================================================
        final_existing_count = dest.query(
            f"SELECT count() FROM {DEST_DB}.{FINAL_TABLE}"
        ).result_rows[0][0]

        if final_existing_count == 0:
            logger.info(f"🚀 Initial Load into {FINAL_TABLE}")

            execute_with_retry(dest, f"""
                INSERT INTO {DEST_DB}.{FINAL_TABLE}
                (
                    PAN_NO,
                    AADHAR_NO,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,
                    CIF_NO,
                    CUSTID,
                    LEAD_ID,
                    LEAD_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    DATE_OF_BIRTH,
                    checksum1,
                    checksum2
                )
                SELECT
                    upper(trimBoth(PAN_NO)) AS PAN_NO,
                    AADHAR_NO,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,
                    CIF_NO,
                    CUSTID,
                    LEAD_ID,
                    LEAD_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    DATE_OF_BIRTH,
                    checksum1,
                    checksum2
                FROM {DEST_DB}.{TEMP_TABLE}
                WHERE
                    PAN_NO IS NOT NULL
                    AND trimBoth(PAN_NO) != ''
                    AND match(
                        upper(trimBoth(PAN_NO)),
                        '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                    )
            """)
        else:
            logger.info(f"🔄 Incremental Load into {FINAL_TABLE}")

            execute_with_retry(dest, f"""
                INSERT INTO {DEST_DB}.{FINAL_TABLE}
                (
                    PAN_NO,
                    AADHAR_NO,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,
                    CIF_NO,
                    CUSTID,
                    LEAD_ID,
                    LEAD_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    DATE_OF_BIRTH,
                    checksum1,
                    checksum2
                )
                SELECT
                    upper(trimBoth(t.PAN_NO)) AS PAN_NO,
                    t.AADHAR_NO,
                    t.MOBILE_NO,
                    t.EMAIL_ID,
                    t.APPLICATIONNO,
                    t.CIF_NO,
                    t.CUSTID,
                    t.LEAD_ID,
                    t.LEAD_STATUS,
                    t.CREATED_DATE,
                    t.SOURCE_BASED_LAST_UPDATED,
                    t.DATE_OF_BIRTH,
                    t.checksum1,
                    t.checksum2
                FROM {DEST_DB}.{TEMP_TABLE} t
                LEFT JOIN
                (
                    SELECT
                        checksum1,
                        argMax(checksum2, version) AS checksum2
                    FROM {DEST_DB}.{FINAL_TABLE}
                    GROUP BY checksum1
                ) m
                    ON t.checksum1 = m.checksum1
                WHERE
                    (
                        m.checksum1 IS NULL
                        OR t.checksum2 != m.checksum2
                    )
                    AND t.PAN_NO IS NOT NULL
                    AND trimBoth(t.PAN_NO) != ''
                    AND match(
                        upper(trimBoth(t.PAN_NO)),
                        '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                    )
            """)

        logger.info("⚡ Optimizing FINAL TABLE")

        # execute_with_retry(
        #     dest,
        #     f"OPTIMIZE TABLE {DEST_DB}.{FINAL_TABLE} FINAL"
        # )

        # =====================================================
        # STEP 6 : CUID REPOSITORY - VALKEY BASED
        # =====================================================
        logger.info("🆔 Preparing PAN staging table...")

        execute_with_retry(
            dest,
            "TRUNCATE TABLE CUID.CUID_NEW_PAN_STAGE"
        )

        execute_with_retry(dest, f"""
            INSERT INTO CUID.CUID_NEW_PAN_STAGE
            SELECT DISTINCT
                upper(trimBoth(PAN_NO)) AS PAN_NO
            FROM {DEST_DB}.{FINAL_TABLE}
            FINAL
            WHERE
                PAN_NO IS NOT NULL
                AND trimBoth(PAN_NO) != ''
                AND match(
                    upper(trimBoth(PAN_NO)),
                    '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                )
                AND upper(trimBoth(PAN_NO)) NOT IN
                (
                    SELECT PAN_NO
                    FROM CUID.CUID_REPOSITORY
                )
        """)

        pending_repository = dest.query("""
            SELECT count()
            FROM CUID.CUID_NEW_PAN_STAGE
        """).result_rows[0][0]

        logger.info(f"Pending PANs : {pending_repository}")

        inserted_repository = 0

        if pending_repository > 0:

            start_id, end_id = reserve_cuid_range(
                pending_repository
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

            execute_with_retry(
                dest,
                repository_sql
            )

            inserted_repository = dest.query("""
                SELECT count()
                FROM
                (
                    SELECT DISTINCT PAN_NO
                    FROM CUID.CUID_NEW_PAN_STAGE
                ) s

                INNER JOIN CUID.CUID_REPOSITORY r
                    ON s.PAN_NO = r.PAN_NO
            """).result_rows[0][0]

        logger.info(f"Repository Inserts : {inserted_repository}")

        gap_count = pending_repository - inserted_repository

        logger.info(
            f"Pending={pending_repository}, "
            f"Inserted={inserted_repository}, "
            f"Gap={gap_count}"
        )

        if gap_count > 0:
            logger.warning(
                f"{gap_count} PANs reserved but not inserted."
            )

        # =====================================================
        # STEP 7 : INSERT MASTER TABLE
        # =====================================================
        logger.info("📦 Loading MASTER TABLE")

        execute_with_retry(dest, f"""
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
                'CUSTOMER' AS TYPE,
                'CSEL_D2C_LOS' AS SOURCE,

                upper(trimBoth(p.PAN_NO)) AS PAN_NO,
                '' AS NAME,

                '' AS GST_NO,
                ifNull(p.AADHAR_NO, '') AS AADHAAR_NO,
                '' AS CKYCID,
                '' AS TIN_NO,
                '' AS VOTER_ID,
                '' AS DRIVING_LICN_NO,
                '' AS RATIONCARD_NO,
                '' AS PASSPORT,

                ifNull(p.MOBILE_NO, '') AS MOBILE_NO,
                ifNull(p.EMAIL_ID, '') AS EMAIL_ID,

                ifNull(p.APPLICATIONNO, '') AS APPLICATIONNO,

                '' AS COAPPLICANT_PAN_NO,
                '' AS GUARANTOR_PAN_NO,

                ifNull(p.CIF_NO, '') AS CIF_NO,
                '' AS AGREEMENTNO,
                '' AS CUST_UNQ_ID,

                ifNull(p.CUSTID, '') AS CUSTID,

                '' AS VENDOR_ID,
                '' AS SOURCE_BASED_STATUS,
                '' AS REGISTRATION_CERTIFICATE,
                '' AS NPA_STAGEID,

                ifNull(p.LEAD_ID, '') AS LEAD_ID,
                ifNull(p.LEAD_STATUS, '') AS LEAD_STATUS,

                ifNull(p.CREATED_DATE, '') AS CREATED_DATE,
                ifNull(p.SOURCE_BASED_LAST_UPDATED, '') AS SOURCE_BASED_LAST_UPDATED,

                '' AS ASSET_VALUE,
                '' AS DATE_OF_INCORPORATION,
                '' AS USER_EMPLOYEE_NO,

                ifNull(p.DATE_OF_BIRTH, '') AS DATE_OF_BIRTH,

                '' AS DATE_OF_JOINING,
                '' AS ALLOCATION_REGION_CODE,
                '' AS ALLOCATION_REGION_TYPE,

                r.CUID,
                r.CUID_CREATION_DATE,

                'Loaded from CSEL_D2C_LOS' AS REMARKS

            FROM {DEST_DB}.{FINAL_TABLE} p

            INNER JOIN CUID.CUID_REPOSITORY r
                ON upper(trimBoth(p.PAN_NO)) = upper(trimBoth(r.PAN_NO))

            WHERE
                p.PAN_NO IS NOT NULL
                AND trimBoth(p.PAN_NO) != ''
                AND match(
                    upper(trimBoth(p.PAN_NO)),
                    '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                )
                AND p.LastUpdated >= today()
                AND p.LastUpdated < today() + 1
        """)

        # =====================================================
        # STEP 8 : AUDIT LOG
        # =====================================================
        duration_minutes = round(
            (time.time() - start_time) / 60,
            2
        )

        execute_with_retry(dest, f"""
            INSERT INTO CUID.ETL_AUDIT_LOG
            VALUES
            (
                '{job_id}',
                'CSEL_D2C_LOS',
                toDateTime('{job_start_str}'),
                now(),
                {source_count},
                {temp_count},
                0,
                {inserted_repository},
                'SUCCESS',
                ''
            )
        """)

        logger.info("🎯 CSEL_D2C_LOS ETL COMPLETED")
        logger.info(f"⏱ Duration : {duration_minutes} Minutes")

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_number = exc_traceback.tb_lineno if exc_traceback else "Unknown"

        logger.error(f"❌ ETL FAILED at line {line_number}: {e}")
        logger.error(traceback.format_exc())


# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    run_etl()