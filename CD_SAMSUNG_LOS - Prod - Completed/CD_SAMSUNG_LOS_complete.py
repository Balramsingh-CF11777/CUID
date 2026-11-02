import os
import sys
import time
import uuid
import logging
import traceback
from datetime import datetime

import clickhouse_connect
import valkey
from valkey.connection import BlockingConnectionPool
from dotenv import load_dotenv
from retrying import retry


# ============================================================
# ENV LOAD
# ============================================================
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# SOURCE CLICKHOUSE - GOLD
# ============================================================
SRC_HOST = os.getenv("SRC_CLICKHOUSE_HOST")
SRC_PORT = int(os.getenv("SRC_CLICKHOUSE_PORT"))
SRC_USER = os.getenv("SRC_CLICKHOUSE_USER")
SRC_PASSWORD = os.getenv("SRC_CLICKHOUSE_PASSWORD", "")
SRC_DB = os.getenv("SRC_CLICKHOUSE_DATABASE")

# ============================================================
# ENVIRONMENT
# ============================================================
ENV = os.getenv("ENVIRON", "UAT").upper()

# ============================================================
# DEST CLICKHOUSE - CUID
# ============================================================
DEST_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
DEST_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
DEST_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
DEST_PASSWORD = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD", "")
DEST_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE")

TEMP_TABLE = os.getenv("TEMP_TABLE", "CSEL_SAMSUNG_LOS_TEMP")
FINAL_TABLE = os.getenv("FINAL_TABLE", "CSEL_SAMSUNG_LOS")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000000))
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
TEST_ROW_LIMIT = int(os.getenv("TEST_ROW_LIMIT", 1000000))

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
# RETRY
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
    job_start_str = job_start.strftime("%Y-%m-%d %H:%M:%S")
    job_id = f"EDLS_CSEL_SAMSUNG_{uuid.uuid4()}"

    source_count = 0
    temp_count = 0
    inserted_repository = 0

    src = None
    dest = None

    try:
        logger.info(f"🚀 Job Started : {job_id}")
        logger.info(f"🕒 Start Time : {job_start_str}")
        logger.info(f"🌐 ENVIRON : {ENV}")

        # =====================================================
        # CONNECT SOURCE CLICKHOUSE
        # =====================================================
        logger.info("🔌 Connecting to Source ClickHouse...")
        src = clickhouse_connect.get_client(
            host=SRC_HOST,
            port=SRC_PORT,
            username=SRC_USER,
            password=SRC_PASSWORD,
            database=SRC_DB
        )

        # =====================================================
        # CONNECT DEST CLICKHOUSE
        # =====================================================
        logger.info("🔌 Connecting to CUID ClickHouse...")
        dest = clickhouse_connect.get_client(
            host=DEST_HOST,
            port=DEST_PORT,
            username=DEST_USER,
            password=DEST_PASSWORD,
            database=DEST_DB
        )

        # =====================================================
        # STEP 1: TRUNCATE TEMP
        # =====================================================
        logger.info("🧹 Truncating temp table...")
        execute_with_retry(
            dest,
            f"TRUNCATE TABLE {DEST_DB}.{TEMP_TABLE}"
        )

        # =====================================================
        # STEP 2: FETCH DATA FROM GOLD
        # =====================================================
        logger.info("📊 Counting Samsung source rows...")

        count_query = """
            SELECT count()
            FROM CD_SAMSUNG.samsung_fms
        """

        source_count = src.query(count_query).result_rows[0][0]

        if TEST_MODE:
            source_count = min(source_count, TEST_ROW_LIMIT)

        logger.info(f"📊 Source rows to process: {source_count}")

        offset = 0
        total_inserted_temp = 0

        while True:
            limit_clause = BATCH_SIZE

            if TEST_MODE:
                remaining = TEST_ROW_LIMIT - total_inserted_temp
                if remaining <= 0:
                    break
                limit_clause = min(BATCH_SIZE, remaining)

            select_query = f"""
                SELECT
                    concat(ifNull(first_name,''), ' ', ifNull(last_name,'')) AS NAME,
                    upper(trimBoth(pan_number)) AS PAN_NO,
                    trim(toString(aadhaar_no)) AS AADHAR_NO,
                    trim(toString(mobile_number)) AS MOBILE_NO,
                    lower(trim(toString(mail_id))) AS EMAIL_ID,
                    trim(toString(loan_app_id)) AS APPLICATIONNO,
                    trim(toString(loan_id)) AS AGREEMENTNO,
                    trim(toString(customer_id)) AS CUSTID,
                    trim(toString(reference_id)) AS LEAD_ID,
                    trim(toString(status)) AS LEAD_STATUS,
                    trim(toString(credit_dc_res_create_at)) AS CREATED_DATE,
                    trim(toString(credit_dc_create_at)) AS SOURCE_BASED_LAST_UPDATED,
                    trim(toString(branch_code)) AS ALLOCATION_REGION_CODE,
                    trim(toString(zone)) AS ALLOCATION_REGION_TYPE
                FROM CD_SAMSUNG.samsung_fms
                LIMIT {limit_clause} OFFSET {offset}
            """

            query_result = src.query(select_query)
            batch_data = query_result.result_rows

            if not batch_data:
                break

            clean_rows = [
                tuple("" if v is None else str(v) for v in row)
                for row in batch_data
            ]

            dest.insert(
                f"{DEST_DB}.{TEMP_TABLE}",
                clean_rows,
                column_names=[
                    "NAME",
                    "PAN_NO",
                    "AADHAR_NO",
                    "MOBILE_NO",
                    "EMAIL_ID",
                    "APPLICATIONNO",
                    "AGREEMENTNO",
                    "CUSTID",
                    "LEAD_ID",
                    "LEAD_STATUS",
                    "CREATED_DATE",
                    "SOURCE_BASED_LAST_UPDATED",
                    "ALLOCATION_REGION_CODE",
                    "ALLOCATION_REGION_TYPE"
                ]
            )

            batch_count = len(clean_rows)
            offset += batch_count
            total_inserted_temp += batch_count

            logger.info(
                f"➡ Inserted to {TEMP_TABLE}: "
                f"{batch_count} | Total: {total_inserted_temp}"
            )

        temp_count = dest.query(
            f"SELECT count() FROM {DEST_DB}.{TEMP_TABLE}"
        ).result_rows[0][0]

        logger.info(f"Temp Count : {temp_count}")

        # =====================================================
        # STEP 3: INSERT INTO FINAL
        # =====================================================
        final_existing_count = dest.query(
            f"SELECT count() FROM {DEST_DB}.{FINAL_TABLE}"
        ).result_rows[0][0]

        if final_existing_count == 0:
            logger.info(f"🚀 Initial Load into {FINAL_TABLE}")

            execute_with_retry(dest, f"""
                INSERT INTO {DEST_DB}.{FINAL_TABLE}
                (
                    NAME,
                    PAN_NO,
                    AADHAR_NO,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,
                    AGREEMENTNO,
                    CUSTID,
                    LEAD_ID,
                    LEAD_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    ALLOCATION_REGION_CODE,
                    ALLOCATION_REGION_TYPE,
                    checksum1,
                    checksum2
                )
                SELECT
                    NAME,
                    upper(trimBoth(PAN_NO)) AS PAN_NO,
                    AADHAR_NO,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,
                    AGREEMENTNO,
                    CUSTID,
                    LEAD_ID,
                    LEAD_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    ALLOCATION_REGION_CODE,
                    ALLOCATION_REGION_TYPE,
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
                    NAME,
                    PAN_NO,
                    AADHAR_NO,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,
                    AGREEMENTNO,
                    CUSTID,
                    LEAD_ID,
                    LEAD_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    ALLOCATION_REGION_CODE,
                    ALLOCATION_REGION_TYPE,
                    checksum1,
                    checksum2
                )
                SELECT
                    t.NAME,
                    upper(trimBoth(t.PAN_NO)) AS PAN_NO,
                    t.AADHAR_NO,
                    t.MOBILE_NO,
                    t.EMAIL_ID,
                    t.APPLICATIONNO,
                    t.AGREEMENTNO,
                    t.CUSTID,
                    t.LEAD_ID,
                    t.LEAD_STATUS,
                    t.CREATED_DATE,
                    t.SOURCE_BASED_LAST_UPDATED,
                    t.ALLOCATION_REGION_CODE,
                    t.ALLOCATION_REGION_TYPE,
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

        # =====================================================
        # STEP 4: CUID REPOSITORY - VALKEY BASED
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
            start_id, end_id = reserve_cuid_range(pending_repository)

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

            execute_with_retry(dest, repository_sql)

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
        # STEP 5: OPTIMIZE FINAL
        # =====================================================
        logger.info("📦 Optimizing final table...")

        # execute_with_retry(
        #     dest,
        #     f"OPTIMIZE TABLE {DEST_DB}.{FINAL_TABLE} FINAL"
        # )

        # =====================================================
        # STEP 6: INSERT INTO CUID MASTER
        # =====================================================
        logger.info("📦 Inserting into CUID_MASTER_TABLE Samsung...")

        execute_with_retry(dest, f"""
            INSERT INTO CUID.CUID_MASTER_TABLE
            (
                TYPE,
                SOURCE,
                NAME,
                PAN_NO,
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
                'CSEL_SAMSUNG_LOS' AS SOURCE,

                ifNull(p.NAME, '') AS NAME,
                upper(trimBoth(p.PAN_NO)) AS PAN_NO,

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
                '' AS CIF_NO,

                ifNull(p.AGREEMENTNO, '') AS AGREEMENTNO,

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
                '' AS DATE_OF_BIRTH,
                '' AS DATE_OF_JOINING,

                ifNull(p.ALLOCATION_REGION_CODE, '') AS ALLOCATION_REGION_CODE,
                ifNull(p.ALLOCATION_REGION_TYPE, '') AS ALLOCATION_REGION_TYPE,

                r.CUID,
                r.CUID_CREATION_DATE,

                'Loaded from SAMSUNG_LOS' AS REMARKS

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

        end_time = time.time()
        duration = round(end_time - start_time, 2)

        logger.info("🎯 ETL COMPLETED SUCCESSFULLY")
        logger.info(f"⏱ Duration Seconds : {duration}")

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_number = exc_traceback.tb_lineno if exc_traceback else "Unknown"

        logger.error(f"❌ ETL FAILED at line {line_number}: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    run_etl()