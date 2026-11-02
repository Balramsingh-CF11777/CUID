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
# SOURCE CLICKHOUSE
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
# DEST CLICKHOUSE
# ============================================================

DEST_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
DEST_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
DEST_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
DEST_PASSWORD = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD", "")
DEST_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE")

TEMP_TABLE = os.getenv("TEMP_TABLE", "EDLS_GOLD_LEAD_TEMP")
FINAL_TABLE = os.getenv("FINAL_TABLE", "EDLS_GOLD_LEAD")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100000))
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
    job_start_str = job_start.strftime("%Y-%m-%d %H:%M:%S")
    job_id = f"EDLS_GOLD_{uuid.uuid4()}"

    source_count = 0
    temp_count = 0
    inserted_repository = 0

    try:
        logger.info(f"🚀 Job Started : {job_id}")
        logger.info(f"🌐 ENVIRON : {ENV}")

        logger.info("🔌 Connecting to GOLD ClickHouse...")
        src = clickhouse_connect.get_client(
            host=SRC_HOST,
            port=SRC_PORT,
            username=SRC_USER,
            password=SRC_PASSWORD,
            database=SRC_DB
        )

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
        # STEP 2: SOURCE COUNT
        # =====================================================
        logger.info("📊 Counting GOLD rows...")

        count_query = """
            SELECT count()
            FROM GOLD.gl_lead gl
            LEFT JOIN GOLD.gl_lead_kyc_flags KF
                ON KF._id = gl._id
        """

        source_count = src.query(count_query).result_rows[0][0]

        if TEST_MODE:
            source_count = min(source_count, TEST_ROW_LIMIT)

        logger.info(f"📊 Source rows to process: {source_count}")

        # =====================================================
        # STEP 3: LOAD TEMP
        # =====================================================
        offset = 0
        total_loaded = 0

        temp_columns = [
            "NAME",
            "PAN_NO",
            "AADHAR_NO",
            "VOTER_ID",
            "DRIVING_LICN_NO",
            "MOBILE_NO",
            "EMAIL_ID",
            "APPLICATIONNO",
            "CIF_NO",
            "AGREEMENTNO",
            "NPA_STAGEID",
            "LEAD_ID",
            "LEAD_STATUS",
            "CREATED_DATE",
            "SOURCE_BASED_LAST_UPDATED",
            "ALLOCATION_REGION_CODE",
            "ALLOCATION_REGION_TYPE"
        ]

        while True:

            if TEST_MODE and total_loaded >= TEST_ROW_LIMIT:
                break

            limit_clause = BATCH_SIZE

            if TEST_MODE:
                remaining = TEST_ROW_LIMIT - total_loaded
                limit_clause = min(BATCH_SIZE, remaining)

            select_query = f"""
                SELECT DISTINCT
                    ifNull(KF.full_name, '') AS NAME,
                    upper(trimBoth(toString(KF.pan_no))) AS PAN_NO,
                    ifNull(toString(gl.aadhaar_no), '') AS AADHAR_NO,
                    ifNull(toString(KF.voterid_no), '') AS VOTER_ID,
                    ifNull(toString(KF.driving_license_no), '') AS DRIVING_LICN_NO,
                    ifNull(toString(MO.maskedvalue), '') AS MOBILE_NO,
                    ifNull(toString(ad.email_id), '') AS EMAIL_ID,
                    ifNull(toString(gl.loan_application_id), '') AS APPLICATIONNO,
                    ifNull(toString(gl.ucic), '') AS CIF_NO,
                    ifNull(toString(gl.loan_agreement_number), '') AS AGREEMENTNO,
                    ifNull(toString(gl.npastatus), '') AS NPA_STAGEID,
                    ifNull(toString(gl.lead_id), '') AS LEAD_ID,
                    ifNull(toString(gl.status), '') AS LEAD_STATUS,

                    substring(
                        replace(toString(gl.created_date), 'T', ' '),
                        1,
                        19
                    ) AS CREATED_DATE,

                    substring(
                        replace(toString(gl.modified_date), 'T', ' '),
                        1,
                        19
                    ) AS SOURCE_BASED_LAST_UPDATED,

                    ifNull(toString(bd.branchcode), '') AS ALLOCATION_REGION_CODE,
                    ifNull(toString(bd.branchdesc), '') AS ALLOCATION_REGION_TYPE

                FROM GOLD.gl_lead gl

                LEFT JOIN GOLD.gl_lead_branch_details bd
                    ON bd.id = gl.id

                LEFT JOIN GOLD.gl_lead_additional_details ad
                    ON ad.id = gl.id

                LEFT JOIN GOLD.gl_lead_mobile_no MO
                    ON MO.id = gl.id

                LEFT JOIN GOLD.gl_lead_kyc_flags KF
                    ON KF._id = gl._id

                ORDER BY gl.loan_application_id

                LIMIT {limit_clause}
                OFFSET {offset}
            """

            rows = src.query(select_query).result_rows

            logger.info(f"Fetched {len(rows)} rows from GOLD")

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
                f"➡ Inserted to temp: {batch_count} | Total: {total_loaded}"
            )

        temp_count = dest.query(
            f"SELECT count() FROM {DEST_DB}.{TEMP_TABLE}"
        ).result_rows[0][0]

        logger.info(f"📦 Temp Count : {temp_count}")

        # =====================================================
        # STEP 4: INITIAL OR INCREMENTAL INSERT
        # =====================================================
        final_count = dest.query(
            f"SELECT count() FROM {DEST_DB}.{FINAL_TABLE}"
        ).result_rows[0][0]

        if final_count == 0:
            logger.info("🚀 Initial Load → Direct Insert")

            execute_with_retry(dest, f"""
                INSERT INTO {DEST_DB}.{FINAL_TABLE}
                (
                    NAME,
                    PAN_NO,
                    AADHAR_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,
                    CIF_NO,
                    AGREEMENTNO,
                    NPA_STAGEID,
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
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,
                    CIF_NO,
                    AGREEMENTNO,
                    NPA_STAGEID,
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

            inserted_final = temp_count
            logger.info(f"📦 Inserted Records: {inserted_final}")

        else:
            logger.info("🔄 Incremental Load")

            new_count = dest.query(f"""
                SELECT count()
                FROM {DEST_DB}.{TEMP_TABLE} t
                LEFT JOIN
                (
                    SELECT DISTINCT checksum1
                    FROM {DEST_DB}.{FINAL_TABLE}
                ) m
                    ON t.checksum1 = m.checksum1
                WHERE
                    m.checksum1 IS NULL
                    AND t.PAN_NO IS NOT NULL
                    AND trimBoth(t.PAN_NO) != ''
                    AND match(
                        upper(trimBoth(t.PAN_NO)),
                        '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                    )
            """).result_rows[0][0]

            changed_count = dest.query(f"""
                SELECT count()
                FROM {DEST_DB}.{TEMP_TABLE} t
                INNER JOIN
                (
                    SELECT
                        checksum1,
                        argMax(checksum2, version) AS checksum2
                    FROM {DEST_DB}.{FINAL_TABLE}
                    GROUP BY checksum1
                ) m
                    ON t.checksum1 = m.checksum1
                WHERE
                    t.checksum2 != m.checksum2
                    AND t.PAN_NO IS NOT NULL
                    AND trimBoth(t.PAN_NO) != ''
                    AND match(
                        upper(trimBoth(t.PAN_NO)),
                        '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                    )
            """).result_rows[0][0]

            logger.info(f"🆕 New Records     : {new_count}")
            logger.info(f"🔄 Changed Records : {changed_count}")

            execute_with_retry(dest, f"""
                INSERT INTO {DEST_DB}.{FINAL_TABLE}
                (
                    NAME,
                    PAN_NO,
                    AADHAR_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,
                    CIF_NO,
                    AGREEMENTNO,
                    NPA_STAGEID,
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
                    t.VOTER_ID,
                    t.DRIVING_LICN_NO,
                    t.MOBILE_NO,
                    t.EMAIL_ID,
                    t.APPLICATIONNO,
                    t.CIF_NO,
                    t.AGREEMENTNO,
                    t.NPA_STAGEID,
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

            logger.info(f"📦 Inserted Records: {new_count + changed_count}")

        # =====================================================
        # STEP 5: CUID REPOSITORY - VALKEY BASED
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
        # STEP 6: MASTER INSERT
        # =====================================================
        logger.info("📦 Inserting into CUID_MASTER_TABLE...")

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
                'GOLD_LOS' AS SOURCE,

                ifNull(p.NAME, '') AS NAME,
                upper(trimBoth(p.PAN_NO)) AS PAN_NO,

                '' AS GST_NO,
                ifNull(p.AADHAR_NO, '') AS AADHAAR_NO,
                '' AS CKYCID,
                '' AS TIN_NO,

                ifNull(p.VOTER_ID, '') AS VOTER_ID,
                ifNull(p.DRIVING_LICN_NO, '') AS DRIVING_LICN_NO,
                '' AS RATIONCARD_NO,
                '' AS PASSPORT,

                ifNull(p.MOBILE_NO, '') AS MOBILE_NO,
                ifNull(p.EMAIL_ID, '') AS EMAIL_ID,

                ifNull(p.APPLICATIONNO, '') AS APPLICATIONNO,

                '' AS COAPPLICANT_PAN_NO,
                '' AS GUARANTOR_PAN_NO,

                ifNull(p.CIF_NO, '') AS CIF_NO,
                ifNull(p.AGREEMENTNO, '') AS AGREEMENTNO,

                '' AS CUST_UNQ_ID,
                '' AS CUSTID,
                '' AS VENDOR_ID,
                '' AS SOURCE_BASED_STATUS,
                '' AS REGISTRATION_CERTIFICATE,

                ifNull(p.NPA_STAGEID, '') AS NPA_STAGEID,
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

                'Loaded from EDLS_GOLD_LEAD' AS REMARKS

            FROM
            (
                SELECT *
                FROM {DEST_DB}.{FINAL_TABLE}
                ORDER BY version DESC
                LIMIT 1 BY checksum1
            ) p

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

        master_loaded = dest.query("""
            SELECT count()
            FROM CUID.CUID_MASTER_TABLE
            WHERE LastUpdated >= today()
              AND LastUpdated < today() + 1
        """).result_rows[0][0]

        logger.info(f"📦 CUID Master Records Loaded: {master_loaded}")

        duration_minutes = round((time.time() - start_time) / 60, 2)

        logger.info("🎯 ETL COMPLETED SUCCESSFULLY")
        logger.info(f"⏱ Duration : {duration_minutes} Minutes")

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_number = exc_traceback.tb_lineno if exc_traceback else "Unknown"

        logger.error(f"❌ ETL FAILED at line {line_number}: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    run_etl()