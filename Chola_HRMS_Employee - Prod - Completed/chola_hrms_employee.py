import os
import time
import uuid
import logging
import clickhouse_connect
import valkey
from valkey.connection import BlockingConnectionPool

from dotenv import load_dotenv
from retrying import retry

# ============================================================
# ENV LOAD
# ============================================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# ============================================================
# SOURCE CLICKHOUSE
# ============================================================
SRC_HOST = os.getenv("SRC_CLICKHOUSE_HOST")
SRC_PORT = int(os.getenv("SRC_CLICKHOUSE_PORT", 8123))
SRC_USER = os.getenv("SRC_CLICKHOUSE_USER")
SRC_PASSWORD = os.getenv("SRC_CLICKHOUSE_PASSWORD")
SRC_DB = os.getenv("SRC_CLICKHOUSE_DATABASE")
SRC_TABLE = os.getenv("SRC_CLICKHOUSE_TABLE")

# ============================================================
# DEST CLICKHOUSE
# ============================================================

ENV = os.getenv("ENVIRON", "UAT").upper()
DEST_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
DEST_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
DEST_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
DEST_PASSWORD = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD")
DEST_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE")

# ============================================================
# TABLE CONFIG
# ============================================================

TEMP_TABLE = os.getenv("TEMP_TABLE")
FINAL_TABLE = os.getenv("FINAL_TABLE")

# ============================================================
# VALKEY CONFIG
# ============================================================

VALKEY_HOST = os.getenv(f"{ENV}_VALKEY_HOST")
VALKEY_PORT = int(os.getenv(f"{ENV}_VALKEY_PORT"))
VALKEY_USERNAME = os.getenv(f"{ENV}_VALKEY_USERNAME","")
VALKEY_PASSWORD = os.getenv(f"{ENV}_VALKEY_PASSWORD","")
VALKEY_KEY = "cuid_counter"

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

    return valkey.Valkey(
        connection_pool=pool
    )


def reserve_cuid_range(count):
    client = get_valkey_client()

    current_counter = client.get(
        VALKEY_KEY
    )

    logger.info(
        f"Current Valkey Counter : "
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
        f"Reserved Range : "
        f"{start_id} -> {end_id}"
    )

    return start_id, end_id

# ============================================================
# BATCH CONFIG
# ============================================================
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100000))

TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
TEST_ROW_LIMIT = int(os.getenv("TEST_ROW_LIMIT", 100000))

# ============================================================
# RETRY
# ============================================================


@retry(stop_max_attempt_number=3, wait_fixed=3000)
def execute_with_retry(client, query):
    client.command(query)

# ============================================================
# MAIN ETL
# ============================================================


def run_etl():
    start_time = time.time()
    job_id = f"HRMS_EMP_{uuid.uuid4()}"
    logger.info(f"🚀 Starting Job : {job_id}")

    # ========================================================
    # SOURCE CONNECTION
    # ========================================================
    logger.info("🔌 Connecting SOURCE ClickHouse...")

    src = clickhouse_connect.get_client(
        host=SRC_HOST,
        port=SRC_PORT,
        username=SRC_USER,
        password=SRC_PASSWORD,
        database=SRC_DB
    )

    # ========================================================
    # DEST CONNECTION
    # ========================================================
    logger.info("🔌 Connecting DEST ClickHouse...")

    dest = clickhouse_connect.get_client(
        host=DEST_HOST,
        port=DEST_PORT,
        username=DEST_USER,
        password=DEST_PASSWORD,
        database=DEST_DB
    )

    # ========================================================
    # STEP 1 : TRUNCATE TEMP TABLE
    # ========================================================
    logger.info("🧹 Truncating TEMP table...")

    execute_with_retry(
        dest,
        f"TRUNCATE TABLE {DEST_DB}.{TEMP_TABLE}"
    )

    # ========================================================
    # STEP 2 : SOURCE COUNT
    # ========================================================
    logger.info("📊 Fetching source count...")

    count_query = f"""
            SELECT count()
            FROM {SRC_DB}.{SRC_TABLE}
        """

    if TEST_MODE:
        source_count = TEST_ROW_LIMIT
    else:
        source_count = src.query(count_query).result_rows[0][0]

    logger.info(f"📦 Source Count : {source_count}")

    # ========================================================
    # STEP 3 : LOAD TEMP TABLE
    # ========================================================

    offset = 0
    inserted_total = 0

    while True:
        current_batch = BATCH_SIZE
        if TEST_MODE:
            remaining = TEST_ROW_LIMIT - inserted_total
            if remaining <= 0:
                break
            current_batch = min(BATCH_SIZE, remaining)
        source_query = f"""
            SELECT
                CONCAT(
                    IFNULL(`FIRST_NAME`, ''),
                    ' ',
                    IFNULL(`LAST_NAME`, '')
                ) AS NAME,

                upper(trimBoth(ifNull(`PAN_NO`, ''))) AS PAN_NO,

                toString(ifNull(`ADHARNO`, '')) AS AADHAAR_NO,

                ifNull(`MOBILE_NUMBER`, '') AS MOBILE_NO,

                COALESCE(
                    `OFFICE_EMAIL_ADDRESS`,
                    `PERSONAL_EMAIL_ADDRESS`,
                    ''
                ) AS EMAIL_ID,

                ifNull(`EMPLOYEE_CODE`, '') AS USER_EMPLOYEE_NO,

                ifNull(toString(`DATE_OF_BIRTH`), '') AS DATE_OF_BIRTH,

                ifNull(toString(`DATE_OF_JOINING`), '') AS DATE_OF_JOINING,

                ifNull(`EMPLOYMENT_STATUS`, '') AS SOURCE_BASED_STATUS,

                formatDateTime(
                    ifNull(
                        parseDateTimeBestEffortOrNull(toString(`GROUP_DOJ`)),
                        now()
                    ),
                    '%Y-%m-%d %H:%i:%S'
                ) AS CREATED_DATE,

                formatDateTime(
                    ifNull(
                        parseDateTimeBestEffortOrNull(toString(`LAST_UPDATED`)),
                        now()
                    ),
                    '%Y-%m-%d %H:%i:%S'
                ) AS SOURCE_BASED_LAST_UPDATED,

                ifNull(`REGION`, '') AS ALLOCATION_REGION_CODE,

                ifNull(`STATE`, '') AS ALLOCATION_REGION_TYPE

            FROM {SRC_DB}.{SRC_TABLE}
            LIMIT {current_batch} OFFSET {offset}
        """

        rows = src.query(source_query).result_rows

        if not rows:
            break
        cleaned_rows = []
        for row in rows:
            cleaned_rows.append([
                "" if v is None else str(v)
                for v in row
            ])

        dest.insert(
            table=f"{DEST_DB}.{TEMP_TABLE}",
            data=cleaned_rows,
            column_names=[
                "NAME",
                "PAN_NO",
                "AADHAAR_NO",
                "MOBILE_NO",
                "EMAIL_ID",
                "USER_EMPLOYEE_NO",
                "DATE_OF_BIRTH",
                "DATE_OF_JOINING",
                "SOURCE_BASED_STATUS",
                "CREATED_DATE",
                "SOURCE_BASED_LAST_UPDATED",
                "ALLOCATION_REGION_CODE",
                "ALLOCATION_REGION_TYPE"
            ]
        )

        inserted_total += len(cleaned_rows)
        offset += current_batch
        logger.info(f"➡ TEMP Inserted : {inserted_total}")
    logger.info(f"✅ TEMP TABLE INSERTED THIS RUN : {inserted_total}")

    # ========================================================
    # STEP 4 : LOAD FINAL TABLE
    # ========================================================
    logger.info("🚀 Loading FINAL table...")
    before_final_count = dest.query(f"""
    SELECT count()
    FROM {DEST_DB}.{FINAL_TABLE}
    """).result_rows[0][0]

    master_count = dest.query(f"""
    SELECT count()
    FROM {DEST_DB}.{FINAL_TABLE}
    """).result_rows[0][0]

    if master_count == 0:
        logger.info("🚀 Initial Load")
        execute_with_retry(dest, f"""
            INSERT INTO {DEST_DB}.{FINAL_TABLE}
            (
                NAME,
                PAN_NO,
                AADHAAR_NO,
                MOBILE_NO,
                EMAIL_ID,
                USER_EMPLOYEE_NO,
                DATE_OF_BIRTH,
                DATE_OF_JOINING,
                SOURCE_BASED_STATUS,
                CREATED_DATE,
                SOURCE_BASED_LAST_UPDATED,
                ALLOCATION_REGION_CODE,
                ALLOCATION_REGION_TYPE,
                checksum1,
                checksum2
            )
            SELECT
                NAME,
                PAN_NO,
                AADHAAR_NO,
                MOBILE_NO,
                EMAIL_ID,
                USER_EMPLOYEE_NO,
                DATE_OF_BIRTH,
                DATE_OF_JOINING,
                SOURCE_BASED_STATUS,
                CREATED_DATE,
                SOURCE_BASED_LAST_UPDATED,
                ALLOCATION_REGION_CODE,
                ALLOCATION_REGION_TYPE,
                checksum1,
                checksum2
            FROM {DEST_DB}.{TEMP_TABLE}
        """)

    else:

        logger.info("🔄 Incremental Load")

        execute_with_retry(dest, f"""
            INSERT INTO {DEST_DB}.{FINAL_TABLE}
            (
                NAME,
                PAN_NO,
                AADHAAR_NO,
                MOBILE_NO,
                EMAIL_ID,
                USER_EMPLOYEE_NO,
                DATE_OF_BIRTH,
                DATE_OF_JOINING,
                SOURCE_BASED_STATUS,
                CREATED_DATE,
                SOURCE_BASED_LAST_UPDATED,
                ALLOCATION_REGION_CODE,
                ALLOCATION_REGION_TYPE,
                checksum1,
                checksum2
            )

            SELECT
                t.NAME,
                t.PAN_NO,
                t.AADHAAR_NO,
                t.MOBILE_NO,
                t.EMAIL_ID,
                t.USER_EMPLOYEE_NO,
                t.DATE_OF_BIRTH,
                t.DATE_OF_JOINING,
                t.SOURCE_BASED_STATUS,
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
                m.checksum1 IS NULL
                OR t.checksum2 != m.checksum2
        """)

    after_final_count = dest.query(f"""
    SELECT count()
    FROM {DEST_DB}.{FINAL_TABLE}
    """).result_rows[0][0]

    final_inserted_this_run = after_final_count - before_final_count

    logger.info(f"✅ FINAL TABLE INSERTED THIS RUN : {final_inserted_this_run}")

    # ========================================================
    # STEP 5 : CUID REPOSITORY
    # ========================================================

    logger.info(
        "🆔 Preparing PAN staging table..."
    )

    execute_with_retry(
        dest,
        "TRUNCATE TABLE CUID.CUID_NEW_PAN_STAGE"
    )

    execute_with_retry(dest, """
    INSERT INTO CUID.CUID_NEW_PAN_STAGE
    SELECT DISTINCT
        upper(trimBoth(PAN_NO))
    FROM CUID.HRMS_EMPLOYEE
    FINAL
    WHERE
        PAN_NO IS NOT NULL
        AND trimBoth(PAN_NO) != ''
        AND match(
            upper(trimBoth(PAN_NO)),
            '^[A-Z]{5}[0-9]{4}[A-Z]$'
        )
        AND upper(trimBoth(PAN_NO))
            NOT IN
        (
            SELECT PAN_NO
            FROM CUID.CUID_REPOSITORY
        )
    """)

    pending_repository = dest.query("""
    SELECT count()
    FROM CUID.CUID_NEW_PAN_STAGE
    """).result_rows[0][0]

    logger.info(
        f"Pending PANs : "
        f"{pending_repository}"
    )

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

            now()

        FROM
        (
            SELECT DISTINCT s.PAN_NO
            FROM CUID.CUID_NEW_PAN_STAGE s

            LEFT ANTI JOIN
            CUID.CUID_REPOSITORY r
                ON s.PAN_NO = r.PAN_NO
        ) t
        """

        # logger.info(repository_sql)

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

            INNER JOIN
            CUID.CUID_REPOSITORY r
                ON s.PAN_NO = r.PAN_NO
        """).result_rows[0][0]

    # logger.info(
    #     f"Repository Inserts : "
    #     f"{inserted_repository}"
    # )

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

    # ========================================================
    # STEP 6 : LOAD CUID MASTER TABLE
    # ========================================================
    logger.info("📦 Loading CUID_MASTER_TABLE...")

    before_master_count = dest.query("""
    SELECT count()
    FROM CUID.CUID_MASTER_TABLE
    """).result_rows[0][0]

    execute_with_retry(dest, """
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
                'EMPLOYEE' AS TYPE,
                'HRMS_EMPLOYEE' AS SOURCE,
                ifNull(p.NAME, '') AS NAME,
                upper(trimBoth(p.PAN_NO)) AS PAN_NO,
                '' AS GST_NO,
                ifNull(p.AADHAAR_NO, '') AS AADHAAR_NO,
                '' AS CKYCID,
                '' AS TIN_NO,
                '' AS VOTER_ID,
                '' AS DRIVING_LICN_NO,
                '' AS RATIONCARD_NO,
                '' AS PASSPORT,
                ifNull(p.MOBILE_NO, '') AS MOBILE_NO,
                ifNull(p.EMAIL_ID, '') AS EMAIL_ID,
                '' AS APPLICATIONNO,
                '' AS COAPPLICANT_PAN_NO,
                '' AS GUARANTOR_PAN_NO,
                '' AS CIF_NO,
                '' AS AGREEMENTNO,
                '' AS CUST_UNQ_ID,
                '' AS CUSTID,
                '' AS VENDOR_ID,
                ifNull(
                    p.SOURCE_BASED_STATUS,
                    ''
                ) AS SOURCE_BASED_STATUS,
                '' AS REGISTRATION_CERTIFICATE,
                '' AS NPA_STAGEID,
                '' AS LEAD_ID,
                '' AS LEAD_STATUS,
                ifNull(
                    p.CREATED_DATE,
                    ''
                )  AS CREATED_DATE,
                ifNull(
                    p.SOURCE_BASED_LAST_UPDATED,
                    ''
                ) AS SOURCE_BASED_LAST_UPDATED,
                '' AS ASSET_VALUE,
                '' AS DATE_OF_INCORPORATION,
                ifNull(
                    p.USER_EMPLOYEE_NO,
                    ''
                ) AS USER_EMPLOYEE_NO,

                ifNull(
                    p.DATE_OF_BIRTH,
                    ''
                ) AS DATE_OF_BIRTH,

                ifNull(
                    p.DATE_OF_JOINING,
                    ''
                ) AS DATE_OF_JOINING,

                ifNull(
                    p.ALLOCATION_REGION_CODE,
                    ''
                ) AS ALLOCATION_REGION_CODE,

                ifNull(
                    p.ALLOCATION_REGION_TYPE,
                    ''
                ) AS ALLOCATION_REGION_TYPE,

                r.CUID,

                r.CUID_CREATION_DATE,

                'Loaded from HRMS Employee' AS REMARKS

            FROM
            (
                SELECT *
                FROM CUID.HRMS_EMPLOYEE
                ORDER BY version DESC
                LIMIT 1 BY checksum1
            ) p

            INNER JOIN CUID.CUID_REPOSITORY r
                ON upper(trimBoth(p.PAN_NO))
                = upper(trimBoth(r.PAN_NO))

            LEFT ANTI JOIN CUID.CUID_MASTER_TABLE cm
                ON upper(trimBoth(p.PAN_NO))
                = upper(trimBoth(cm.PAN_NO))
            AND cm.SOURCE = 'HRMS_EMPLOYEE'

            WHERE
                p.PAN_NO IS NOT NULL
                AND trimBoth(p.PAN_NO) != ''

                AND match(
                    upper(trimBoth(p.PAN_NO)),
                    '^[A-Z]{5}[0-9]{4}[A-Z]$'
                )
                AND p.LastUpdated >= today()
                AND p.LastUpdated < today() + 1
            """)

    after_master_count = dest.query("""
    SELECT count()
    FROM CUID.CUID_MASTER_TABLE
    """).result_rows[0][0]

    master_inserted_this_run = after_master_count - before_master_count

    logger.info(
        f"✅ CUID MASTER INSERTED THIS RUN : {master_inserted_this_run}")

    # ========================================================
    # COMPLETE
    # ========================================================
    total_time = round((time.time() - start_time) / 60, 2)

    logger.info("🎯 ETL COMPLETED SUCCESSFULLY")
    logger.info(f"⏱ Total Time : {total_time} Minutes")

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    run_etl()
