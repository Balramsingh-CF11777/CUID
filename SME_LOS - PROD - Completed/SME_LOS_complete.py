import os
import clickhouse_connect
import logging
import time
import uuid
import traceback
import sys

from dotenv import load_dotenv
from datetime import datetime
from retrying import retry

import valkey
from valkey.connection import BlockingConnectionPool

job_id = f"SME_LOS_{uuid.uuid4()}"

source_count = 0
temp_count = 0
inserted_count = 0
new_count = 0
changed_count = 0
inserted_repository = 0
gap_count = 0

# ============================================================
# ENV LOAD
# ============================================================

load_dotenv()

ENV = os.getenv("ENVIRON", "UAT").upper()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# ============================================================
# SOURCE CLICKHOUSE (SME)
# ============================================================

SRC_HOST = os.getenv("SRC_CLICKHOUSE_HOST")
SRC_PORT = int(os.getenv("SRC_CLICKHOUSE_PORT"))
SRC_USER = os.getenv("SRC_CLICKHOUSE_USER")
SRC_PASSWORD = os.getenv("SRC_CLICKHOUSE_PASSWORD")
SRC_DB = os.getenv("SRC_CLICKHOUSE_DATABASE")

# ============================================================
# DEST CLICKHOUSE (CUID)
# ============================================================

DEST_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
DEST_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
DEST_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
DEST_PASSWORD = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD")
DEST_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE")

# ============================================================
# TABLE CONFIG
# ============================================================

TEMP_TABLE = os.getenv("TEMP_TABLE", "SME_LOS_TEMP")
FINAL_TABLE = os.getenv("FINAL_TABLE", "SME_LOS")

# ============================================================
# BATCH CONFIG
# ============================================================

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10000))
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
TEST_ROW_LIMIT = int(os.getenv("TEST_ROW_LIMIT", 10000))

# ============================================================
# VALKEY CONFIG
# ============================================================

VALKEY_HOST = os.getenv(f"{ENV}_VALKEY_HOST")
VALKEY_PORT = int(os.getenv(f"{ENV}_VALKEY_PORT"))
VALKEY_USERNAME = os.getenv(f"{ENV}_VALKEY_USERNAME","")
VALKEY_PASSWORD = os.getenv(f"{ENV}_VALKEY_PASSWORD","")

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

    pool = BlockingConnectionPool(
        **pool_kwargs
    )

    return valkey.Valkey(
        connection_pool=pool
    )


# ============================================================
# RESERVE CUID RANGE
# ============================================================

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

    start_id = end_id - count + 1

    logger.info(
        f"Reserved Range : "
        f"{start_id} -> {end_id}"
    )

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

    job_id = f"SME_LOS_{uuid.uuid4()}"

    logger.info("=" * 80)
    logger.info("🚀 SME LOS ETL STARTED")
    logger.info(f"🆔 Job ID : {job_id}")
    logger.info("=" * 80)

    try:

        # =====================================================
        # SOURCE CONNECTION
        # =====================================================

        logger.info("🔌 Connecting to SOURCE ClickHouse...")

        src = clickhouse_connect.get_client(
            host=SRC_HOST,
            port=SRC_PORT,
            username=SRC_USER,
            password=SRC_PASSWORD,
            database=SRC_DB
        )

        # =====================================================
        # DEST CONNECTION
        # =====================================================

        logger.info("🔌 Connecting to DEST ClickHouse...")

        dest = clickhouse_connect.get_client(
            host=DEST_HOST,
            port=DEST_PORT,
            username=DEST_USER,
            password=DEST_PASSWORD,
            database=DEST_DB
        )

        # =====================================================
        # STEP 1 : TRUNCATE TEMP TABLE
        # =====================================================

        logger.info("🧹 Truncating TEMP table...")

        execute_with_retry(
            dest,
            f"TRUNCATE TABLE {DEST_DB}.{TEMP_TABLE}"
        )

        # =====================================================
        # STEP 2 : SOURCE COUNT
        # =====================================================

        logger.info("📊 Counting source records...")

        count_query = """
        SELECT count()
        FROM
        (
            SELECT
                any(A.`APPLICANT NAME`) AS NAME,
                upper(trimBoth(A.`APPLICANT PAN`)) AS PAN_NO,
                any(A.`GST NUMBER`) AS GST_NO,
                any(A.`APPLICANT AADHAR`) AS AADHAR_NO,
                any(A.`APPLICANT VOTERID`) AS VOTER_ID,
                any(A.`APPLICANT DRIVING LICENS`) AS DRIVING_LICN_NO,
                '' AS PASSPORT,
                any(
                    replaceAll(
                        toString(A.`APPLICANT MOBILE NO1`),
                        ',',
                        ''
                    )
                ) AS MOBILE_NO,
                any(A.`APPLICANT EMAIL ID`) AS EMAIL_ID,
                any(A.`APP ID`) AS APPLICATIONNO,
                arrayStringConcat(
                    arrayDistinct(
                        arrayFilter(
                            x -> x != '',
                            arrayMap(
                                x -> trimBoth(x),
                                arrayFlatten(
                                    groupArray(
                                        splitByChar(
                                            ',',
                                            ifNull(A.`CO_APPLICANT PAN`, '')
                                        )
                                    )
                                )
                            )
                        )
                    ),
                    ', '
                ) AS COAPPLICANT_PAN_NO,
                arrayStringConcat(
                    arrayDistinct(
                        arrayFilter(
                            x -> x != '',
                            arrayMap(
                                x -> trimBoth(x),
                                arrayFlatten(
                                    groupArray(
                                        splitByChar(
                                            ',',
                                            ifNull(A.`GUARANTOR PAN`, '')
                                        )
                                    )
                                )
                            )
                        )
                    ),
                    ', '
                ) AS GUARANTOR_PAN_NO,
                any(B.`AGREEMENT NUMBER`) AS AGREEMENTNO,
                any(A.`Applicant UCIC ID`) AS CIF_NO,
                any(A.`APPLICANT CUSTID`) AS CUSTID,
                any(B.`AGREEMENT NUMBER`) AS LEAD_ID,
                any(B.`APPLICATION STAUS`) AS LEAD_STATUS,
                ifNull(
                    toString(
                        any(
                            parseDateTimeBestEffortOrNull(
                                toString(B.`BUSINESS DATE`)
                            )
                        )
                    ),
                    ''
                ) AS CREATED_DATE,
                ifNull(
                    toString(
                        any(
                            parseDateTimeBestEffortOrNull(
                                toString(B.`LOS DISB DATE`)
                            )
                        )
                    ),
                    ''
                ) AS SOURCE_BASED_LAST_UPDATED,
                ifNull(
                    toString(
                        any(
                            parseDateTimeBestEffortOrNull(
                                toString(A.`DATE OF INCORPORATION`)
                            )
                        )
                    ),
                    ''
                ) AS DATE_OF_INCORPORATION,
                ifNull(
                    toString(
                        any(
                            parseDateTimeBestEffortOrNull(
                                toString(A.`APPLICANT DOB`)
                            )
                        )
                    ),
                    ''
                ) AS DATE_OF_BIRTH,
                '' AS ALLOCATION_REGION_CODE,
                any(A.`ZONE`) AS ALLOCATION_REGION_TYPE,
                any(A.`GST NUMBER`) AS REGISTRATION_CERTIFICATE
            FROM SME.SME_CUSBASIC_DETAILS A
            LEFT JOIN SME.SME_LOAN_DETAILS B
                ON toString(A.`APP ID`) = toString(B.`APP ID`)
            WHERE
                A.`APPLICANT PAN` IS NOT NULL
                AND trimBoth(A.`APPLICANT PAN`) != ''
            GROUP BY
                upper(trimBoth(A.`APPLICANT PAN`)),
                A.`APP ID`
            ORDER BY
                A.`APP ID`
        )
        """

        source_count = src.query(count_query).result_rows[0][0]

        logger.info(f"📊 Source Count : {source_count}")

        # =====================================================
        # TEST MODE
        # =====================================================

        if TEST_MODE:
            logger.info("🧪 TEST MODE ENABLED")
            logger.info(f"🧪 TEST ROW LIMIT : {TEST_ROW_LIMIT}")

        offset = 0

        inserted_count = 0

        # =====================================================
        # STEP 3 : FETCH + INSERT TO TEMP
        # =====================================================

        while True:

            if TEST_MODE and inserted_count >= TEST_ROW_LIMIT:
                logger.info("🧪 Test limit reached")
                break

            logger.info(f"📥 Processing OFFSET : {offset}")

            # =================================================
            # SOURCE QUERY
            # =================================================

            select_query = f"""
            SELECT
                any(A.`APPLICANT NAME`) AS NAME,

                upper(trimBoth(A.`APPLICANT PAN`)) AS PAN_NO,

                any(A.`GST NUMBER`) AS GST_NO,

                any(A.`APPLICANT AADHAR`) AS AADHAR_NO,

                any(A.`APPLICANT VOTERID`) AS VOTER_ID,

                any(A.`APPLICANT DRIVING LICENS`) AS DRIVING_LICN_NO,

                '' AS PASSPORT,

                any(
                    replaceAll(
                        toString(A.`APPLICANT MOBILE NO1`),
                        ',',
                        ''
                    )
                ) AS MOBILE_NO,

                any(A.`APPLICANT EMAIL ID`) AS EMAIL_ID,

                any(A.`APP ID`) AS APPLICATIONNO,

                arrayStringConcat(
                    arrayDistinct(
                        arrayFilter(
                            x -> x != '',
                            arrayMap(
                                x -> trimBoth(x),
                                arrayFlatten(
                                    groupArray(
                                        splitByChar(
                                            ',',
                                            ifNull(A.`CO_APPLICANT PAN`, '')
                                        )
                                    )
                                )
                            )
                        )
                    ),
                    ', '
                ) AS COAPPLICANT_PAN_NO,

                arrayStringConcat(
                    arrayDistinct(
                        arrayFilter(
                            x -> x != '',
                            arrayMap(
                                x -> trimBoth(x),
                                arrayFlatten(
                                    groupArray(
                                        splitByChar(
                                            ',',
                                            ifNull(A.`GUARANTOR PAN`, '')
                                        )
                                    )
                                )
                            )
                        )
                    ),
                    ', '
                ) AS GUARANTOR_PAN_NO,

                any(B.`AGREEMENT NUMBER`) AS AGREEMENTNO,

                any(A.`Applicant UCIC ID`) AS CIF_NO,

                any(A.`APPLICANT CUSTID`) AS CUSTID,

                any(B.`AGREEMENT NUMBER`) AS LEAD_ID,

                any(B.`APPLICATION STAUS`) AS LEAD_STATUS,

                ifNull(
                    toString(
                        any(
                            parseDateTimeBestEffortOrNull(
                                toString(B.`BUSINESS DATE`)
                            )
                        )
                    ),
                    ''
                ) AS CREATED_DATE,

                ifNull(
                    toString(
                        any(
                            parseDateTimeBestEffortOrNull(
                                toString(B.`LOS DISB DATE`)
                            )
                        )
                    ),
                    ''
                ) AS SOURCE_BASED_LAST_UPDATED,

                ifNull(
                    toString(
                        any(
                            parseDateTimeBestEffortOrNull(
                                toString(A.`DATE OF INCORPORATION`)
                            )
                        )
                    ),
                    ''
                ) AS DATE_OF_INCORPORATION,

                ifNull(
                    toString(
                        any(
                            parseDateTimeBestEffortOrNull(
                                toString(A.`APPLICANT DOB`)
                            )
                        )
                    ),
                    ''
                ) AS DATE_OF_BIRTH,

                '' AS ALLOCATION_REGION_CODE,

                any(A.`ZONE`) AS ALLOCATION_REGION_TYPE,

                any(A.`GST NUMBER`) AS REGISTRATION_CERTIFICATE

            FROM SME.SME_CUSBASIC_DETAILS A

            LEFT JOIN SME.SME_LOAN_DETAILS B
                ON toString(A.`APP ID`) = toString(B.`APP ID`)

            WHERE
                A.`APPLICANT PAN` IS NOT NULL
                AND trimBoth(A.`APPLICANT PAN`) != ''

            GROUP BY
                upper(trimBoth(A.`APPLICANT PAN`)),
                A.`APP ID`

            ORDER BY
                A.`APP ID`

            LIMIT {BATCH_SIZE} OFFSET {offset}
            """

            query_result = src.query(select_query)

            rows = query_result.result_rows

            logger.info(
                f"📊 Batch rows fetched: {len(rows):,}"
            )

            if not rows:
                logger.info("✅ No more records found")
                break

            # =================================================
            # INSERT INTO TEMP
            # =================================================

            dest.insert(
                f"{DEST_DB}.{TEMP_TABLE}",
                rows,
                column_names=[
                    "NAME",
                    "PAN_NO",
                    "GST_NO",
                    "AADHAR_NO",
                    "VOTER_ID",
                    "DRIVING_LICN_NO",
                    "PASSPORT",
                    "MOBILE_NO",
                    "EMAIL_ID",
                    "APPLICATIONNO",
                    "COAPPLICANT_PAN_NO",
                    "GUARANTOR_PAN_NO",
                    "AGREEMENTNO",
                    "CIF_NO",
                    "CUSTID",
                    "LEAD_ID",
                    "LEAD_STATUS",
                    "CREATED_DATE",
                    "SOURCE_BASED_LAST_UPDATED",
                    "DATE_OF_INCORPORATION",
                    "DATE_OF_BIRTH",
                    "ALLOCATION_REGION_CODE",
                    "ALLOCATION_REGION_TYPE",
                    "REGISTRATION_CERTIFICATE"
                ]
            )

            batch_inserted = len(rows)

            inserted_count += batch_inserted

            offset += batch_inserted

            logger.info(
                f"✅ Batch Inserted : {batch_inserted} | "
                f"Total Inserted : {inserted_count}"
            )

        # =====================================================
        # STEP 4 : TEMP COUNT
        # =====================================================

        temp_count = dest.query(
            f"SELECT count() FROM {DEST_DB}.{TEMP_TABLE}"
        ).result_rows[0][0]

        logger.info(f"📊 Temp Count : {temp_count}")

        # =====================================================
        # STEP 5 : FINAL TABLE LOAD
        # =====================================================

        master_count = dest.query(
            f"SELECT count() FROM {DEST_DB}.{FINAL_TABLE}"
        ).result_rows[0][0]

        logger.info(f"📊 Final Table Count : {master_count}")

        # =====================================================
        # INITIAL LOAD
        # =====================================================

        if master_count == 0:

            logger.info("🚀 INITIAL LOAD STARTED")

            execute_with_retry(dest, f"""
            INSERT INTO {DEST_DB}.{FINAL_TABLE}
            (
                NAME,
                PAN_NO,
                GST_NO,
                AADHAR_NO,
                VOTER_ID,
                DRIVING_LICN_NO,
                PASSPORT,
                MOBILE_NO,
                EMAIL_ID,
                APPLICATIONNO,
                COAPPLICANT_PAN_NO,
                CIF_NO,
                GUARANTOR_PAN_NO,
                AGREEMENTNO,
                CUSTID,
                LEAD_ID,
                LEAD_STATUS,
                CREATED_DATE,
                SOURCE_BASED_LAST_UPDATED,
                DATE_OF_INCORPORATION,
                DATE_OF_BIRTH,
                ALLOCATION_REGION_CODE,
                ALLOCATION_REGION_TYPE,
                REGISTRATION_CERTIFICATE,
                checksum1,
                checksum2
            )
            SELECT
                NAME,
                PAN_NO,
                GST_NO,
                AADHAR_NO,
                VOTER_ID,
                DRIVING_LICN_NO,
                PASSPORT,
                MOBILE_NO,
                EMAIL_ID,
                APPLICATIONNO,
                COAPPLICANT_PAN_NO,
                CIF_NO,
                GUARANTOR_PAN_NO,
                AGREEMENTNO,
                CUSTID,
                LEAD_ID,
                LEAD_STATUS,
                CREATED_DATE,
                SOURCE_BASED_LAST_UPDATED,
                DATE_OF_INCORPORATION,
                DATE_OF_BIRTH,
                ALLOCATION_REGION_CODE,
                ALLOCATION_REGION_TYPE,
                REGISTRATION_CERTIFICATE,
                checksum1,
                checksum2
            FROM {DEST_DB}.{TEMP_TABLE}
            """)

        # =====================================================
        # INCREMENTAL LOAD
        # =====================================================

        else:

            logger.info("🔄 INCREMENTAL LOAD STARTED")

            new_count = dest.query(f"""
            SELECT count()
            FROM {DEST_DB}.{TEMP_TABLE} t
            LEFT JOIN
            (
                SELECT DISTINCT checksum1
                FROM {DEST_DB}.{FINAL_TABLE}
            ) m
            ON t.checksum1 = m.checksum1
            WHERE m.checksum1 IS NULL
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
            WHERE t.checksum2 != m.checksum2
            """).result_rows[0][0]

            execute_with_retry(dest, f"""
                INSERT INTO {DEST_DB}.{FINAL_TABLE}
                (
                    NAME,
                    PAN_NO,
                    GST_NO,
                    AADHAR_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    PASSPORT,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,
                    COAPPLICANT_PAN_NO,
                    CIF_NO,
                    GUARANTOR_PAN_NO,
                    AGREEMENTNO,
                    CUSTID,
                    LEAD_ID,
                    LEAD_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    DATE_OF_INCORPORATION,
                    DATE_OF_BIRTH,
                    ALLOCATION_REGION_CODE,
                    ALLOCATION_REGION_TYPE,
                    REGISTRATION_CERTIFICATE,
                    checksum1,
                    checksum2
                )

                SELECT
                    t.NAME,
                    t.PAN_NO,
                    t.GST_NO,
                    t.AADHAR_NO,
                    t.VOTER_ID,
                    t.DRIVING_LICN_NO,
                    t.PASSPORT,
                    t.MOBILE_NO,
                    t.EMAIL_ID,
                    t.APPLICATIONNO,
                    t.COAPPLICANT_PAN_NO,
                    t.CIF_NO,
                    t.GUARANTOR_PAN_NO,
                    t.AGREEMENTNO,
                    t.CUSTID,
                    t.LEAD_ID,
                    t.LEAD_STATUS,
                    t.CREATED_DATE,
                    t.SOURCE_BASED_LAST_UPDATED,
                    t.DATE_OF_INCORPORATION,
                    t.DATE_OF_BIRTH,
                    t.ALLOCATION_REGION_CODE,
                    t.ALLOCATION_REGION_TYPE,
                    t.REGISTRATION_CERTIFICATE,
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
            
   

            logger.info(f"🆕 New Records     : {new_count:,}")
            logger.info(f"🔄 Changed Records : {changed_count:,}")

            logger.info(
                f"📦 Inserted Records : {new_count + changed_count:,}"
            )

        # =====================================================
        # STEP 6 : CUID REPOSITORY (VALKEY)
        # =====================================================

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
            upper(trimBoth(PAN_NO)) AS PAN_NO
        FROM CUID.SME_LOS
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
            f"Pending PANs : {pending_repository:,}"
        )

        inserted_repository = 0
        gap_count = 0

        if pending_repository > 0:

            start_id, end_id = reserve_cuid_range(
                pending_repository
            )

            logger.info(
                f"🔒 Reserved Range : "
                f"{start_id:,} -> {end_id:,}"
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

                toString(now()) AS CUID_CREATION_DATE

            FROM
            (
                SELECT DISTINCT
                    s.PAN_NO

                FROM CUID.CUID_NEW_PAN_STAGE s

                LEFT ANTI JOIN
                (
                    SELECT DISTINCT PAN_NO
                    FROM CUID.CUID_REPOSITORY
                ) r
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

            gap_count = (
                pending_repository
                - inserted_repository
            )

            logger.info(
                f"Pending={pending_repository:,}, "
                f"Inserted={inserted_repository:,}, "
                f"Gap={gap_count:,}"
            )

            if gap_count > 0:

                logger.warning(
                    f"⚠ {gap_count:,} PANs reserved "
                    f"but not inserted."
                )

        else:

            logger.info(
                "✅ No new PANs found"
            )

        # =====================================================
        # STEP 7 : CUID MASTER TABLE
        # =====================================================

        logger.info("📦 Inserting into CUID_MASTER_TABLE...")

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
                'SME_LOS' AS SOURCE,

                p.NAME,

                upper(trimBoth(p.PAN_NO)) AS PAN_NO,

                p.GST_NO,

                p.AADHAR_NO,

                '' AS CKYCID,

                '' AS TIN_NO,

                p.VOTER_ID,

                p.DRIVING_LICN_NO,

                '' AS RATIONCARD_NO,

                p.PASSPORT,

                p.MOBILE_NO,

                p.EMAIL_ID,

                p.APPLICATIONNO,

                p.COAPPLICANT_PAN_NO,

                p.GUARANTOR_PAN_NO,

                p.CIF_NO,

                p.AGREEMENTNO,

                p.CIF_NO AS CUST_UNQ_ID,

                p.CUSTID,

                '' AS VENDOR_ID,

                '' AS SOURCE_BASED_STATUS,

                p.REGISTRATION_CERTIFICATE,

                '' AS NPA_STAGEID,

                p.LEAD_ID,

                p.LEAD_STATUS,

                p.SOURCE_BASED_LAST_UPDATED,

                '' AS ASSET_VALUE,

                p.DATE_OF_INCORPORATION,

                '' AS USER_EMPLOYEE_NO,

                p.DATE_OF_BIRTH,

                '' AS DATE_OF_JOINING,

                p.ALLOCATION_REGION_CODE,

                p.ALLOCATION_REGION_TYPE,

                r.CUID,

                r.CUID_CREATION_DATE,

                'Loaded from SME_LOS' AS REMARKS

            FROM CUID.SME_LOS p

            INNER JOIN CUID.CUID_REPOSITORY r
                ON upper(trimBoth(p.PAN_NO))
                = upper(trimBoth(r.PAN_NO))

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

        # =====================================================
        # STEP 8 : OPTIMIZE
        # =====================================================

        logger.info("⚡ Optimizing tables...")

        # execute_with_retry(
        #     dest,
        #     f"OPTIMIZE TABLE {DEST_DB}.{FINAL_TABLE} FINAL"
        # )

        master_inserted = dest.query("""
        SELECT count()
        FROM CUID.CUID_MASTER_TABLE
        WHERE SOURCE='SME_LOS'
        AND LastUpdated >= today()
        AND LastUpdated < today() + 1
        """).result_rows[0][0]

        logger.info(
            f"📦 CUID_MASTER_TABLE Rows Inserted : {master_inserted:,}"
        )

        # =====================================================
        # COMPLETED
        # =====================================================

        end_time = time.time()

        logger.info(f"📦 Inserted     : {inserted_count:,}")

        logger.info(
            f"🆔 Repository Inserts : "
            f"{inserted_repository:,}"
        )

        logger.info(
            f"📊 Repository Pending : "
            f"{pending_repository:,}"
        )

        logger.info(
            f"📊 Repository Gap : "
            f"{gap_count:,}"
        )

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_number = exc_traceback.tb_lineno
        logger.exception(
            f"❌ SME_LOS ETL FAILED at line {line_number}: {e}"
        )
        logger.exception(traceback.format_exc())
        raise

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    run_etl()