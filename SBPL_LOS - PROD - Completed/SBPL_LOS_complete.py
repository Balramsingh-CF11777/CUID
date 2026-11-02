import os
import time
import uuid
import logging
import traceback
import sys
from datetime import datetime
import valkey
from valkey.connection import BlockingConnectionPool

import clickhouse_connect

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

ENV = os.getenv("ENVIRON", "UAT").upper()

# ============================================================
# SOURCE CLICKHOUSE
# ============================================================
SRC_HOST = os.getenv("SRC_CLICKHOUSE_HOST")
SRC_PORT = int(os.getenv("SRC_CLICKHOUSE_PORT"))
SRC_USER = os.getenv("SRC_CLICKHOUSE_USER")
SRC_PASSWORD = os.getenv("SRC_CLICKHOUSE_PASSWORD")
SRC_DB = os.getenv("SRC_CLICKHOUSE_DATABASE")

# ============================================================
# DEST CLICKHOUSE
# ============================================================
DEST_HOST = os.getenv(
    f"{ENV}_CUID_CLICKHOUSE_HOST"
)

DEST_PORT = int(
    os.getenv(
        f"{ENV}_CUID_CLICKHOUSE_PORT"
    )
)

DEST_USER = os.getenv(
    f"{ENV}_CUID_CLICKHOUSE_USER"
)

DEST_PASSWORD = os.getenv(
    f"{ENV}_CUID_CLICKHOUSE_PASSWORD"
)

DEST_DB = os.getenv(
    f"{ENV}_CUID_CLICKHOUSE_DATABASE"
)

# ============================================================
# TABLES
# ============================================================
TEMP_TABLE = "SBPL_LOS_TEMP"
FINAL_TABLE = "SBPL_LOS"

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 500000))

# ============================================================
# Valkey
# ============================================================

VALKEY_HOST = os.getenv(
    f"{ENV}_VALKEY_HOST"
)

VALKEY_PORT = int(
    os.getenv(
        f"{ENV}_VALKEY_PORT"
    )
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
    job_start_str = job_start.strftime("%Y-%m-%d %H:%M:%S")

    source_count = 0
    temp_count = 0
    inserted_master = 0
    inserted_repository = 0
    pending_repository = 0
    gap_count = 0
    master_inserted = 0

    try:

        # ====================================================
        # SOURCE CONNECTION
        # ====================================================
        logger.info("🔌 Connecting Source ClickHouse...")

        src = clickhouse_connect.get_client(
            host=SRC_HOST,
            port=SRC_PORT,
            username=SRC_USER,
            password=SRC_PASSWORD,
            database=SRC_DB
        )

        # ====================================================
        # DEST CONNECTION
        # ====================================================
        logger.info("🔌 Connecting Destination ClickHouse...")

        dest = clickhouse_connect.get_client(
            host=DEST_HOST,
            port=DEST_PORT,
            username=DEST_USER,
            password=DEST_PASSWORD,
            database=DEST_DB
        )

        # ====================================================
        # STEP 1 : TRUNCATE TEMP TABLE
        # ====================================================
        logger.info("🧹 Truncating Temp Table...")

        execute_with_retry(
            dest,
            f"TRUNCATE TABLE {DEST_DB}.{TEMP_TABLE}"
        )

        # ====================================================
        # STEP 2 : COUNT SOURCE
        # ====================================================
        logger.info("📊 Counting Source Records...")

        count_query = """
        SELECT count()
        FROM
        (
            WITH aggregated_data AS
                (
                    SELECT
                        anyIf(
                            trimBoth(
                                replaceRegexpAll(
                                    concat(
                                        ifNull(fslcd.FIRST_NAME, ''),
                                        ' ',
                                        ifNull(fslcd.MIDDLE_NAME, ''),
                                        ' ',
                                        ifNull(fslcd.LAST_NAME, '')
                                    ),
                                    '\\s+',
                                    ' '
                                )
                            ),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_NAME,

                        anyIf(
                            upper(trimBoth(ifNull(fslcd.PAN_NO, ''))),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_PAN_NO,

                        anyIf(
                            trimBoth(ifNull(fslcd.AADHAAR_NO, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_AADHAAR_NO,

                        anyIf(
                            trimBoth(ifNull(fslcd.VOTER_ID, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_VOTER_ID,

                        anyIf(
                            trimBoth(ifNull(fslcd.DRIVINGLICENSENO, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_DRIVING_LICN_NO,

                        anyIf(
                            trimBoth(ifNull(fslcd.PASSPORTNO, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_PASSPORT,

                        anyIf(
                            trimBoth(ifNull(fslcd.EMP_PH_NO, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_MOBILE_NO,

                        anyIf(
                            trimBoth(ifNull(fslcd.EMAIL_ID, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_EMAIL_ID,

                        anyIf(
                            toString(fslcd.UCIC),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_CIF_NO,

                        anyIf(
                            trimBoth(ifNull(fslcd.CUSTOMERID, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_CUSTID,

                        'APPLICANT' AS Customer_Type,

                        toString(flldv.LEADID) AS raw_APPLICATIONNO,

                        flldv.AGREEMENTNO AS raw_AGREEMENTNO,

                        arrayStringConcat(
                            arrayFilter(
                                x -> x != '',
                                groupUniqArrayIf(
                                    upper(
                                        replaceRegexpAll(
                                            trimBoth(
                                                replaceRegexpAll(
                                                    assumeNotNull(fslcd.PAN_NO),
                                                    '[[:space:]]+',
                                                    ' '
                                                )
                                            ),
                                            '^[, ]+|[, ]+$',
                                            ''
                                        )
                                    ),
                                    fslcd.CUST_TYPE = 'COAPPLICANT'
                                )
                            ),
                            ', '
                        ) AS raw_COAPPLICANT_PAN_NO,

                        toString(flldv.LEADID) AS raw_LEAD_ID,

                        flldv.STATUS AS raw_LEAD_STATUS,

                        ifNull(
                            formatDateTime(
                                toDateTime(flldv.LEAD_GENERATED_DATE),
                                '%Y-%m-%d %H:%i:%S'
                            ),
                            ''
                        ) AS raw_CREATED_DATE,

                        ifNull(
                            formatDateTime(
                                flldv.DISBURSALDATE,
                                '%Y-%m-%d %H:%i:%S'
                            ),
                            ''
                        ) AS raw_SOURCE_BASED_LAST_UPDATED,

                        flldv.TOTAL_ESTIMATE_VALUE AS raw_ASSET_VALUE,

                        any(sbm.BRANCH_REGION) AS raw_BRANCH_REGION

                    FROM sbpl.FACT_LEAD_LOAN_DTLS_VW flldv

                    LEFT JOIN sbpl.FACT_SBPL_LOS_CUST_DTLS fslcd
                        ON flldv.LEADID = fslcd.LEADID

                    LEFT JOIN sbpl.sbpl_Branch_Master sbm
                        ON sbm.BRANCHID = flldv.BRANCHID

                    GROUP BY
                        flldv.LEADID,
                        flldv.AGREEMENTNO,
                        flldv.STATUS,
                        flldv.LEAD_GENERATED_DATE,
                        flldv.DISBURSALDATE,
                        flldv.TOTAL_ESTIMATE_VALUE
                )

                SELECT
                    coalesce(raw_NAME, '') AS NAME,
                    coalesce(raw_PAN_NO, '') AS PAN_NO,
                    coalesce(raw_AADHAAR_NO, '') AS AADHAAR_NO,
                    coalesce(raw_VOTER_ID, '') AS VOTER_ID,
                    coalesce(raw_DRIVING_LICN_NO, '') AS DRIVING_LICN_NO,
                    coalesce(raw_PASSPORT, '') AS PASSPORT,
                    coalesce(raw_MOBILE_NO, '') AS MOBILE_NO,
                    coalesce(raw_EMAIL_ID, '') AS EMAIL_ID,
                    coalesce(raw_CIF_NO, '') AS CIF_NO,
                    coalesce(raw_CUSTID, '') AS CUSTID,
                    Customer_Type,
                    coalesce(raw_APPLICATIONNO, '') AS APPLICATIONNO,
                    coalesce(raw_AGREEMENTNO, '') AS AGREEMENTNO,
                    coalesce(raw_COAPPLICANT_PAN_NO, '') AS COAPPLICANT_PAN_NO,
                    coalesce(raw_LEAD_ID, '') AS LEAD_ID,
                    coalesce(raw_LEAD_STATUS, '') AS LEAD_STATUS,
                    coalesce(raw_CREATED_DATE, '') AS CREATED_DATE,
                    coalesce(raw_SOURCE_BASED_LAST_UPDATED, '') AS SOURCE_BASED_LAST_UPDATED,
                    toString(ifNull(raw_ASSET_VALUE, 0)) AS ASSET_VALUE,
                    coalesce(raw_BRANCH_REGION, '') AS BRANCH_REGION
                FROM aggregated_data
        )
        """

        source_count = src.query(count_query).result_rows[0][0]

        logger.info(
            f"📦 Source Count : "
            f"{source_count:,}"
        )

        # ====================================================
        # STEP 3 : LOAD TEMP TABLE
        # ====================================================
        offset = 0

        while True:

            logger.info(f"📥 Fetching Batch Offset : {offset}")

            select_query = f"""
            WITH aggregated_data AS
                (
                    SELECT
                        anyIf(
                            trimBoth(
                                replaceRegexpAll(
                                    concat(
                                        ifNull(fslcd.FIRST_NAME, ''),
                                        ' ',
                                        ifNull(fslcd.MIDDLE_NAME, ''),
                                        ' ',
                                        ifNull(fslcd.LAST_NAME, '')
                                    ),
                                    '\\s+',
                                    ' '
                                )
                            ),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_NAME,

                        anyIf(
                            upper(trimBoth(ifNull(fslcd.PAN_NO, ''))),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_PAN_NO,

                        anyIf(
                            trimBoth(ifNull(fslcd.AADHAAR_NO, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_AADHAAR_NO,

                        anyIf(
                            trimBoth(ifNull(fslcd.VOTER_ID, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_VOTER_ID,

                        anyIf(
                            trimBoth(ifNull(fslcd.DRIVINGLICENSENO, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_DRIVING_LICN_NO,

                        anyIf(
                            trimBoth(ifNull(fslcd.PASSPORTNO, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_PASSPORT,

                        anyIf(
                            trimBoth(ifNull(fslcd.EMP_PH_NO, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_MOBILE_NO,

                        anyIf(
                            trimBoth(ifNull(fslcd.EMAIL_ID, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_EMAIL_ID,

                        anyIf(
                            toString(fslcd.UCIC),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_CIF_NO,

                        anyIf(
                            trimBoth(ifNull(fslcd.CUSTOMERID, '')),
                            fslcd.CUST_TYPE = 'APPLICANT'
                        ) AS raw_CUSTID,

                        'APPLICANT' AS Customer_Type,

                        toString(flldv.LEADID) AS raw_APPLICATIONNO,

                        flldv.AGREEMENTNO AS raw_AGREEMENTNO,

                        arrayStringConcat(
                            arrayFilter(
                                x -> x != '',
                                groupUniqArrayIf(
                                    upper(
                                        replaceRegexpAll(
                                            trimBoth(
                                                replaceRegexpAll(
                                                    assumeNotNull(fslcd.PAN_NO),
                                                    '[[:space:]]+',
                                                    ' '
                                                )
                                            ),
                                            '^[, ]+|[, ]+$',
                                            ''
                                        )
                                    ),
                                    fslcd.CUST_TYPE = 'COAPPLICANT'
                                )
                            ),
                            ', '
                        ) AS raw_COAPPLICANT_PAN_NO,

                        toString(flldv.LEADID) AS raw_LEAD_ID,

                        flldv.STATUS AS raw_LEAD_STATUS,

                        ifNull(
                            formatDateTime(
                                toDateTime(flldv.LEAD_GENERATED_DATE),
                                '%Y-%m-%d %H:%i:%S'
                            ),
                            ''
                        ) AS raw_CREATED_DATE,

                        ifNull(
                            formatDateTime(
                                flldv.DISBURSALDATE,
                                '%Y-%m-%d %H:%i:%S'
                            ),
                            ''
                        ) AS raw_SOURCE_BASED_LAST_UPDATED,

                        flldv.TOTAL_ESTIMATE_VALUE AS raw_ASSET_VALUE,

                        any(sbm.BRANCH_REGION) AS raw_BRANCH_REGION

                    FROM sbpl.FACT_LEAD_LOAN_DTLS_VW flldv

                    LEFT JOIN sbpl.FACT_SBPL_LOS_CUST_DTLS fslcd
                        ON flldv.LEADID = fslcd.LEADID

                    LEFT JOIN sbpl.sbpl_Branch_Master sbm
                        ON sbm.BRANCHID = flldv.BRANCHID

                    GROUP BY
                        flldv.LEADID,
                        flldv.AGREEMENTNO,
                        flldv.STATUS,
                        flldv.LEAD_GENERATED_DATE,
                        flldv.DISBURSALDATE,
                        flldv.TOTAL_ESTIMATE_VALUE
                )

                SELECT
                    coalesce(raw_NAME, '') AS NAME,
                    coalesce(raw_PAN_NO, '') AS PAN_NO,
                    coalesce(raw_AADHAAR_NO, '') AS AADHAAR_NO,
                    coalesce(raw_VOTER_ID, '') AS VOTER_ID,
                    coalesce(raw_DRIVING_LICN_NO, '') AS DRIVING_LICN_NO,
                    coalesce(raw_PASSPORT, '') AS PASSPORT,
                    coalesce(raw_MOBILE_NO, '') AS MOBILE_NO,
                    coalesce(raw_EMAIL_ID, '') AS EMAIL_ID,
                    coalesce(raw_CIF_NO, '') AS CIF_NO,
                    coalesce(raw_CUSTID, '') AS CUSTID,
                    Customer_Type,
                    coalesce(raw_APPLICATIONNO, '') AS APPLICATIONNO,
                    coalesce(raw_AGREEMENTNO, '') AS AGREEMENTNO,
                    coalesce(raw_COAPPLICANT_PAN_NO, '') AS COAPPLICANT_PAN_NO,
                    coalesce(raw_LEAD_ID, '') AS LEAD_ID,
                    coalesce(raw_LEAD_STATUS, '') AS LEAD_STATUS,
                    coalesce(raw_CREATED_DATE, '') AS CREATED_DATE,
                    coalesce(raw_SOURCE_BASED_LAST_UPDATED, '') AS SOURCE_BASED_LAST_UPDATED,
                    toString(ifNull(raw_ASSET_VALUE, 0)) AS ASSET_VALUE,
                    coalesce(raw_BRANCH_REGION, '') AS BRANCH_REGION
                FROM aggregated_data
            LIMIT {BATCH_SIZE}
            OFFSET {offset}
            """

            query_result = src.query(select_query)

            batch_data = query_result.result_rows

            if not batch_data:
                break

            cleaned_batch = [
                tuple(
                    "" if v is None else str(v).strip()
                    for v in row
                )
                for row in batch_data
            ]

            # ====================================================
            # INSERT TEMP
            # ====================================================
            dest.insert(
                f"{DEST_DB}.{TEMP_TABLE}",
                cleaned_batch,
                column_names=[
                    "NAME",
                    "PAN_NO",
                    "AADHAAR_NO",
                    "VOTER_ID",
                    "DRIVING_LICN_NO",
                    "PASSPORT",
                    "MOBILE_NO",
                    "EMAIL_ID",
                    "CIF_NO",
                    "CUSTID",
                    "Customer_Type",
                    "APPLICATIONNO",
                    "AGREEMENTNO",
                    "COAPPLICANT_PAN_NO",
                    "LEAD_ID",
                    "LEAD_STATUS",
                    "CREATED_DATE",
                    "SOURCE_BASED_LAST_UPDATED",
                    "ASSET_VALUE",
                    "BRANCH_REGION"
                ]
            )

            offset += len(cleaned_batch)

            logger.info(f"✅ Inserted Into Temp : {offset}")

        # ====================================================
        # STEP 4 : LOAD FINAL TABLE
        # ====================================================
        logger.info("🚀 Loading Final Table...")

        master_count = dest.query(
            f"SELECT count() FROM {DEST_DB}.{FINAL_TABLE}"
        ).result_rows[0][0]

        if master_count == 0:

            logger.info("🚀 Initial Load Started")

            execute_with_retry(dest, f"""
                INSERT INTO {DEST_DB}.{FINAL_TABLE}
                (
                    NAME,
                    PAN_NO,
                    AADHAAR_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    PASSPORT,
                    MOBILE_NO,
                    EMAIL_ID,
                    CIF_NO,
                    CUSTID,
                    Customer_Type,
                    APPLICATIONNO,
                    AGREEMENTNO,
                    COAPPLICANT_PAN_NO,
                    LEAD_ID,
                    LEAD_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    ASSET_VALUE,
                    BRANCH_REGION,
                    checksum1,
                    checksum2
                )
                SELECT
                    NAME,
                    PAN_NO,
                    AADHAAR_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    PASSPORT,
                    MOBILE_NO,
                    EMAIL_ID,
                    CIF_NO,
                    CUSTID,
                    Customer_Type,
                    APPLICATIONNO,
                    AGREEMENTNO,
                    COAPPLICANT_PAN_NO,
                    LEAD_ID,
                    LEAD_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    ASSET_VALUE,
                    BRANCH_REGION,
                    checksum1,
                    checksum2
                FROM {DEST_DB}.{TEMP_TABLE}
            """)
        else:
            logger.info("🔄 Incremental Load Started")

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

            logger.info(
                f"🆕 New Records     : "
                f"{new_count:,}"
            )

            logger.info(
                f"🔄 Changed Records : "
                f"{changed_count:,}"
            )

            execute_with_retry(dest, f"""
                INSERT INTO {DEST_DB}.{FINAL_TABLE}
                (
                    NAME,
                    PAN_NO,
                    AADHAAR_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    PASSPORT,
                    MOBILE_NO,
                    EMAIL_ID,
                    CIF_NO,
                    CUSTID,
                    Customer_Type,
                    APPLICATIONNO,
                    AGREEMENTNO,
                    COAPPLICANT_PAN_NO,
                    LEAD_ID,
                    LEAD_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    ASSET_VALUE,
                    BRANCH_REGION,
                    checksum1,
                    checksum2
                )
                SELECT
                    t.NAME,
                    t.PAN_NO,
                    t.AADHAAR_NO,
                    t.VOTER_ID,
                    t.DRIVING_LICN_NO,
                    t.PASSPORT,
                    t.MOBILE_NO,
                    t.EMAIL_ID,
                    t.CIF_NO,
                    t.CUSTID,
                    t.Customer_Type,
                    t.APPLICATIONNO,
                    t.AGREEMENTNO,
                    t.COAPPLICANT_PAN_NO,
                    t.LEAD_ID,
                    t.LEAD_STATUS,
                    t.CREATED_DATE,
                    t.SOURCE_BASED_LAST_UPDATED,
                    t.ASSET_VALUE,
                    t.BRANCH_REGION,
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

        logger.info("✅ Final Table Loaded")

        logger.info(
            f"📦 Inserted Records : "
            f"{new_count + changed_count:,}"
        )

        temp_count = dest.query(
            f"SELECT count() FROM {DEST_DB}.{TEMP_TABLE}"
        ).result_rows[0][0]

        logger.info(
            f"📦 Temp Count : "
            f"{temp_count:,}"
        )

        inserted_master = dest.query(f"""
            SELECT count()
            FROM
            (
                SELECT *
                FROM {DEST_DB}.{FINAL_TABLE}
                ORDER BY version DESC
                LIMIT 1 BY checksum1
            )
        """).result_rows[0][0]

        logger.info(
            f"✅ Records inserted/updated in SBPL_LOS : {inserted_master}"
        )
        # ====================================================
        # STEP 5 : GENERATE CUID
        # ONLY VALID PAN
        # ====================================================
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
        FROM CUID.SBPL_LOS
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
            f"{pending_repository:,}"
        )

        inserted_repository = 0
        gap_count = 0

        if pending_repository > 0:

            repository_before = dest.query("""
            SELECT count()
            FROM CUID.CUID_REPOSITORY
            """).result_rows[0][0]

            start_id, end_id = reserve_cuid_range(
                pending_repository
            )

            logger.info(
                f"🔒 Reserved Range : "
                f"{start_id:,} -> {end_id:,}"
            )

            execute_with_retry(dest, f"""
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
                        OVER (
                            ORDER BY PAN_NO
                        )
                        + {start_id} - 1
                    ),
                    16,
                    '0'
                ) AS CUID,

                now()

            FROM CUID.CUID_NEW_PAN_STAGE
            """)

            repository_after = dest.query("""
            SELECT count()
            FROM CUID.CUID_REPOSITORY
            """).result_rows[0][0]

            inserted_repository = (
                repository_after
                - repository_before
            )

            gap_count = (
                pending_repository
                - inserted_repository
            )

            if gap_count > 0:

                logger.warning(
                    f"⚠ {gap_count:,} PANs reserved "
                    f"but not inserted."
                )

            logger.info(
                f"🆔 Repository Inserts : "
                f"{inserted_repository:,}"
            )

            logger.info(
                f"Pending={pending_repository:,}, "
                f"Inserted={inserted_repository:,}, "
                f"Gap={gap_count:,}"
            )
        else:

            logger.info(
                "✅ No new PANs found"
            )

        # ====================================================
        # STEP 6 : INSERT MASTER TABLE
        # ONLY VALID PAN
        # ====================================================
        logger.info("📦 Loading CUID_MASTER_TABLE...")

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

                'CUSTOMER' AS TYPE,
                'SBPL_LOS' AS SOURCE,
                ifNull(s.NAME, '') AS NAME,

                upper(trimBoth(s.PAN_NO)) AS PAN_NO,

                '' AS GST_NO,

                ifNull(s.AADHAAR_NO, '') AS AADHAAR_NO,

                '' AS CKYCID,
                '' AS TIN_NO,

                ifNull(s.VOTER_ID, '') AS VOTER_ID,

                ifNull(s.DRIVING_LICN_NO, '') AS DRIVING_LICN_NO,

                '' AS RATIONCARD_NO,

                ifNull(s.PASSPORT, '') AS PASSPORT,

                ifNull(s.MOBILE_NO, '') AS MOBILE_NO,

                ifNull(s.EMAIL_ID, '') AS EMAIL_ID,

                ifNull(s.APPLICATIONNO, '') AS APPLICATIONNO,

                ifNull(s.COAPPLICANT_PAN_NO, '') AS COAPPLICANT_PAN_NO,

                '' AS GUARANTOR_PAN_NO,

                ifNull(s.CIF_NO, '') AS CIF_NO,

                ifNull(s.AGREEMENTNO, '') AS AGREEMENTNO,

                '' AS CUST_UNQ_ID,

                ifNull(s.CUSTID, '') AS CUSTID,

                '' AS VENDOR_ID,

                '' AS SOURCE_BASED_STATUS,

                '' AS REGISTRATION_CERTIFICATE,

                '' AS NPA_STAGEID,

                ifNull(s.LEAD_ID, '') AS LEAD_ID,

                ifNull(s.LEAD_STATUS, '') AS LEAD_STATUS,

                ifNull(s.CREATED_DATE, '') AS CREATED_DATE,

                ifNull(s.SOURCE_BASED_LAST_UPDATED, '')
                    AS SOURCE_BASED_LAST_UPDATED,

                ifNull(toString(s.ASSET_VALUE), '')
                    AS ASSET_VALUE,

                '' AS DATE_OF_INCORPORATION,

                '' AS USER_EMPLOYEE_NO,

                '' AS DATE_OF_BIRTH,

                '' AS DATE_OF_JOINING,

                ifNull(s.BRANCH_REGION, '')
                    AS ALLOCATION_REGION_CODE,

                ifNull(s.BRANCH_REGION, '')
                    AS ALLOCATION_REGION_TYPE,

                r.CUID,

                r.CUID_CREATION_DATE,

                'Loaded From SBPL LOS ETL'
                    AS REMARKS

            FROM
                (
                    SELECT *
                    FROM CUID.SBPL_LOS
                    ORDER BY version DESC
                    LIMIT 1 BY checksum1
                ) s
                INNER JOIN CUID.CUID_REPOSITORY r
                    ON upper(trimBoth(s.PAN_NO)) = upper(trimBoth(r.PAN_NO))
                LEFT ANTI JOIN CUID.CUID_MASTER_TABLE cm
                    ON upper(trimBoth(s.PAN_NO)) = upper(trimBoth(cm.PAN_NO))
                AND cm.SOURCE = 'SBPL_LOS'
                WHERE
                    s.PAN_NO IS NOT NULL
                    AND trimBoth(s.PAN_NO) != ''
                    AND match(
                        upper(trimBoth(s.PAN_NO)),
                        '^[A-Z]{5}[0-9]{4}[A-Z]$'
                    )
        """)

        logger.info("✅ CUID_MASTER_TABLE Loaded")

        master_inserted = dest.query("""
            SELECT count()
            FROM CUID.CUID_MASTER_TABLE
            WHERE SOURCE = 'SBPL_LOS'
            AND LastUpdated >= today()
            AND LastUpdated < today() + 1
            """).result_rows[0][0]

        logger.info(
            f"📦 CUID_MASTER_TABLE Rows Inserted : "
            f"{master_inserted:,}"
        )

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

        total_time = round(
            time.time() - start_time,
            2
        )

        logger.info(
            "========== ETL SUMMARY =========="
        )

        logger.info(
            f"📦 Source Count              : "
            f"{source_count:,}"
        )

        logger.info(
            f"📦 Temp Count                : "
            f"{temp_count:,}"
        )

        logger.info(
            f"📦 Final Count               : "
            f"{inserted_master:,}"
        )

        logger.info(
            f"📊 Repository Pending        : "
            f"{pending_repository:,}"
        )

        logger.info(
            f"🆔 Repository Inserted       : "
            f"{inserted_repository:,}"
        )

        logger.info(
            f"⚠ Repository Gap            : "
            f"{gap_count:,}"
        )

        logger.info(
            f"📦 Master Table Inserted     : "
            f"{master_inserted:,}"
        )

        logger.info(
            "================================="
        )

        logger.info(
            "🎯 ETL COMPLETED SUCCESSFULLY"
        )

        logger.info(
            f"⏱ Total Time : "
            f"{total_time} sec"
        )

    except Exception as e:

        exc_type, exc_value, exc_traceback = sys.exc_info()

        line_number = exc_traceback.tb_lineno

        logger.error(
            f"❌ ETL FAILED at line {line_number}: {e}"
        )

        logger.error(traceback.format_exc())

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    run_etl()