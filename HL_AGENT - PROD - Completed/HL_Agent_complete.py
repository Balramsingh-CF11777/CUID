import os
import pyodbc
import clickhouse_connect
import logging
import time
import uuid

from dotenv import load_dotenv
from datetime import datetime
from retrying import retry

import valkey
from valkey.connection import BlockingConnectionPool

# ============================================================
# ENV LOAD
# ============================================================
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# MSSQL CONFIG
# ============================================================
MSSQL_HOST = os.getenv("MSSQL_HL_HOST")
MSSQL_PORT = os.getenv("MSSQL_HL_PORT")
MSSQL_DB = os.getenv("MSSQL_HL_DATABASE")
MSSQL_USER = os.getenv("MSSQL_HL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_HL_PASSWORD")

# ============================================================
# CLICKHOUSE CONFIG
# ============================================================
ENV = os.getenv("ENVIRON", "UAT").upper()

CH_HOST = os.getenv(
    f"{ENV}_CUID_CLICKHOUSE_HOST"
)

CH_PORT = int(
    os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT")
)

CH_USER = os.getenv(
    f"{ENV}_CUID_CLICKHOUSE_USER"
)

CH_PASSWORD = os.getenv(
    f"{ENV}_CUID_CLICKHOUSE_PASSWORD"
)

CH_DB = os.getenv(
    f"{ENV}_CUID_CLICKHOUSE_DATABASE"
)

TEMP_TABLE = "HL_AGENT_TEMP"
FINAL_TABLE = "HL_AGENT"

BATCH_SIZE = 100000

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

    job_id = f"HL_AGENT_{uuid.uuid4()}"

    job_start_str = job_start.strftime("%Y-%m-%d %H:%M:%S")

    source_count = 0
    temp_count = 0
    inserted_master = 0
    inserted_repository = 0

    mssql_conn = None
    cursor = None

    try:
        # =====================================================
        # MSSQL CONNECTION
        # =====================================================
        logger.info("Connecting MSSQL...")

        mssql_conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
            f"DATABASE={MSSQL_DB};"
            f"UID={MSSQL_USER};"
            f"PWD={MSSQL_PASSWORD};"
            f"Connection Timeout=60;"
        )

        cursor = mssql_conn.cursor()

        # =====================================================
        # MSSQL QUERY
        # =====================================================
        query = r"""
            SELECT
                UPPER(A.Category) AS TYPE,
                A.AgentName AS NAME,
                CASE
                    WHEN CAST(A.DateOfBirth AS DATE) = '1900-01-01' THEN NULL
                    ELSE CONVERT(VARCHAR(19), A.DateOfBirth, 120)
                END AS DATE_OF_BIRTH,
                CASE
                    WHEN PATINDEX(
                    '%[A-Z][A-Z][A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9][A-Z]%',
                    UPPER(ISNULL(A.PAN, ''))
                    ) > 0 THEN SUBSTRING(
                    UPPER(A.PAN),
                    PATINDEX(
                        '%[A-Z][A-Z][A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9][A-Z]%',
                        UPPER(ISNULL(A.PAN, ''))
                    ),
                    10
                    )
                    ELSE NULL
                END AS PAN_NO,
                A.Mobile AS MOBILE_NO,
                A.Email AS EMAIL_ID,
                A.Branch AS ALLOCATION_REGION_CODE,
                A.AgentStatus AS SOURCE_BASED_STATUS,
                CONVERT(VARCHAR(19), A.CrtdDt, 120) AS CREATED_DATE,
                CONVERT(VARCHAR(19), A.LstModDt, 120) AS SOURCE_BASED_LAST_UPDATED
                FROM
                devhfc02..Sam_tdsagentmst A
            """

        logger.info("Executing MSSQL Query...")

        cursor.execute(query)

        columns = [col[0] for col in cursor.description]

        # =====================================================
        # CLICKHOUSE CONNECTION
        # =====================================================
        logger.info("🔌 Connecting ClickHouse...")

        ch = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DB
        )

        # =====================================================
        # STEP 1 : TRUNCATE TEMP
        # =====================================================
        logger.info("Truncating temp table...")

        execute_with_retry(
            ch,
            f"TRUNCATE TABLE {CH_DB}.{TEMP_TABLE}"
        )

        # =====================================================
        # STEP 2 : LOAD TEMP TABLE
        # =====================================================
        logger.info("Loading temp table...")

        batch = []
        total_rows = 0

        for row in cursor:

            total_rows += 1

            cleaned = []

            for col in row:

                if col is None:
                    cleaned.append(None)

                elif isinstance(col, datetime):
                    cleaned.append(
                        col.strftime("%Y-%m-%d %H:%M:%S")
                    )

                else:
                    cleaned.append(str(col).strip())

            batch.append(cleaned)

            if len(batch) >= BATCH_SIZE:

                ch.insert(
                    f"{CH_DB}.{TEMP_TABLE}",
                    batch,
                    column_names=columns
                )

                logger.info(
                    f"Inserted {total_rows} rows"
                )

                batch.clear()

        if batch:

            ch.insert(
                f"{CH_DB}.{TEMP_TABLE}",
                batch,
                column_names=columns
            )

        source_count = total_rows

        logger.info("Temp Load Completed")

        temp_count = ch.query(f"""
            SELECT count()
            FROM {CH_DB}.{TEMP_TABLE}
        """).result_rows[0][0]

        # =====================================================
        # STEP 3 : INSERT INTO FINAL TABLE
        # =====================================================
        logger.info("Loading final table...")

        master_count = ch.query(f"""
            SELECT count()
            FROM {CH_DB}.{FINAL_TABLE}
        """).result_rows[0][0]

        if master_count == 0:
                logger.info("🚀 Initial Load")

                execute_with_retry(ch, f"""
                    INSERT INTO {CH_DB}.{FINAL_TABLE}
                    (
                        TYPE,
                        NAME,
                        DATE_OF_BIRTH,
                        PAN_NO,
                        MOBILE_NO,
                        EMAIL_ID,
                        ALLOCATION_REGION_CODE,
                        SOURCE_BASED_STATUS,
                        CREATED_DATE,
                        SOURCE_BASED_LAST_UPDATED,
                        checksum1,
                        checksum2
                    )
                    SELECT
                        TYPE,
                        NAME,
                        DATE_OF_BIRTH,
                        PAN_NO,
                        MOBILE_NO,
                        EMAIL_ID,
                        ALLOCATION_REGION_CODE,
                        SOURCE_BASED_STATUS,
                        CREATED_DATE,
                        SOURCE_BASED_LAST_UPDATED,
                        checksum1,
                        checksum2
                    FROM {CH_DB}.{TEMP_TABLE}
                    """)

        else:

            logger.info("🔄 Incremental Load")
            new_count = ch.query(f"""
                SELECT count()
                FROM {CH_DB}.{TEMP_TABLE} t
                LEFT JOIN
                (
                    SELECT DISTINCT checksum1
                    FROM {CH_DB}.{FINAL_TABLE}
                ) m
                ON t.checksum1 = m.checksum1
                WHERE m.checksum1 IS NULL
            """).result_rows[0][0]

            changed_count = ch.query(f"""
                SELECT count()
                FROM {CH_DB}.{TEMP_TABLE} t
                INNER JOIN
                (
                    SELECT
                        checksum1,
                        argMax(checksum2, version) AS checksum2
                    FROM {CH_DB}.{FINAL_TABLE}
                    GROUP BY checksum1
                ) m
                ON t.checksum1 = m.checksum1
                WHERE t.checksum2 != m.checksum2
            """).result_rows[0][0]

            logger.info(f"🆕 New Records     : {new_count}")
            logger.info(f"🔄 Changed Records : {changed_count}")

            execute_with_retry(ch, f"""
                INSERT INTO {CH_DB}.{FINAL_TABLE}
                (
                    TYPE,
                    NAME,
                    DATE_OF_BIRTH,
                    PAN_NO,
                    MOBILE_NO,
                    EMAIL_ID,
                    ALLOCATION_REGION_CODE,
                    SOURCE_BASED_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    checksum1,
                    checksum2
                )
                SELECT
                    t.TYPE,
                    t.NAME,
                    t.DATE_OF_BIRTH,
                    t.PAN_NO,
                    t.MOBILE_NO,
                    t.EMAIL_ID,
                    t.ALLOCATION_REGION_CODE,
                    t.SOURCE_BASED_STATUS,
                    t.CREATED_DATE,
                    t.SOURCE_BASED_LAST_UPDATED,
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

            logger.info(
                f"📦 Inserted Records: {new_count + changed_count}"
            )

        # =====================================================
        # STEP 4 : CUID GENERATION
        # =====================================================
        logger.info(
            "🆔 Preparing PAN staging table..."
        )

        execute_with_retry(
            ch,
            "TRUNCATE TABLE CUID.CUID_NEW_PAN_STAGE"
        )

        execute_with_retry(ch, """
        INSERT INTO CUID.CUID_NEW_PAN_STAGE
        SELECT DISTINCT
            upper(trimBoth(PAN_NO))
        FROM CUID.HL_AGENT
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

        pending_repository = ch.query("""
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

                toString(now())

            FROM
            (
                SELECT DISTINCT s.PAN_NO
                FROM CUID.CUID_NEW_PAN_STAGE s

                LEFT ANTI JOIN CUID.CUID_REPOSITORY r
                    ON s.PAN_NO = r.PAN_NO
            ) t
            """

            logger.info(repository_sql)

            execute_with_retry(
                ch,
                repository_sql
            )

            inserted_repository = ch.query("""
                SELECT countDistinct(s.PAN_NO)
                FROM
                (
                    SELECT DISTINCT PAN_NO
                    FROM CUID.CUID_NEW_PAN_STAGE
                ) s

                INNER JOIN CUID.CUID_REPOSITORY r
                    ON s.PAN_NO = r.PAN_NO
            """).result_rows[0][0]

        else:

            logger.info(
                "✅ No new PANs found"
            )

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
        # STEP 5 : INSERT INTO CUID MASTER
        # =====================================================
        logger.info("Inserting into CUID_MASTER_TABLE...")

        execute_with_retry(ch, """
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
                ifNull(p.TYPE, 'HL_AGENT') AS TYPE,
                'HL_AGENT' AS SOURCE,
                ifNull(p.NAME, '') AS NAME,
                upper(trimBoth(p.PAN_NO)) AS PAN_NO,

                '' AS GST_NO,
                '' AS AADHAAR_NO,
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

                ifNull(p.SOURCE_BASED_STATUS, '') AS SOURCE_BASED_STATUS,

                '' AS REGISTRATION_CERTIFICATE,
                '' AS NPA_STAGEID,
                '' AS LEAD_ID,
                '' AS LEAD_STATUS,

                ifNull(p.CREATED_DATE, '') AS CREATED_DATE,

                ifNull(
                    p.SOURCE_BASED_LAST_UPDATED,
                    ''
                ) AS SOURCE_BASED_LAST_UPDATED,

                '' AS ASSET_VALUE,
                '' AS DATE_OF_INCORPORATION,
                '' AS USER_EMPLOYEE_NO,

                ifNull(
                    p.DATE_OF_BIRTH,
                    ''
                ) AS DATE_OF_BIRTH,

                '' AS DATE_OF_JOINING,

                ifNull(
                    p.ALLOCATION_REGION_CODE,
                    ''
                ) AS ALLOCATION_REGION_CODE,

                '' AS ALLOCATION_REGION_TYPE,

                r.CUID,
                r.CUID_CREATION_DATE,

                'Loaded from HL_AGENT' AS REMARKS

            FROM
            (
                SELECT *
                FROM CUID.HL_AGENT
                ORDER BY version DESC
                LIMIT 1 BY checksum1
            ) p

            INNER JOIN CUID.CUID_REPOSITORY r
                ON upper(trimBoth(p.PAN_NO))
                = upper(trimBoth(r.PAN_NO))

            LEFT ANTI JOIN CUID.CUID_MASTER_TABLE cm
                ON upper(trimBoth(p.PAN_NO))
                = upper(trimBoth(cm.PAN_NO))
            AND cm.SOURCE = 'HL_AGENT'

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
        # STEP 6 : AUDIT LOG
        # =====================================================
        duration_minutes = round(
            (time.time() - start_time) / 60,
            2
        )

        execute_with_retry(ch, f"""
            INSERT INTO CUID.ETL_AUDIT_LOG
            VALUES
            (
                '{job_id}',
                'HL_AGENT',
                toDateTime('{job_start_str}'),
                now(),
                {source_count},
                {temp_count},
                {inserted_master},
                {inserted_repository},
                'SUCCESS',
                ''
            )
        """)

        logger.info("🎯 ETL Completed Successfully")
        logger.info(f"⏱ Duration: {duration_minutes} minutes")

    except Exception as e:

        logger.error(f"❌ ETL FAILED: {e}")

    finally:

        try:

            if cursor is not None:
                cursor.close()

            if mssql_conn is not None:
                mssql_conn.close()

        except Exception as e:

            logger.error(
                f"❌ Connection Close Failure: {e}"
            )

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    run_etl()