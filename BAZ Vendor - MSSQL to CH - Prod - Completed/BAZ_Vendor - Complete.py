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

# =========================
# MSSQL CONFIG
# =========================
MSSQL_HOST = os.getenv("MSSQL_VENDORMASTER_HOST")
MSSQL_PORT = os.getenv("MSSQL_VENDORMASTER_PORT")
MSSQL_DB = os.getenv("MSSQL_VENDORMASTER_DATABASE")
MSSQL_USER = os.getenv("MSSQL_VENDORMASTER_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_VENDORMASTER_PASSWORD")
MSSQL_TABLE = os.getenv("MSSQL_VENDORMASTER_TABLE_NAME")

# ============================================================
# CLICKHOUSE CONFIG
# ============================================================

ENV = os.getenv("ENVIRON", "UAT").upper()

CH_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
CH_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
CH_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
CH_PASSWORD = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD")
CH_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE")

TEMP_TABLE = "BAZ_Vendor_Master_Temp"
FINAL_TABLE = "BAZ_Vendor_Master"

BATCH_SIZE = 10000

# ============================================================
# VALKEY CONFIG
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
    job_id = f"BAZ_VENDOR_{uuid.uuid4()}"
    job_start_str = job_start.strftime("%Y-%m-%d %H:%M:%S")

    source_count = 0
    temp_count = 0
    inserted_master = 0
    inserted_repository = 0

    try:
        # =====================================================
        # CONNECT MSSQL
        # =====================================================
        logger.info("🔌 Connecting to MSSQL...")
        mssql_conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
            f"DATABASE={MSSQL_DB};"
            f"UID={MSSQL_USER};"
            f"PWD={MSSQL_PASSWORD}"
        )

        cursor = mssql_conn.cursor()

        query = """
            SELECT
                UPPER(v.PAN_No) AS PAN_NO,
                v.Vendor_Name AS NAME,
                v.GSTIN_No AS GST_NO,
                COALESCE(v.VAT_No, v.CST_No) AS TIN_NO,
                v.Cnt_MobileNo AS MOBILE_NO,
                v.EMail_ID AS EMAIL_ID,
                '' AS CUSTID,
                v.VENDOR_ID AS VENDOR_ID,

                CASE
                    WHEN v.Vendor_IsActive = 1 THEN 'Active'
                    WHEN v.Vendor_IsActive = 0 THEN 'Inactive'
                    ELSE 'Unknown'
                END AS SOURCE_BASED_STATUS,

                CONVERT(
                    VARCHAR(19),
                    CAST(
                        STUFF(
                            STUFF(
                                STUFF(
                                    STUFF(v.Created_Date, 5, 0, '-'),
                                8, 0, '-'),
                            11, 0, ' '),
                        14, 0, ':') + ':00'
                        AS DATETIME
                    ),
                    120
                ) AS CREATED_DATE,

                CONVERT(
                    VARCHAR(19),
                    CAST(
                        STUFF(
                            STUFF(
                                STUFF(
                                    STUFF(v.Modified_Date, 5, 0, '-'),
                                8, 0, '-'),
                            11, 0, ' '),
                        14, 0, ':') + ':00'
                        AS DATETIME
                    ),
                    120
                ) AS SOURCE_BASED_LAST_UPDATED,

                v.Addr_StateCode AS ALLOCATION_REGION_CODE,
                v.Addr_StateType AS ALLOCATION_REGION_TYPE

            FROM BAZ_CIFCL.dbo.Vendor_Master v WITH (NOLOCK)
            WHERE v.Company_Code = 'A'
        """

        logger.info("📥 Executing MSSQL Query...")
        cursor.execute(query)

        columns = [col[0] for col in cursor.description]

        # =====================================================
        # CONNECT CLICKHOUSE
        # =====================================================
        logger.info("🔌 Connecting to ClickHouse...")
        ch = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DB
        )

        # =====================================================
        # STEP 1: TRUNCATE TEMP
        # =====================================================
        logger.info("🧹 Truncating temp table...")
        execute_with_retry(ch, f"TRUNCATE TABLE {CH_DB}.{TEMP_TABLE}")

        # =====================================================
        # STEP 2: LOAD INTO TEMP
        # =====================================================
        batch = []
        total_rows = 0

        for row in cursor:
            total_rows += 1
            cleaned = []

            for col in row:
                if col is None:
                    cleaned.append(None)
                elif isinstance(col, datetime):
                    cleaned.append(col)   # keep datetime object
                else:
                    cleaned.append(str(col).strip())

            batch.append(cleaned)

            if len(batch) >= BATCH_SIZE:
                ch.insert(
                    f"{CH_DB}.{TEMP_TABLE}",
                    batch,
                    column_names=columns
                )
                batch.clear()
                logger.info(f"Inserted {total_rows} rows...")

        if batch:
            ch.insert(
                f"{CH_DB}.{TEMP_TABLE}",
                batch,
                column_names=columns
            )

        source_count = total_rows
        logger.info("✅ Temp Load Completed")

        temp_count = ch.query(
            f"SELECT count() FROM {CH_DB}.{TEMP_TABLE}"
        ).result_rows[0][0]
        # =====================================================
        # STEP 3: SMART INSERT (INITIAL vs INCREMENTAL)
        # =====================================================
        master_count = ch.query(
            f"SELECT count() FROM {CH_DB}.{FINAL_TABLE}"
        ).result_rows[0][0]

        if master_count == 0:
            logger.info("🚀 Initial Load → Direct Insert")

            execute_with_retry(ch, f"""
                INSERT INTO {CH_DB}.{FINAL_TABLE}
                (
                    PAN_NO,
                    NAME,
                    GST_NO,
                    TIN_NO,
                    MOBILE_NO,
                    EMAIL_ID,
                    CUSTID,
                    VENDOR_ID,
                    SOURCE_BASED_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    ALLOCATION_REGION_CODE,
                    ALLOCATION_REGION_TYPE,
                    checksum1,
                    checksum2
                )

                SELECT
                    PAN_NO,
                    NAME,
                    GST_NO,
                    TIN_NO,
                    MOBILE_NO,
                    EMAIL_ID,
                    CUSTID,
                    VENDOR_ID,
                    SOURCE_BASED_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    ALLOCATION_REGION_CODE,
                    ALLOCATION_REGION_TYPE,
                    checksum1,
                    checksum2

                FROM {CH_DB}.{TEMP_TABLE}
            """)
        else:
            logger.info("🔄 Incremental Load → Checksum Based Insert")

            execute_with_retry(ch, f"""
                INSERT INTO {CH_DB}.{FINAL_TABLE}
                (
                    PAN_NO,
                    NAME,
                    GST_NO,
                    TIN_NO,
                    MOBILE_NO,
                    EMAIL_ID,
                    CUSTID,
                    VENDOR_ID,
                    SOURCE_BASED_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    ALLOCATION_REGION_CODE,
                    ALLOCATION_REGION_TYPE,
                    checksum1,
                    checksum2
                )

                SELECT
                    t.PAN_NO,
                    t.NAME,
                    t.GST_NO,
                    t.TIN_NO,
                    t.MOBILE_NO,
                    t.EMAIL_ID,
                    t.CUSTID,
                    t.VENDOR_ID,
                    t.SOURCE_BASED_STATUS,
                    t.CREATED_DATE,
                    t.SOURCE_BASED_LAST_UPDATED,
                    t.ALLOCATION_REGION_CODE,
                    t.ALLOCATION_REGION_TYPE,
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

        inserted_master = ch.query(f"""
            SELECT count()
            FROM {CH_DB}.{FINAL_TABLE}
            
        """).result_rows[0][0]
# WHERE LastUpdated >= toDateTime('{job_start_str}')
        # =====================================================
        # STEP 4 : CUID GENERATION (VALKEY)
        # =====================================================

        logger.info(
            "🆔 Preparing PAN staging table..."
        )

        execute_with_retry(
            ch,
            "TRUNCATE TABLE CUID.CUID_NEW_PAN_STAGE"
        )

        execute_with_retry(ch, f"""
        INSERT INTO CUID.CUID_NEW_PAN_STAGE
        SELECT DISTINCT
            upper(
                replaceRegexpAll(
                    trimBoth(PAN_NO),
                    '[^A-Z0-9]',
                    ''
                )
            ) AS PAN_NO
        FROM {CH_DB}.{FINAL_TABLE}
        WHERE
            PAN_NO IS NOT NULL
            AND trimBoth(PAN_NO) != ''
            AND match(
                upper(
                    replaceRegexpAll(
                        trimBoth(PAN_NO),
                        '[^A-Z0-9]',
                        ''
                    )
                ),
                '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
            )
            AND upper(
                replaceRegexpAll(
                    trimBoth(PAN_NO),
                    '[^A-Z0-9]',
                    ''
                )
            )
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
            f"Pending PANs : {pending_repository}"
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

            logger.info(repository_sql)

            execute_with_retry(
                ch,
                repository_sql
            )

            inserted_repository = ch.query("""
                SELECT count(DISTINCT s.PAN_NO)
                FROM
                (
                    SELECT DISTINCT PAN_NO
                    FROM CUID.CUID_NEW_PAN_STAGE
                ) s

                INNER JOIN
                CUID.CUID_REPOSITORY r
                    ON s.PAN_NO = r.PAN_NO
            """).result_rows[0][0]

        logger.info(
            f"Repository Inserts : {inserted_repository}"
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
        # STEP 5: INSERT INTO CUID_MASTER_TABLE
        # =====================================================
        logger.info("📦 Inserting into CUID_MASTER_TABLE...")

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
                'VENDOR' AS TYPE,
                'BAZ_VENDOR' AS SOURCE,

                ifNull(p.NAME, '') AS NAME,
                upper(trimBoth(p.PAN_NO)) AS PAN_NO,

                ifNull(p.GST_NO, '') AS GST_NO,
                '' AS AADHAAR_NO,
                '' AS CKYCID,
                ifNull(p.TIN_NO, '') AS TIN_NO,
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

                ifNull(p.CUSTID, '') AS CUSTID,
                ifNull(p.VENDOR_ID, '') AS VENDOR_ID,
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
                '' AS DATE_OF_BIRTH,
                '' AS DATE_OF_JOINING,

                ifNull(
                    p.ALLOCATION_REGION_CODE,
                    ''
                ) AS ALLOCATION_REGION_CODE,

                ifNull(
                    p.ALLOCATION_REGION_TYPE,
                    ''
                ) AS ALLOCATION_REGION_TYPE,

                r.CUID AS CUID,
                r.CUID_CREATION_DATE AS CUID_CREATION_DATE,

                'Loaded from BAZ Vendor Master' AS REMARKS

            FROM CUID.BAZ_Vendor_Master p

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
        # STEP 6: AUDIT LOG
        # =====================================================
        duration_minutes = round((time.time() - start_time) / 60, 2)

        execute_with_retry(ch, f"""
        INSERT INTO CUID.ETL_AUDIT_LOG
        VALUES
        (
            '{job_id}',
            'BAZ_VENDOR',
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

        logger.info("🎯 MSSQL → CH ETL Completed Successfully")
        logger.info(f"⏱ Duration: {duration_minutes} minutes")

    except Exception as e:
        logger.error(f"❌ ETL FAILED: {e}")

    finally:
        try:
            cursor.close()
            mssql_conn.close()
        except Exception as e:
            logger.error(f"❌ Connection Failure: {e}")


if __name__ == "__main__":
    run_etl()