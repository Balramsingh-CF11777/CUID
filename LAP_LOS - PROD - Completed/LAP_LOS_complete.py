import os
import pyodbc
import clickhouse_connect
import logging
import time
import uuid
import valkey
from valkey.connection import BlockingConnectionPool

from dotenv import load_dotenv
from datetime import datetime
from retrying import retry

# ============================================================
# ENV LOAD
# ============================================================
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# MSSQL CONFIG
# ============================================================
MSSQL_HOST = os.getenv("MSSQL_LAP_HOST")
MSSQL_PORT = os.getenv("MSSQL_LAP_PORT")
MSSQL_DB = os.getenv("MSSQL_LAP_DATABASE")
MSSQL_USER = os.getenv("MSSQL_LAP_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_LAP_PASSWORD")

# ============================================================
# CLICKHOUSE CONFIG
# ============================================================
ENV = os.getenv("ENVIRON", "UAT").upper()

CH_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
CH_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
CH_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
CH_PASSWORD = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD")
CH_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE")

TEMP_TABLE = "LAP_LOS_TEMP"
FINAL_TABLE = "LAP_LOS"

BATCH_SIZE = 100000

# ============================================================
# VALKEY
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

    job_id = f"LAP_LOS_{uuid.uuid4()}"

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
        query = """
            WITH BASE AS
                (
                    SELECT DISTINCT

                        LTRIM(RTRIM(
                            CONCAT(
                                ISNULL(CU.CBSI_CUSFISNAM, ''),
                                ' ',
                                ISNULL(CU.CBSI_CUSLSTNAM, '')
                            )
                        )) AS NAME,

                        ISNULL(CU.CBSI_PAN, '')                    AS PAN_NO,
                        ISNULL(CU.CBSI_GSTNUM, '')                 AS GST_NO,
                        ISNULL(CU.CBSI_AADHAR, '')                 AS AADHAAR_NO,
                        ISNULL(CU.CBSI_VOTERID, '')                AS VOTER_ID,
                        ISNULL(CU.CBSI_DRIVLICNS, '')              AS DRIVING_LICN_NO,
                        ISNULL(CU.CBSI_PASSPORT, '')               AS PASSPORT,
                        ISNULL(CA.CADI_MOBNOI, '')                 AS MOBILE_NO,
                        ISNULL(CA.CADI_EMAILID, '')                AS EMAIL_ID,
                        ISNULL(CU.CBSI_APPLCNTNO, '')              AS APPLICATIONNO,
                        ISNULL(CO.COBI_PAN, '')                    AS COAPPLICANT_PAN_NO,
                        ISNULL(GU.GRBI_PAN, '')                    AS GUARANTOR_PAN_NO,
                        ISNULL(T.AcctID, '')                       AS AGREEMENTNO,
                        ISNULL(CU.CBSI_CUSID, '')                  AS CUSTID,
                        ISNULL(CU.CBSI_APPLCNTNO, '')              AS LEAD_ID,
                        ISNULL(LON.LBSI_APPSTATUS, '')             AS LEAD_STATUS,
                        
                        ISNULL(CONVERT(VARCHAR(19), CU.CBSI_DTCREATED, 120), '')
                                                                    AS CREATED_DATE,
                        ISNULL(CONVERT(VARCHAR(19), LON.LBSI_DISBBUSDT, 120), '')
                                                                    AS SOURCE_BASED_LAST_UPDATED,
                        ISNULL(CONVERT(VARCHAR(19), CU.CBSI_APPLCNTDATE, 120), '')
                                                                    AS DATE_OF_INCORPORATION,
                        ISNULL(CONVERT(VARCHAR(19), CU.CBSI_DOB, 120), '')
                                                                    AS DATE_OF_BIRTH,
                        ISNULL(BR.REGION, '')                      AS ALLOCATION_REGION_CODE,
                        ISNULL(BR.ZONE, '')                        AS ALLOCATION_REGION_TYPE

                    FROM TPLSW..LSW_TCUSBASICINFO CU

                    LEFT JOIN TPLSW..LSW_TCUSADDRINFO CA
                        ON CA.CADI_PRCSID = CU.CBSI_PRCSID

                    LEFT JOIN TPLSW..LSW_TCOCUSBASICINFO CO
                        ON CO.COBI_PRCSID = CU.CBSI_PRCSID

                    LEFT JOIN TPLSW..LSW_TGURCUSBASICINFO GU
                        ON GU.GRBI_PRCSID = CU.CBSI_PRCSID

                    LEFT JOIN TPLSW..LSW_TLONBASICINFO LON
                        ON LON.LBSI_PRCSID = CU.CBSI_PRCSID

                    LEFT JOIN CHOLA_HE..SAM_TBRMST BR
                        ON BR.BrId = CU.CBSI_BRANCH

                    INNER JOIN CHOLA_HE..LON_TINSTENT T
                        ON T.AlterAcctID = CU.CBSI_APPLCNTNO
                    AND T.AcctID <> CU.CBSI_APPLCNTNO

                    WHERE CU.CBSI_APPLCNTNO IS NOT NULL
                )

                SELECT
                    NAME,
                    PAN_NO,
                    GST_NO,
                    AADHAAR_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    PASSPORT,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,

                    STRING_AGG(
                        COALESCE(NULLIF(COAPPLICANT_PAN_NO, ''), NULL),
                        ','
                    ) AS COAPPLICANT_PAN_NO,

                    STRING_AGG(
                        COALESCE(NULLIF(GUARANTOR_PAN_NO, ''), NULL),
                        ','
                    ) AS GUARANTOR_PAN_NO,

                    AGREEMENTNO,
                    CUSTID,
                    LEAD_ID,
                    LEAD_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    DATE_OF_INCORPORATION,
                    DATE_OF_BIRTH,
                    ALLOCATION_REGION_CODE,
                    ALLOCATION_REGION_TYPE

                FROM BASE

                GROUP BY
                    NAME,
                    PAN_NO,
                    GST_NO,
                    AADHAAR_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    PASSPORT,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,
                    AGREEMENTNO,
                    CUSTID,
                    LEAD_ID,
                    LEAD_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    DATE_OF_INCORPORATION,
                    DATE_OF_BIRTH,
                    ALLOCATION_REGION_CODE,
                    ALLOCATION_REGION_TYPE
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
                    NAME,
                    PAN_NO,
                    GST_NO,
                    AADHAAR_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    PASSPORT,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,
                    COAPPLICANT_PAN_NO,
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
                    checksum1,
                    checksum2
                )
                SELECT       
                    NAME,
                    PAN_NO,
                    GST_NO,
                    AADHAAR_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    PASSPORT,
                    MOBILE_NO,
                    EMAIL_ID,
                    APPLICATIONNO,
                    COAPPLICANT_PAN_NO,
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
                NAME,
                PAN_NO,
                GST_NO,
                AADHAAR_NO,
                VOTER_ID,
                DRIVING_LICN_NO,
                PASSPORT,
                MOBILE_NO,
                EMAIL_ID,
                APPLICATIONNO,
                COAPPLICANT_PAN_NO,
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
                checksum1,
                checksum2
            )
            SELECT
                t.NAME,
                t.PAN_NO,
                t.GST_NO,
                t.AADHAAR_NO,
                t.VOTER_ID,
                t.DRIVING_LICN_NO,
                t.PASSPORT,
                t.MOBILE_NO,
                t.EMAIL_ID,
                t.APPLICATIONNO,
                t.COAPPLICANT_PAN_NO,
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
        # STEP 4 : CUID REPOSITORY (VALKEY)
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
            upper(trimBoth(PAN_NO))
        FROM {CH_DB}.{FINAL_TABLE}
        FINAL
        WHERE
            PAN_NO IS NOT NULL
            AND trimBoth(PAN_NO) != ''
            AND match(
                upper(trimBoth(PAN_NO)),
                '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
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
            f"{pending_repository:,}"
        )

        inserted_repository = 0
        gap_count = 0

        if pending_repository > 0:

            repository_before = ch.query("""
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

            execute_with_retry(ch, f"""
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

            FROM
            (
                SELECT DISTINCT s.PAN_NO
                FROM CUID.CUID_NEW_PAN_STAGE s

                LEFT ANTI JOIN
                CUID.CUID_REPOSITORY r
                    ON s.PAN_NO = r.PAN_NO
            ) t
            """)

            repository_after = ch.query("""
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

            logger.info(
                f"🆔 Repository Inserts : "
                f"{inserted_repository:,}"
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
        # STEP 5 : INSERT INTO CUID MASTER
        # =====================================================
        logger.info("Inserting into CUID_MASTER_TABLE...")

        execute_with_retry(ch, """
            INSERT INTO CUID.CUID_MASTER_TABLE
            (
                TYPE,
                SOURCE,
                PAN_NO,
                GST_NO,
                AADHAAR_NO,
                VOTER_ID,
                DRIVING_LICN_NO,
                PASSPORT,
                MOBILE_NO,
                EMAIL_ID,
                APPLICATIONNO,
                COAPPLICANT_PAN_NO,
                GUARANTOR_PAN_NO,
                AGREEMENTNO,
                CUST_UNQ_ID,
                CUSTID,
                LEAD_ID,
                LEAD_STATUS,
                CREATED_DATE,
                SOURCE_BASED_LAST_UPDATED,
                DATE_OF_INCORPORATION,
                DATE_OF_BIRTH,
                ALLOCATION_REGION_CODE,
                ALLOCATION_REGION_TYPE,
                CUID,
                CUID_CREATION_DATE,
                REMARKS
            )

            SELECT

                'CUSTOMER' AS TYPE,
                'LAP_LOS' AS SOURCE,
                upper(trimBoth(p.PAN_NO)) AS PAN_NO,
                ifNull(p.GST_NO, '') AS GST_NO,
                ifNull(p.AADHAAR_NO, '') AS AADHAAR_NO,
                ifNull(p.VOTER_ID, '') AS VOTER_ID,
                ifNull(p.DRIVING_LICN_NO, '') AS DRIVING_LICN_NO,
                ifNull(p.PASSPORT, '') AS PASSPORT,
                ifNull(p.MOBILE_NO, '') AS MOBILE_NO,
                ifNull(p.EMAIL_ID, '') AS EMAIL_ID,
                ifNull(p.APPLICATIONNO, '') AS APPLICATIONNO,
                ifNull(p.COAPPLICANT_PAN_NO, '') AS COAPPLICANT_PAN_NO,
                ifNull(p.GUARANTOR_PAN_NO, '') AS GUARANTOR_PAN_NO,
                ifNull(p.AGREEMENTNO, '') AS AGREEMENTNO,
                '' AS CUST_UNQ_ID,
                ifNull(p.CUSTID, '') AS CUSTID,
                ifNull(p.LEAD_ID, '') AS LEAD_ID,
                ifNull(p.LEAD_STATUS, '') AS LEAD_STATUS,
                ifNull(
                    su.APPLICATION_CREATED_DATE,
                    ''
                ) AS CREATED_DATE,
                ifNull(
                    p.SOURCE_BASED_LAST_UPDATED,
                    ''
                ) AS SOURCE_BASED_LAST_UPDATED,
                ifNull(
                    p.DATE_OF_INCORPORATION,
                    ''
                ) AS DATE_OF_INCORPORATION,
                ifNull(
                    p.DATE_OF_BIRTH,
                    ''
                ) AS DATE_OF_BIRTH,
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
                'Loaded from LAP LOS' AS REMARKS
            FROM
            (
                SELECT *
                FROM CUID.LAP_LOS
                WHERE
                    LastUpdated >= today()
                    AND LastUpdated < today() + 1
                ORDER BY version DESC
                LIMIT 1 BY checksum1
            ) p
            INNER JOIN CUID.CUID_REPOSITORY r
                ON upper(trimBoth(p.PAN_NO))
                = upper(trimBoth(r.PAN_NO))
            ANY LEFT JOIN CUID.LAP_LSD_REPORT su
                ON toString(p.LEAD_ID)
                = toString(su.APPLICATION_NO)
            WHERE
                p.PAN_NO IS NOT NULL
                AND trimBoth(p.PAN_NO) != ''
                AND match(
                    upper(trimBoth(p.PAN_NO)),
                    '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                )
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
                'LAP_LOS',
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

        logger.info(
            "========== ETL SUMMARY =========="
        )

        logger.info(
            f"📦 Source Count          : {source_count:,}"
        )

        logger.info(
            f"📦 Temp Count            : {temp_count:,}"
        )

        logger.info(
            f"🆔 Repository Pending    : {pending_repository:,}"
        )

        logger.info(
            f"🆔 Repository Inserted   : {inserted_repository:,}"
        )

        logger.info(
            f"⚠ Repository Gap        : {gap_count:,}"
        )

        logger.info(
            "================================="
        )

        logger.info("🎯 ETL Completed Successfully")
        logger.info(f"⏱ Duration: {duration_minutes} minutes")

    except Exception as e:

        logger.exception(f"❌ ETL FAILED: {e}")

    finally:

        try:

            if cursor is not None:
                cursor.close()

            if mssql_conn is not None:
                mssql_conn.close()

        except Exception as e:

            logger.exception(
                f"❌ Connection Close Failure: {e}"
            )

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    run_etl()

