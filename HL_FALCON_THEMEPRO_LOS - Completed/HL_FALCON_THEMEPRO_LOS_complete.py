import os
import sys
import time
import uuid
import logging
import traceback
from datetime import datetime, date

import pyodbc
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
# MSSQL CONFIG
# ============================================================
MSSQL_HOST = os.getenv("MSSQL_HL_LOS_HOST")
MSSQL_PORT = os.getenv("MSSQL_HL_LOS_PORT")
MSSQL_DB = os.getenv("MSSQL_HL_LOS_DATABASE")
MSSQL_USER = os.getenv("MSSQL_HL_LOS_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_HL_LOS_PASSWORD", "")

# ============================================================
# ENVIRONMENT
# ============================================================
ENV = os.getenv("ENVIRON", "UAT").upper()

# ============================================================
# CLICKHOUSE CONFIG
# ============================================================
CH_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
CH_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
CH_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
CH_PASSWORD = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE")

TEMP_TABLE = os.getenv("TEMP_TABLE", "HL_LOS_TEMP")
FINAL_TABLE = os.getenv("FINAL_TABLE", "HL_LOS")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100000))

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
# RETRY FUNCTION
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

    job_id = f"HL_LOS_{uuid.uuid4()}"
    job_start_str = job_start.strftime("%Y-%m-%d %H:%M:%S")

    source_count = 0
    temp_count = 0
    inserted_master = 0
    inserted_repository = 0

    mssql_conn = None
    cursor = None

    try:
        logger.info(f"🚀 Job Started : {job_id}")
        logger.info(f"🌐 ENVIRON : {ENV}")

        # ====================================================
        # MSSQL CONNECTION
        # ====================================================
        logger.info("🔌 Connecting MSSQL...")

        mssql_conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
            f"DATABASE={MSSQL_DB};"
            f"UID={MSSQL_USER};"
            f"PWD={MSSQL_PASSWORD}"
        )

        cursor = mssql_conn.cursor()

        # ====================================================
        # MSSQL QUERY
        # ====================================================
        query = """
            SELECT DISTINCT
                ISNULL(OA.name, '') AS NAME,
                ISNULL(UPPER(LTRIM(RTRIM(OA.panNumber))), '') AS PAN_NO,
                ISNULL(OA.gstNumber, '') AS GST_NO,
                ISNULL(OA.aadhar, '') AS AADHAR_NO,
                ISNULL(OA.voterId, '') AS VOTER_ID,
                ISNULL(OA.drivingLicense, '') AS DRIVING_LICN_NO,
                ISNULL(OA.passport, '') AS PASSPORT,
                ISNULL(OA.mobile, '') AS MOBILE_NO,
                ISNULL(OA.email, '') AS EMAIL_ID,
                ISNULL(OA.application_id, '') AS APPLICATIONNO,
                ISNULL(SEC.COAPPLICANT_PAN_NO, '') AS COAPPLICANT_PAN_NO,
                ISNULL(SEC.GUARANTOR_PAN_NO, '') AS GUARANTOR_PAN_NO,
                ISNULL(CM.UCIC_ID, '') AS CIF_NO,
                ISNULL(OA.agreementNumber, '') AS AGREEMENTNO,
                ISNULL(OA.customerId, '') AS CUST_UNQ_ID,
                ISNULL(OA.customerId, '') AS CUSTID,
                ISNULL(OA.application_id, '') AS LEAD_ID,
                ISNULL(OA.application_status, '') AS LEAD_STATUS,
                ISNULL(CONVERT(VARCHAR(19), OA.cre_time, 120), '') AS CREATED_DATE,
                ISNULL(CONVERT(VARCHAR(19), OA.mod_time, 120), '') AS SOURCE_BASED_LAST_UPDATED,
                ISNULL(CONVERT(VARCHAR(19), TRY_CONVERT(DATETIME, OA.cre_time), 120), '') AS DATE_OF_INCORPORATION,
                ISNULL(CONVERT(VARCHAR(19), TRY_CONVERT(DATETIME, OA.dob), 120), '') AS DATE_OF_BIRTH,
                ISNULL(OA.[branchInfo.regionName], '') AS ALLOCATION_REGION_CODE,
                ISNULL(OA.[branchInfo.zoneName], '') AS ALLOCATION_REGION_TYPE
            FROM LOSDATA..oaoapplicants_full_v1 OA WITH (NOLOCK)
            LEFT JOIN
            (
                SELECT
                    primaryApplicantId,
                    STRING_AGG(
                        CASE
                            WHEN applicant <> 'guarantor'
                            AND NULLIF(LTRIM(RTRIM(panNumber)), '') IS NOT NULL
                            THEN UPPER(LTRIM(RTRIM(panNumber)))
                        END, ','
                    ) AS COAPPLICANT_PAN_NO,
                    STRING_AGG(
                        CASE
                            WHEN applicant = 'guarantor'
                            AND NULLIF(LTRIM(RTRIM(panNumber)), '') IS NOT NULL
                            THEN UPPER(LTRIM(RTRIM(panNumber)))
                        END, ','
                    ) AS GUARANTOR_PAN_NO
                FROM LOSDATA..oaoapplicants_full_v1 WITH (NOLOCK)
                GROUP BY primaryApplicantId
            ) SEC
                ON SEC.primaryApplicantId = OA.application_id
            LEFT JOIN DEVHFC02..LON_TINSTENT LI
                ON LI.CUSID = OA.customerId
            LEFT JOIN DEVHFC02..CUS_TCUSMST CM
                ON CM.CUSID = LI.CUSID
            WHERE OA.agreementNumber IS NOT NULL

            UNION ALL

            SELECT DISTINCT
                ISNULL(CU.CBSI_CUSFISNAM, '') AS NAME,
                ISNULL(UPPER(LTRIM(RTRIM(CU.CBSI_PAN))), '') AS PAN_NO,
                ISNULL(CU.CBSI_GSTNUM, '') AS GST_NO,
                ISNULL(CU.CBSI_AADHAR, '') AS AADHAR_NO,
                ISNULL(CU.CBSI_VOTERID, '') AS VOTER_ID,
                ISNULL(CU.CBSI_DRIVLICNS, '') AS DRIVING_LICN_NO,
                ISNULL(CU.CBSI_PASSPORT, '') AS PASSPORT,
                ISNULL(CA.CADI_MOBNOI, '') AS MOBILE_NO,
                ISNULL(CA.CADI_EMAILID, '') AS EMAIL_ID,
                ISNULL(CU.CBSI_APPLCNTNO, '') AS APPLICATIONNO,
                ISNULL(CO.COAPPLICANT_PAN_NO, '') AS COAPPLICANT_PAN_NO,
                ISNULL(GU.GUARANTOR_PAN_NO, '') AS GUARANTOR_PAN_NO,
                ISNULL(CM.UCIC_ID, '') AS CIF_NO,
                ISNULL(T.AcctID, '') AS AGREEMENTNO,
                ISNULL(CU.CBSI_CUSID, '') AS CUST_UNQ_ID,
                ISNULL(CU.CBSI_CUSID, '') AS CUSTID,
                ISNULL(CU.CBSI_APPLCNTNO, '') AS LEAD_ID,
                ISNULL(LON.LBSI_APPSTATUS, '') AS LEAD_STATUS,
                ISNULL(CONVERT(VARCHAR(19), LON.LBSI_DTCREATED, 120), '') AS CREATED_DATE,
                ISNULL(
                    CONVERT(
                        VARCHAR(19),
                        COALESCE(LON.LBSI_DISBBUSDT, LON.LBSI_DTMODIFIED),
                        120
                    ),
                    ''
                ) AS SOURCE_BASED_LAST_UPDATED,
                ISNULL(CONVERT(VARCHAR(19), CAST(CU.CBSI_DTCREATED AS DATETIME), 120), '') AS DATE_OF_INCORPORATION,
                ISNULL(CONVERT(VARCHAR(19), CAST(CU.CBSI_DOB AS DATETIME), 120), '') AS DATE_OF_BIRTH,
                ISNULL(BR.REGION, '') AS ALLOCATION_REGION_CODE,
                ISNULL(BR.ZONE, '') AS ALLOCATION_REGION_TYPE
            FROM TPLSW..LSW_TCUSBASICINFO CU
            LEFT JOIN TPLSW..LSW_TCUSADDRINFO CA
                ON CA.CADI_PRCSID = CU.CBSI_PRCSID
            LEFT JOIN
            (
                SELECT
                    COBI_PRCSID,
                    STRING_AGG(UPPER(LTRIM(RTRIM(COBI_PAN))), ',') AS COAPPLICANT_PAN_NO
                FROM TPLSW..LSW_TCOCUSBASICINFO
                WHERE NULLIF(LTRIM(RTRIM(COBI_PAN)), '') IS NOT NULL
                GROUP BY COBI_PRCSID
            ) CO
                ON CO.COBI_PRCSID = CU.CBSI_PRCSID
            LEFT JOIN
            (
                SELECT
                    GRBI_PRCSID,
                    STRING_AGG(UPPER(LTRIM(RTRIM(GRBI_PAN))), ',') AS GUARANTOR_PAN_NO
                FROM TPLSW..LSW_TGURCUSBASICINFO
                WHERE NULLIF(LTRIM(RTRIM(GRBI_PAN)), '') IS NOT NULL
                GROUP BY GRBI_PRCSID
            ) GU
                ON GU.GRBI_PRCSID = CU.CBSI_PRCSID
            LEFT JOIN TPLSW..LSW_TLONBASICINFO LON
                ON LON.LBSI_PRCSID = CU.CBSI_PRCSID
            LEFT JOIN DEVHFC02..SAM_TBRMST BR
                ON BR.BrId = CU.CBSI_BRANCH
            INNER JOIN DEVHFC02..LON_TINSTENT T
                ON T.AlterAcctID = CU.CBSI_APPLCNTNO
            AND T.AcctID <> CU.CBSI_APPLCNTNO
            LEFT JOIN DEVHFC02..CUS_TCUSMST CM
                ON CM.CUSID = T.CUSID
            WHERE CU.CBSI_APPLCNTNO IS NOT NULL
        """

        logger.info("📥 Executing MSSQL Query...")
        cursor.execute(query)

        columns = [col[0] for col in cursor.description]

        # ====================================================
        # CLICKHOUSE CONNECTION
        # ====================================================
        logger.info("🔌 Connecting ClickHouse...")

        ch = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DB
        )

        # ====================================================
        # STEP 1 : TRUNCATE TEMP TABLE
        # ====================================================
        logger.info("🧹 Truncating temp table...")

        execute_with_retry(
            ch,
            f"TRUNCATE TABLE {CH_DB}.{TEMP_TABLE}"
        )

        # ====================================================
        # STEP 2 : LOAD INTO TEMP
        # ====================================================
        logger.info("📥 Loading data into temp table batch-wise...")

        total_rows = 0

        while True:
            rows = cursor.fetchmany(BATCH_SIZE)

            if not rows:
                break

            batch = []

            for row in rows:
                cleaned = []

                for col in row:
                    if col is None:
                        cleaned.append("")
                    elif isinstance(col, datetime):
                        cleaned.append(col.strftime("%Y-%m-%d %H:%M:%S"))
                    elif isinstance(col, date):
                        cleaned.append(col.strftime("%Y-%m-%d"))
                    else:
                        cleaned.append(str(col).strip())

                batch.append(cleaned)

            insert_with_retry(
                ch,
                TEMP_TABLE,
                batch,
                columns
            )

            total_rows += len(batch)

            logger.info(
                f"✅ Inserted Batch : {len(batch)} | "
                f"Total Inserted : {total_rows}"
            )

        source_count = total_rows
        temp_count = ch.query(
            f"SELECT count() FROM {CH_DB}.{TEMP_TABLE}"
        ).result_rows[0][0]

        logger.info(f"✅ Temp Load Completed | Total Rows : {temp_count}")

        # ====================================================
        # STEP 3 : INITIAL / INCREMENTAL LOAD
        # ====================================================
        master_count = ch.query(
            f"SELECT count() FROM {CH_DB}.{FINAL_TABLE}"
        ).result_rows[0][0]

        if master_count == 0:
            logger.info("🚀 Initial Load Started")

            execute_with_retry(ch, f"""
                INSERT INTO {CH_DB}.{FINAL_TABLE}
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
                    GUARANTOR_PAN_NO,
                    CIF_NO,
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
                    checksum1,
                    checksum2
                )
                SELECT
                    NAME,
                    upper(trimBoth(PAN_NO)) AS PAN_NO,
                    GST_NO,
                    AADHAR_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
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
                WHERE
                    PAN_NO IS NOT NULL
                    AND trimBoth(PAN_NO) != ''
                    AND match(
                        upper(trimBoth(PAN_NO)),
                        '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                    )
            """)

        else:
            logger.info("🔄 Incremental Load Started")

            execute_with_retry(ch, f"""
                INSERT INTO {CH_DB}.{FINAL_TABLE}
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
                    GUARANTOR_PAN_NO,
                    CIF_NO,
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
                    checksum1,
                    checksum2
                )
                SELECT
                    t.NAME,
                    upper(trimBoth(t.PAN_NO)) AS PAN_NO,
                    t.GST_NO,
                    t.AADHAR_NO,
                    t.VOTER_ID,
                    t.DRIVING_LICN_NO,
                    t.PASSPORT,
                    t.MOBILE_NO,
                    t.EMAIL_ID,
                    t.APPLICATIONNO,
                    t.COAPPLICANT_PAN_NO,
                    t.GUARANTOR_PAN_NO,
                    t.CIF_NO,
                    t.AGREEMENTNO,
                    t.CUST_UNQ_ID,
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

        inserted_master = ch.query(f"""
            SELECT count()
            FROM
            (
                SELECT *
                FROM {CH_DB}.{FINAL_TABLE}
                WHERE LastUpdated >= toDateTime('{job_start_str}')
                ORDER BY version DESC
                LIMIT 1 BY checksum1
            )
        """).result_rows[0][0]

        # ====================================================
        # STEP 4 : CUID REPOSITORY - VALKEY BASED
        # ====================================================
        logger.info("🆔 Preparing PAN staging table...")

        execute_with_retry(
            ch,
            "TRUNCATE TABLE CUID.CUID_NEW_PAN_STAGE"
        )

        execute_with_retry(ch, f"""
            INSERT INTO CUID.CUID_NEW_PAN_STAGE
            SELECT DISTINCT
                upper(
                    trimBoth(
                        replaceAll(
                            replaceAll(
                                replaceAll(PAN_NO, '\\r', ''),
                            '\\n', ''),
                        '\\t', '')
                    )
                ) AS PAN_NO
            FROM {CH_DB}.{FINAL_TABLE}
            FINAL
            WHERE
                PAN_NO IS NOT NULL
                AND trimBoth(PAN_NO) != ''
                AND match(
                    upper(
                        trimBoth(
                            replaceAll(
                                replaceAll(
                                    replaceAll(PAN_NO, '\\r', ''),
                                '\\n', ''),
                            '\\t', '')
                        )
                    ),
                    '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                )
                AND upper(
                    trimBoth(
                        replaceAll(
                            replaceAll(
                                replaceAll(PAN_NO, '\\r', ''),
                            '\\n', ''),
                        '\\t', '')
                    )
                ) NOT IN
                (
                    SELECT PAN_NO
                    FROM CUID.CUID_REPOSITORY
                )
        """)

        pending_repository = ch.query("""
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

            execute_with_retry(ch, repository_sql)

            inserted_repository = ch.query("""
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

        logger.info("🎯 HL LOS Valkey CUID Generation Completed")

        # ====================================================
        # STEP 5 : INSERT INTO MASTER TABLE
        # ====================================================
        logger.info("📦 Inserting into CUID_MASTER_TABLE...")

        execute_with_retry(ch, f"""
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
                'HL_LOS' AS SOURCE,

                ifNull(p.NAME, '') AS NAME,
                upper(trimBoth(p.PAN_NO)) AS PAN_NO,

                ifNull(p.GST_NO, '') AS GST_NO,
                ifNull(p.AADHAR_NO, '') AS AADHAAR_NO,

                '' AS CKYCID,
                '' AS TIN_NO,

                ifNull(p.VOTER_ID, '') AS VOTER_ID,
                ifNull(p.DRIVING_LICN_NO, '') AS DRIVING_LICN_NO,

                '' AS RATIONCARD_NO,

                ifNull(p.PASSPORT, '') AS PASSPORT,
                ifNull(p.MOBILE_NO, '') AS MOBILE_NO,
                ifNull(p.EMAIL_ID, '') AS EMAIL_ID,

                ifNull(p.APPLICATIONNO, '') AS APPLICATIONNO,
                ifNull(p.COAPPLICANT_PAN_NO, '') AS COAPPLICANT_PAN_NO,
                ifNull(p.GUARANTOR_PAN_NO, '') AS GUARANTOR_PAN_NO,

                ifNull(p.CIF_NO, '') AS CIF_NO,
                ifNull(p.AGREEMENTNO, '') AS AGREEMENTNO,
                ifNull(p.CUST_UNQ_ID, '') AS CUST_UNQ_ID,
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
                ifNull(p.DATE_OF_INCORPORATION, '') AS DATE_OF_INCORPORATION,
                '' AS USER_EMPLOYEE_NO,
                ifNull(p.DATE_OF_BIRTH, '') AS DATE_OF_BIRTH,
                '' AS DATE_OF_JOINING,

                ifNull(p.ALLOCATION_REGION_CODE, '') AS ALLOCATION_REGION_CODE,
                ifNull(p.ALLOCATION_REGION_TYPE, '') AS ALLOCATION_REGION_TYPE,

                r.CUID,
                r.CUID_CREATION_DATE,

                'Loaded from HL LOS' AS REMARKS

            FROM
            (
                SELECT *
                FROM {CH_DB}.{FINAL_TABLE}
                WHERE LastUpdated >= today()
                  AND LastUpdated < today() + 1
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
        """)

        # ====================================================
        # STEP 6 : OPTIMIZE
        # ====================================================
        # logger.info("⚡ Optimizing tables...")

        # execute_with_retry(
        #     ch,
        #     f"OPTIMIZE TABLE {CH_DB}.{FINAL_TABLE} FINAL"
        # )

        # execute_with_retry(
        #     ch,
        #     "OPTIMIZE TABLE CUID.CUID_MASTER_TABLE FINAL"
        # )

        # ====================================================
        # STEP 7 : AUDIT LOG
        # ====================================================
        duration_minutes = round(
            (time.time() - start_time) / 60,
            2
        )

        execute_with_retry(ch, f"""
            INSERT INTO CUID.ETL_AUDIT_LOG
            VALUES
            (
                '{job_id}',
                'HL_LOS',
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

        logger.info("🎯 HL LOS ETL Completed Successfully")
        logger.info(f"⏱ Duration: {duration_minutes} minutes")

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_number = exc_traceback.tb_lineno if exc_traceback else "Unknown"

        logger.error(f"❌ ETL FAILED at line {line_number}: {e}")
        logger.error(traceback.format_exc())

    finally:
        try:
            if cursor:
                cursor.close()

            if mssql_conn:
                mssql_conn.close()

        except Exception as e:
            logger.error(f"❌ Connection Close Failure : {e}")

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    run_etl()
