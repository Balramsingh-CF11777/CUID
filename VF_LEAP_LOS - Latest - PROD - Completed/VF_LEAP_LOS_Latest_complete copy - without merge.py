import os
import clickhouse_connect
import logging
import time
import uuid
from dotenv import load_dotenv
from datetime import datetime
from retrying import retry
import traceback
import sys

# ============================================================
# ENV LOAD
# ============================================================
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# SOURCE CLICKHOUSE (GOLD)
# =========================
SRC_HOST = os.getenv("SRC_CLICKHOUSE_HOST")
SRC_PORT = int(os.getenv("SRC_CLICKHOUSE_PORT"))
SRC_USER = os.getenv("SRC_CLICKHOUSE_USER")
SRC_PASSWORD = os.getenv("SRC_CLICKHOUSE_PASSWORD")
SRC_DB = os.getenv("SRC_CLICKHOUSE_DATABASE")

# =========================
# DEST CLICKHOUSE (CUID)
# =========================
DEST_HOST = os.getenv("DEST_CLICKHOUSE_HOST")
DEST_PORT = int(os.getenv("DEST_CLICKHOUSE_PORT"))
DEST_USER = os.getenv("DEST_CLICKHOUSE_USER")
DEST_PASSWORD = os.getenv("DEST_CLICKHOUSE_PASSWORD")
DEST_DB = os.getenv("DEST_CLICKHOUSE_DATABASE")

TEMP_TABLE="VF_LEAP_LOS_TEMP"
FINAL_TABLE="VF_LEAP_LOS"

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000000))

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
    job_id = f"EDLS_VF_LOS_{uuid.uuid4()}"

    source_count = 0
    temp_count = 0
    inserted_master = 0
    inserted_repository = 0

    try:
        logger.info("🔌 Connecting to VF Leap ClickHouse...")
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
        execute_with_retry(dest, f"TRUNCATE TABLE {DEST_DB}.{TEMP_TABLE}")

        # =====================================================
        # STEP 2: FETCH VALID PAN DATA FROM GOLD
        # =====================================================
        logger.info("📊 Counting rows...")

        count_query = """
        SELECT count()
                FROM
                (
                SELECT
                    CASE
                        WHEN pd.isIndividual = 1 THEN 
                            concat(
                                ifNull(pd.firstName, ''), ' ',
                                ifNull(pd.middleName, ''), ' ',
                                ifNull(pd.lastName, '')
                            )
                        ELSE 
                            ifNull(pd.companyName, '')
                    END AS NAME,
                    MAX(CASE WHEN doc.subcategory = 'PAN CARD' 
                        THEN trimBoth(doc.kycProofValue) END) AS PAN_NO,
                    pd.partyType as PARTY_TYPE,
                    MAX(CASE WHEN doc.subcategory = 'GST' 
                        THEN trimBoth(doc.kycProofValue) END) AS GST_NO,
                    MAX(CASE WHEN doc.subcategory = 'AADHAR CARD(U-ID)' 
                        THEN trimBoth(doc.kycProofValue) END) AS AADHAAR_NO,
                    MAX(CASE WHEN doc.subcategory = 'VOTER ID' 
                        THEN trimBoth(doc.kycProofValue) END) AS VOTER_ID,
                    MAX(CASE WHEN doc.subcategory = 'DL' 
                        THEN trimBoth(doc.kycProofValue) END) AS DRIVING_LICN_NO,
                    MAX(CASE WHEN doc.subcategory = 'PASSPORT' 
                        THEN trimBoth(doc.kycProofValue) END) AS PASSPORT,
                    replaceAll(
                        replaceAll(toString(pd.mobileNos), '[', ''),
                        ']',
                        ''
                    ) AS MOBILE_NO,
                    encmm.CIF_NO AS CIF_NO,
                    ld.vehicleDetail_agreementNo AS AGREEMENTNO,
                    pd.customerID AS CUSTID,
                    doc.leadID AS LEAD_ID,
                    ls.currentWorkStatus AS LEAD_STATUS,
                    ld.createdDate AS CREATED_DATE,
                    ld.modifiedDate AS SOURCE_BASED_LAST_UPDATED,
                    pd.dateOfIncorporation AS DATE_OF_INCORPORATION,
                    pd.dateOfBirth AS DATE_OF_BIRTH
                FROM
                    VF_LOS.SalesDB_Lead_Inflight_documents doc
                LEFT JOIN VF_LOS.SalesDB_Lead_Inflight_partyDetails pd
                                    ON
                    doc.partyID = pd.partyID
                LEFT JOIN VF_LOS.SalesDB_Lead_Inflight ls
                                    ON
                    ls.leadID = pd.leadID
                INNER JOIN VF_LOS.SalesDB_Lead_Inflight_LoanDetails ld
                                    ON
                    doc.leadID = ld.leadID
                INNER JOIN Operations_EDLS.EDLS_NBFC_CUSTOMER_M_MAIN encmm 
                on
                    toString(pd.customerID)= toString(encmm.CUSTOMERID)
                GROUP BY
                    doc.leadID,
                    pd.partyType,
                    pd.mobileNos,
                    pd.dateOfBirth,
                    pd.dateOfIncorporation,
                    ld.vehicleDetail_agreementNo,
                    ld.modifiedDate,
                    ld.createdDate,
                    pd.isIndividual,
                    pd.firstName,
                    pd.middleName,
                    pd.lastName,
                    pd.companyName,
                    ls.currentWorkStatus,
                    pd.customerID,
                    encmm.CIF_NO
                )
        """

        source_count = src.query(count_query).result_rows[0][0]
        logger.info(f"📊 Valid PAN rows: {source_count}")

        offset = 0
        total_inserted = 0

        while True:

            logger.info(
                f"📥 Fetching batch - OFFSET: {offset:,}, LIMIT: {BATCH_SIZE:,}"
            )

            select_query = f"""
            SELECT
                    CASE
                        WHEN pd.isIndividual = 1 THEN 
                            concat(
                                ifNull(pd.firstName, ''), ' ',
                                ifNull(pd.middleName, ''), ' ',
                                ifNull(pd.lastName, '')
                            )
                        ELSE 
                            ifNull(pd.companyName, '')
                    END AS NAME,
                    MAX(CASE WHEN doc.subcategory = 'PAN CARD' 
                        THEN trimBoth(doc.kycProofValue) END) AS PAN_NO,
                    pd.partyType as PARTY_TYPE,
                    MAX(CASE WHEN doc.subcategory = 'GST' 
                        THEN trimBoth(doc.kycProofValue) END) AS GST_NO,
                    MAX(CASE WHEN doc.subcategory = 'AADHAR CARD(U-ID)' 
                        THEN trimBoth(doc.kycProofValue) END) AS AADHAAR_NO,
                    MAX(CASE WHEN doc.subcategory = 'VOTER ID' 
                        THEN trimBoth(doc.kycProofValue) END) AS VOTER_ID,
                    MAX(CASE WHEN doc.subcategory = 'DL' 
                        THEN trimBoth(doc.kycProofValue) END) AS DRIVING_LICN_NO,
                    MAX(CASE WHEN doc.subcategory = 'PASSPORT' 
                        THEN trimBoth(doc.kycProofValue) END) AS PASSPORT,
                    
                    pd.mobileNos AS MOBILE_NO,
                    encmm.CIF_NO AS CIF_NO,
                    ld.vehicleDetail_agreementNo AS AGREEMENTNO,
                    pd.customerID AS CUSTID,
                    doc.leadID AS LEAD_ID,
                    ls.currentWorkStatus AS LEAD_STATUS,
                    ld.createdDate AS CREATED_DATE,
                    ld.modifiedDate AS SOURCE_BASED_LAST_UPDATED,
                    pd.dateOfIncorporation AS DATE_OF_INCORPORATION,
                    pd.dateOfBirth AS DATE_OF_BIRTH
                FROM
                    VF_LOS.SalesDB_Lead_Inflight_documents doc
                LEFT JOIN VF_LOS.SalesDB_Lead_Inflight_partyDetails pd
                                    ON
                    doc.partyID = pd.partyID
                LEFT JOIN VF_LOS.SalesDB_Lead_Inflight ls
                                    ON
                    ls.leadID = pd.leadID
                INNER JOIN VF_LOS.SalesDB_Lead_Inflight_LoanDetails ld
                                    ON
                    doc.leadID = ld.leadID
                INNER JOIN Operations_EDLS.EDLS_NBFC_CUSTOMER_M_MAIN encmm 
                on
                    toString(pd.customerID)= toString(encmm.CUSTOMERID)
                GROUP BY
                    doc.leadID,
                    pd.partyType,
                    pd.mobileNos,
                    pd.dateOfBirth,
                    pd.dateOfIncorporation,
                    ld.vehicleDetail_agreementNo,
                    ld.modifiedDate,
                    ld.createdDate,
                    pd.isIndividual,
                    pd.firstName,
                    pd.middleName,
                    pd.lastName,
                    pd.companyName,
                    ls.currentWorkStatus,
                    pd.customerID,
                    encmm.CIF_NO

            ORDER BY doc.leadID

            LIMIT {BATCH_SIZE}
            OFFSET {offset}
            """

            query_result = src.query(select_query)

            batch_data = query_result.result_rows

            if not batch_data:
                logger.info("✅ No more rows found.")
                break

            logger.info(f"📊 Batch rows fetched: {len(batch_data):,}")

            batch_data = [
                tuple("" if v is None else str(v) for v in row)
                for row in batch_data
            ]

            dest.insert(
                f"{DEST_DB}.{TEMP_TABLE}",
                batch_data,
                column_names=[
                    "NAME",
                    "PAN_NO",
                    "PARTY_TYPE",
                    "GST_NO",
                    "AADHAAR_NO",
                    "VOTER_ID",
                    "DRIVING_LICN_NO",
                    "PASSPORT",
                    "MOBILE_NO",
                    "CIF_NO",
                    "AGREEMENTNO",
                    "CUSTID",
                    "LEAD_ID",
                    "LEAD_STATUS",
                    "CREATED_DATE",
                    "SOURCE_BASED_LAST_UPDATED",
                    "DATE_OF_INCORPORATION",
                    "DATE_OF_BIRTH"
                ]
            )

            total_inserted += len(batch_data)

            logger.info(
                f"➡ Batch Inserted: {len(batch_data):,} | Total Inserted: {total_inserted:,}"
            )

            offset += BATCH_SIZE

        logger.info(
            f"🎯 Temp Load Completed. Total Rows Inserted: {total_inserted:,}"
        )

        temp_count = dest.query(
            f"SELECT count() FROM {DEST_DB}.{TEMP_TABLE}"
        ).result_rows[0][0]

        # =====================================================
        # STEP 3: INITIAL OR INCREMENTAL INSERT
        # =====================================================

        master_count = dest.query(
            f"SELECT count() FROM {DEST_DB}.{FINAL_TABLE}"
        ).result_rows[0][0]

        if master_count == 0:

            logger.info("🚀 Initial Load → Direct Insert")

            execute_with_retry(dest, f"""
            INSERT INTO {DEST_DB}.{FINAL_TABLE}
            (
                NAME,
                PAN_NO,
                PARTY_TYPE,
                GST_NO,
                AADHAAR_NO,
                VOTER_ID,
                DRIVING_LICN_NO,
                PASSPORT,
                MOBILE_NO,
                CIF_NO,
                AGREEMENTNO,
                CUSTID,
                LEAD_ID,
                LEAD_STATUS,
                CREATED_DATE,
                SOURCE_BASED_LAST_UPDATED,
                DATE_OF_INCORPORATION,
                DATE_OF_BIRTH,
                checksum1,
                checksum2
            )
            SELECT
                NAME,
                PAN_NO,
                PARTY_TYPE,
                GST_NO,
                AADHAAR_NO,
                VOTER_ID,
                DRIVING_LICN_NO,
                PASSPORT,
                MOBILE_NO,
                CIF_NO,
                AGREEMENTNO,
                CUSTID,
                LEAD_ID,
                LEAD_STATUS,
                CREATED_DATE,
                SOURCE_BASED_LAST_UPDATED,
                DATE_OF_INCORPORATION,
                DATE_OF_BIRTH,
                checksum1,
                checksum2
            FROM {DEST_DB}.{TEMP_TABLE}
            """)

            logger.info(f"📦 Inserted Records: {temp_count}")

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

            logger.info(f"🆕 New Records     : {new_count}")
            logger.info(f"🔄 Changed Records : {changed_count}")

            execute_with_retry(dest, f"""
                INSERT INTO {DEST_DB}.{FINAL_TABLE}
                (
                    NAME,
                    PAN_NO,
                    PARTY_TYPE,
                    GST_NO,
                    AADHAAR_NO,
                    VOTER_ID,
                    DRIVING_LICN_NO,
                    PASSPORT,
                    MOBILE_NO,
                    CIF_NO,
                    AGREEMENTNO,
                    CUSTID,
                    LEAD_ID,
                    LEAD_STATUS,
                    CREATED_DATE,
                    SOURCE_BASED_LAST_UPDATED,
                    DATE_OF_INCORPORATION,
                    DATE_OF_BIRTH,
                    checksum1,
                    checksum2
                )
                SELECT
                    t.NAME,
                    t.PAN_NO,
                    t.PARTY_TYPE,
                    t.GST_NO,
                    t.AADHAAR_NO,
                    t.VOTER_ID,
                    t.DRIVING_LICN_NO,
                    t.PASSPORT,
                    t.MOBILE_NO,
                    t.CIF_NO,
                    t.AGREEMENTNO,
                    t.CUSTID,
                    t.LEAD_ID,
                    t.LEAD_STATUS,
                    t.CREATED_DATE,
                    t.SOURCE_BASED_LAST_UPDATED,
                    t.DATE_OF_INCORPORATION,
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
                    m.checksum1 IS NULL
                    OR t.checksum2 != m.checksum2
                """)

            logger.info(
                f"📦 Inserted Records: {new_count + changed_count}"
            )

        # =====================================================
        # STEP 4: CUID REPOSITORY
        # =====================================================
        logger.info("🆔 Generating CUIDs...")

        execute_with_retry(dest, """
        INSERT INTO CUID.CUID_REPOSITORY
            SELECT
                PAN_NO,
                lpad(
                    toString(
                        row_number() OVER (ORDER BY PAN_NO) + ifNull(max_cuid, 0)
                    ),
                    16,
                    '0'
                ) AS CUID,
                formatDateTime(now(), '%Y-%m-%d %H:%i:%S') AS CUID_CREATION_DATE
            FROM
            (
                SELECT DISTINCT
                    upper(trimBoth(PAN_NO)) AS PAN_NO
                FROM CUID.VF_LEAP_LOS
                WHERE
                    PAN_NO IS NOT NULL
                    AND trimBoth(PAN_NO) != ''
                    AND match(upper(trimBoth(PAN_NO)), '^[A-Z]{5}[0-9]{4}[A-Z]$')
                    AND upper(trimBoth(PAN_NO)) NOT IN
                    (
                        SELECT PAN_NO FROM CUID.CUID_REPOSITORY
                    )
            ) new_pans
            CROSS JOIN
            (
                SELECT
                    max(toUInt64(CUID)) AS max_cuid
                FROM CUID.CUID_REPOSITORY
            ) offset_table;
        """)

        logger.info("📦 Optimize the product master table...")
        execute_with_retry(dest, f"""OPTIMIZE TABLE {DEST_DB}.{FINAL_TABLE} FINAL;""")

        # =====================================================
        # STEP 5: MASTER INSERT
        # =====================================================
        logger.info("📦 Inserting into CUID_MASTER_TABLE...")

        execute_with_retry(dest, """
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
                coalesce(d.TYPE_OF_BROKER, 'CUSTOMER') AS TYPE,   -- ✅ dynamic TYPE

                'VF_LOS' AS SOURCE,

                upper(trimBoth(v.PAN_NO)) AS PAN_NO,

                v.NAME,
                v.GST_NO,
                v.AADHAAR_NO,

                '' AS CKYCID,
                '' AS TIN_NO,

                v.VOTER_ID,
                v.DRIVING_LICN_NO,

                '' AS RATIONCARD_NO,

                v.PASSPORT,
                v.MOBILE_NO,

                '' AS EMAIL_ID,

                '' AS APPLICATIONNO,

                '' AS COAPPLICANT_PAN_NO,
                '' AS GUARANTOR_PAN_NO,
                v.CIF_NO AS CIF_NO,

                v.AGREEMENTNO,

                '' AS CUST_UNQ_ID,
                v.CUSTID AS CUSTID,
                '' AS VENDOR_ID,

                '' AS SOURCE_BASED_STATUS,

                '' AS REGISTRATION_CERTIFICATE,
                '' AS NPA_STAGEID,

                v.LEAD_ID,
                v.LEAD_STATUS,
                v.CREATED_DATE,

                v.SOURCE_BASED_LAST_UPDATED,

                '' AS ASSET_VALUE,

                v.DATE_OF_INCORPORATION,

                '' AS USER_EMPLOYEE_NO,

                v.DATE_OF_BIRTH,

                '' AS DATE_OF_JOINING,
                '' AS ALLOCATION_REGION_CODE,
                '' AS ALLOCATION_REGION_TYPE,

                r.CUID,
                r.CUID_CREATION_DATE,

                'Loaded from VF_LEAP_LOS_Latest' AS REMARKS

            FROM CUID.VF_LEAP_LOS v

            INNER JOIN CUID.CUID_REPOSITORY r
                ON upper(trimBoth(v.PAN_NO)) = r.PAN_NO

            LEFT JOIN CUID.VF_DEALER d   -- ✅ Join dealer table
                ON upper(trimBoth(v.PAN_NO)) = upper(trimBoth(d.PANNO))

            WHERE
                v.PAN_NO IS NOT NULL
                AND trimBoth(v.PAN_NO) != ''
                AND match(upper(trimBoth(v.PAN_NO)), '^[A-Z]{5}[0-9]{4}[A-Z]$');
        """)

        logger.info("🎯 ETL COMPLETED SUCCESSFULLY")

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_number = exc_traceback.tb_lineno
        logger.error(f"❌ ETL FAILED at line {line_number}: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    run_etl()