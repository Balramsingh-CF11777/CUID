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
import valkey
from valkey.connection import BlockingConnectionPool

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
ENV = os.getenv("ENVIRON", "UAT").upper()

DEST_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
DEST_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
DEST_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
DEST_PASSWORD = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD")
DEST_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE")

TEMP_TABLE="VF_LEAP_LOS_TEMP"
FINAL_TABLE="VF_LEAP_LOS"

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000000))

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
                WITH party_pan_map AS
                    (
                        SELECT
                            doc.leadID,

                            arrayStringConcat(
                                groupUniqArrayIf(
                                    upper(trimBoth(doc.kycProofValue)),
                                    lower(pd.partyType) = 'coapplicant'
                                    AND doc.subcategory = 'PAN CARD'
                                    AND doc.kycProofValue IS NOT NULL
                                ),
                                ','
                            ) AS COAPPLICANT_PAN_NO,

                            arrayStringConcat(
                                groupUniqArrayIf(
                                    upper(trimBoth(doc.kycProofValue)),
                                    lower(pd.partyType) = 'guarantor'
                                    AND doc.subcategory = 'PAN CARD'
                                    AND doc.kycProofValue IS NOT NULL
                                ),
                                ','
                            ) AS GUARANTOR_PAN_NO

                        FROM VF_LOS.SalesDB_Lead_Inflight_documents doc
                        INNER JOIN VF_LOS.SalesDB_Lead_Inflight_partyDetails pd
                            ON doc.partyID = pd.partyID

                        GROUP BY doc.leadID
                    )

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

                        MAX(
                            CASE
                                WHEN doc.subcategory = 'PAN CARD'
                                THEN trimBoth(doc.kycProofValue)
                            END
                        ) AS PAN_NO,

                        pd.partyType AS PARTY_TYPE,

                        MAX(
                            CASE
                                WHEN doc.subcategory = 'GST'
                                THEN trimBoth(doc.kycProofValue)
                            END
                        ) AS GST_NO,

                        MAX(
                            CASE
                                WHEN doc.subcategory = 'AADHAR CARD(U-ID)'
                                THEN trimBoth(doc.kycProofValue)
                            END
                        ) AS AADHAAR_NO,

                        MAX(
                            CASE
                                WHEN doc.subcategory = 'VOTER ID'
                                THEN trimBoth(doc.kycProofValue)
                            END
                        ) AS VOTER_ID,

                        MAX(
                            CASE
                                WHEN doc.subcategory = 'DL'
                                THEN trimBoth(doc.kycProofValue)
                            END
                        ) AS DRIVING_LICN_NO,

                        MAX(
                            CASE
                                WHEN doc.subcategory = 'PASSPORT'
                                THEN trimBoth(doc.kycProofValue)
                            END
                        ) AS PASSPORT,

                        replaceRegexpAll(
                            trim(BOTH '[]' FROM ifNull(toString(pd.mobileNos), '')),
                            '^null$',
                            ''
                        ) AS MOBILE_NO,
                        
                        ppm.COAPPLICANT_PAN_NO,

                        ppm.GUARANTOR_PAN_NO,

                        ld.vehicleDetail_agreementNo AS AGREEMENTNO,

                        doc.leadID AS LEAD_ID,

                        ls.currentWorkStatus AS LEAD_STATUS,

                        ld.createdDate AS CREATED_DATE,

                        ld.modifiedDate AS SOURCE_BASED_LAST_UPDATED,

                        pd.dateOfIncorporation AS DATE_OF_INCORPORATION,

                        pd.customerID AS CUSTID,

                        encmm.CIF_NO AS CIF_NO,

                        pd.dateOfBirth AS DATE_OF_BIRTH

                    FROM VF_LOS.SalesDB_Lead_Inflight_documents doc

                    LEFT JOIN VF_LOS.SalesDB_Lead_Inflight_partyDetails pd
                        ON doc.partyID = pd.partyID

                    LEFT JOIN VF_LOS.SalesDB_Lead_Inflight ls
                        ON ls.leadID = pd.leadID

                    INNER JOIN VF_LOS.SalesDB_Lead_Inflight_LoanDetails ld
                        ON doc.leadID = ld.leadID

                    INNER JOIN Operations_EDLS.EDLS_NBFC_CUSTOMER_M_MAIN encmm
                        ON toString(pd.customerID) = toString(encmm.CUSTOMERID)

                    LEFT JOIN party_pan_map ppm
                        ON ppm.leadID = doc.leadID

                    WHERE lower(pd.partyType) = 'applicant'

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
                        encmm.CIF_NO,
                        ppm.COAPPLICANT_PAN_NO,
                        ppm.GUARANTOR_PAN_NO
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
            WITH party_pan_map AS
                (
                    SELECT
                        doc.leadID,

                        arrayStringConcat(
                            groupUniqArrayIf(
                                upper(trimBoth(doc.kycProofValue)),
                                lower(pd.partyType) = 'coapplicant'
                                AND doc.subcategory = 'PAN CARD'
                                AND doc.kycProofValue IS NOT NULL
                            ),
                            ','
                        ) AS COAPPLICANT_PAN_NO,

                        arrayStringConcat(
                            groupUniqArrayIf(
                                upper(trimBoth(doc.kycProofValue)),
                                lower(pd.partyType) = 'guarantor'
                                AND doc.subcategory = 'PAN CARD'
                                AND doc.kycProofValue IS NOT NULL
                            ),
                            ','
                        ) AS GUARANTOR_PAN_NO

                    FROM VF_LOS.SalesDB_Lead_Inflight_documents doc
                    INNER JOIN VF_LOS.SalesDB_Lead_Inflight_partyDetails pd
                        ON doc.partyID = pd.partyID

                    GROUP BY doc.leadID
                )

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

                    MAX(
                        CASE
                            WHEN doc.subcategory = 'PAN CARD'
                            THEN trimBoth(doc.kycProofValue)
                        END
                    ) AS PAN_NO,

                    pd.partyType AS PARTY_TYPE,

                    MAX(
                        CASE
                            WHEN doc.subcategory = 'GST'
                            THEN trimBoth(doc.kycProofValue)
                        END
                    ) AS GST_NO,

                    MAX(
                        CASE
                            WHEN doc.subcategory = 'AADHAR CARD(U-ID)'
                            THEN trimBoth(doc.kycProofValue)
                        END
                    ) AS AADHAAR_NO,

                    MAX(
                        CASE
                            WHEN doc.subcategory = 'VOTER ID'
                            THEN trimBoth(doc.kycProofValue)
                        END
                    ) AS VOTER_ID,

                    MAX(
                        CASE
                            WHEN doc.subcategory = 'DL'
                            THEN trimBoth(doc.kycProofValue)
                        END
                    ) AS DRIVING_LICN_NO,

                    MAX(
                        CASE
                            WHEN doc.subcategory = 'PASSPORT'
                            THEN trimBoth(doc.kycProofValue)
                        END
                    ) AS PASSPORT,

                    replaceRegexpAll(
                        trim(BOTH '[]' FROM ifNull(toString(pd.mobileNos), '')),
                        '^null$',
                        ''
                    ) AS MOBILE_NO,
                    
                    ppm.COAPPLICANT_PAN_NO,

                    ppm.GUARANTOR_PAN_NO,

                    ld.vehicleDetail_agreementNo AS AGREEMENTNO,

                    doc.leadID AS LEAD_ID,

                    ls.currentWorkStatus AS LEAD_STATUS,

                    ld.createdDate AS CREATED_DATE,

                    ld.modifiedDate AS SOURCE_BASED_LAST_UPDATED,

                    pd.dateOfIncorporation AS DATE_OF_INCORPORATION,

                    pd.customerID AS CUSTID,

                    encmm.CIF_NO AS CIF_NO,

                    pd.dateOfBirth AS DATE_OF_BIRTH

                FROM VF_LOS.SalesDB_Lead_Inflight_documents doc

                LEFT JOIN VF_LOS.SalesDB_Lead_Inflight_partyDetails pd
                    ON doc.partyID = pd.partyID

                LEFT JOIN VF_LOS.SalesDB_Lead_Inflight ls
                    ON ls.leadID = pd.leadID

                INNER JOIN VF_LOS.SalesDB_Lead_Inflight_LoanDetails ld
                    ON doc.leadID = ld.leadID

                INNER JOIN Operations_EDLS.EDLS_NBFC_CUSTOMER_M_MAIN encmm
                    ON toString(pd.customerID) = toString(encmm.CUSTOMERID)

                LEFT JOIN party_pan_map ppm
                    ON ppm.leadID = doc.leadID

                WHERE lower(pd.partyType) = 'applicant'

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
                    encmm.CIF_NO,
                    ppm.COAPPLICANT_PAN_NO,
                    ppm.GUARANTOR_PAN_NO

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
                    "COAPPLICANT_PAN_NO",
                    "GUARANTOR_PAN_NO",
                    "AGREEMENTNO",
                    "LEAD_ID",
                    "LEAD_STATUS",
                    "CREATED_DATE",
                    "SOURCE_BASED_LAST_UPDATED",
                    "DATE_OF_INCORPORATION",
                    "CUSTID",
                    "CIF_NO",
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
                COAPPLICANT_PAN_NO,
                GUARANTOR_PAN_NO,
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
                COAPPLICANT_PAN_NO,
                GUARANTOR_PAN_NO,
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
                    COAPPLICANT_PAN_NO,
                    GUARANTOR_PAN_NO,
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
                    t.COAPPLICANT_PAN_NO,
                    t.GUARANTOR_PAN_NO,
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
        # STEP 4: CUID REPOSITORY (VALKEY)
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
            upper(trimBoth(PAN_NO))
        FROM CUID.VF_LEAP_LOS
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
                'CUSTOMER' AS TYPE,

                'VF_LOS' AS SOURCE,

                upper(trimBoth(v.PAN_NO)) AS PAN_NO,

                ifNull(v.NAME, '') AS NAME,
                ifNull(v.GST_NO, '') AS GST_NO,
                ifNull(v.AADHAAR_NO, '') AS AADHAAR_NO,

                '' AS CKYCID,
                '' AS TIN_NO,

                ifNull(v.VOTER_ID, '') AS VOTER_ID,
                ifNull(v.DRIVING_LICN_NO, '') AS DRIVING_LICN_NO,

                '' AS RATIONCARD_NO,

                ifNull(v.PASSPORT, '') AS PASSPORT,
                ifNull(v.MOBILE_NO, '') AS MOBILE_NO,

                '' AS EMAIL_ID,
                '' AS APPLICATIONNO,

                ifNull(v.COAPPLICANT_PAN_NO, '') AS COAPPLICANT_PAN_NO,
                ifNull(v.GUARANTOR_PAN_NO, '') AS GUARANTOR_PAN_NO,

                ifNull(v.CIF_NO, '') AS CIF_NO,

                ifNull(v.AGREEMENTNO, '') AS AGREEMENTNO,

                '' AS CUST_UNQ_ID,

                ifNull(v.CUSTID, '') AS CUSTID,

                '' AS VENDOR_ID,

                '' AS SOURCE_BASED_STATUS,

                '' AS REGISTRATION_CERTIFICATE,
                '' AS NPA_STAGEID,

                ifNull(v.LEAD_ID, '') AS LEAD_ID,

                ifNull(v.LEAD_STATUS, '') AS LEAD_STATUS,

                ifNull(v.CREATED_DATE, '') AS CREATED_DATE,

                ifNull(v.SOURCE_BASED_LAST_UPDATED, '')
                    AS SOURCE_BASED_LAST_UPDATED,

                '' AS ASSET_VALUE,

                ifNull(v.DATE_OF_INCORPORATION, '')
                    AS DATE_OF_INCORPORATION,

                '' AS USER_EMPLOYEE_NO,

                ifNull(v.DATE_OF_BIRTH, '')
                    AS DATE_OF_BIRTH,

                '' AS DATE_OF_JOINING,
                '' AS ALLOCATION_REGION_CODE,
                '' AS ALLOCATION_REGION_TYPE,

                r.CUID,
                r.CUID_CREATION_DATE,

                'Loaded from VF_LEAP_LOS_Latest' AS REMARKS

            FROM
            (
                SELECT *
                FROM CUID.VF_LEAP_LOS
                ORDER BY version DESC
                LIMIT 1 BY checksum1
            ) v

            INNER JOIN CUID.CUID_REPOSITORY r
                ON upper(trimBoth(v.PAN_NO))
                = upper(trimBoth(r.PAN_NO))

            LEFT ANTI JOIN CUID.CUID_MASTER_TABLE cm
                ON upper(trimBoth(v.PAN_NO))
                = upper(trimBoth(cm.PAN_NO))
            AND cm.SOURCE = 'VF_LOS'

            WHERE
                v.PAN_NO IS NOT NULL
                AND trimBoth(v.PAN_NO) != ''
                AND match(
                    upper(trimBoth(v.PAN_NO)),
                    '^[A-Z]{5}[0-9]{4}[A-Z]$'
                )
                AND v.LastUpdated >= today()
                AND v.LastUpdated < today() + 1
        """)

        logger.info("🎯 ETL COMPLETED SUCCESSFULLY")

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_number = exc_traceback.tb_lineno
        logger.error(f"❌ ETL FAILED at line {line_number}: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    run_etl()