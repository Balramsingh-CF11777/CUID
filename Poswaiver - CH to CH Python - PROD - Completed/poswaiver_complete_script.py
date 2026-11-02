import clickhouse_connect
from dotenv import load_dotenv
import os
import time
import logging
from datetime import datetime
from retrying import retry
import uuid
import valkey

from valkey.connection import BlockingConnectionPool

# ============================================================
# ENV LOAD
# ============================================================
load_dotenv()

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ENV = os.getenv("ENVIRON", "UAT").upper()

# ============================================================
# VALKEY CONFIG
# ============================================================
VALKEY_HOST = os.getenv(f"{ENV}_VALKEY_HOST")
VALKEY_PORT = int(os.getenv(f"{ENV}_VALKEY_PORT"))

VALKEY_USERNAME = os.getenv(
    f"{ENV}_VALKEY_USERNAME",
    ""
)

VALKEY_PASSWORD = os.getenv(
    f"{ENV}_VALKEY_PASSWORD",
    ""
)

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

# ============================================================
# RESERVE CUID RANGE
# ============================================================
def reserve_cuid_range(count):

    client = get_valkey_client()

    current_counter = client.get(VALKEY_KEY)

    msg = f"Current Valkey Counter: {current_counter}"
    logger.info(msg)
    print(msg)

    end_id = int(
        client.incrby(
            VALKEY_KEY,
            count
        )
    )

    start_id = end_id - count + 1

    logger.info(
        f"🔒 Reserved CUID Range "
        f"{start_id} -> {end_id}"
    )

    return start_id, end_id


# -------- SOURCE CLICKHOUSE ----------

SRC_HOST = os.getenv("POSWAIVER_CLICKHOUSE_HOST")
SRC_PORT = int(os.getenv("POSWAIVER_CLICKHOUSE_PORT"))
SRC_USER = os.getenv("POSWAIVER_CLICKHOUSE_USER")
SRC_PASSWORD = os.getenv("POSWAIVER_CLICKHOUSE_PASSWORD")
SRC_DB = os.getenv("POSWAIVER_CLICKHOUSE_DATABASE")
SRC_TABLE = os.getenv("POSWAIVER_TABLE_NAME")

# -------- DEST CLICKHOUSE ----------
DEST_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
DEST_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
DEST_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
DEST_PASSWORD = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD")
DEST_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE")

TEMP_TABLE = "poswaiver_productlines_mysql_temp"
FINAL_TABLE = "poswaiver_productlines_mysql"

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000000))

# ============================================================
# RETRY DECORATOR
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
    job_id = f"POSWAIVER_{uuid.uuid4()}"

    source_count = 0
    temp_count = 0
    inserted_master = 0
    inserted_repository = 0

    logger.info("🔌 Connecting to ClickHouse...")

    src = clickhouse_connect.get_client(
        host=SRC_HOST, port=SRC_PORT,
        username=SRC_USER, password=SRC_PASSWORD,
        database=SRC_DB
    )

    dest = clickhouse_connect.get_client(
        host=DEST_HOST, port=DEST_PORT,
        username=DEST_USER, password=DEST_PASSWORD,
        database=DEST_DB
    )

    # =====================================================
    # STEP 1: TRUNCATE TEMP
    # =====================================================
    logger.info("🧹 Truncating temp table...")
    execute_with_retry(dest, f"TRUNCATE TABLE {DEST_DB}.{TEMP_TABLE}")

    # =====================================================
    # STEP 2: COUNT SOURCE
    # =====================================================
    source_count = src.query(
        f"SELECT count() FROM {SRC_DB}.{SRC_TABLE}"
    ).result_rows[0][0]

    logger.info(f"📊 Source rows: {source_count}")

    # =====================================================
    # STEP 3: STREAM LOAD TO TEMP (NO OFFSET / NO LAST_KEY)
    # =====================================================
    columns = [
        "CUSTOMERNAME",
        "pan_no",
        "RESIMOBILE",
        "email",
        "COAPPLICANT_PAN_NO",
        "GUARANTOR_PAN_NO",
        "cif_no",
        "AGREEMENTNO",
        "CUSTOMERID",
        "NPA_STAGEID",
        "dob",
        "EMP_ID"
    ]

    column_str = ", ".join(columns)
    total_loaded = 0

    offset = 0
    total_loaded = 0

    while True:

        query = f"""
            SELECT {column_str}
            FROM {SRC_DB}.{SRC_TABLE}
            LIMIT {BATCH_SIZE} OFFSET {offset}
        """

        rows = src.query(query).result_rows

        if not rows:
            break

        dest.insert(
            f"{DEST_DB}.{TEMP_TABLE}",
            rows,
            column_names=columns
        )

        batch_count = len(rows)
        total_loaded += batch_count
        offset += batch_count   # ✅ important (not fixed BATCH_SIZE)

        logger.info(f"➡ Loaded to temp: {total_loaded}")

        # stop when last batch is smaller
        if batch_count < BATCH_SIZE:
            break

    logger.info("✅ Temp load completed")

    temp_count = dest.query(
        f"SELECT count() FROM {DEST_DB}.{TEMP_TABLE}"
    ).result_rows[0][0]

    # =====================================================
    # STEP 4: INSERT INTO FINAL TABLE (DEDUP)
    # =====================================================
    logger.info("🔄 Deduplicating into final table...")

    # Check if FINAL table is empty
    master_count = dest.query(
        f"SELECT count() FROM {DEST_DB}.{FINAL_TABLE}"
    ).result_rows[0][0]

    # =========================
    # INITIAL LOAD
    # =========================
    if master_count == 0:
        logger.info("🚀 Initial Load → Direct Insert")

        execute_with_retry(dest, f"""
        INSERT INTO {DEST_DB}.{FINAL_TABLE}
        (
            CUSTOMERNAME,
            pan_no,
            RESIMOBILE,
            email,
            COAPPLICANT_PAN_NO,
            GUARANTOR_PAN_NO,
            cif_no,
            AGREEMENTNO,
            CUSTOMERID,
            NPA_STAGEID,
            dob,
            EMP_ID,
            checksum1,
            checksum2
        )
        SELECT
            CUSTOMERNAME,
            pan_no,
            RESIMOBILE,
            email,
            COAPPLICANT_PAN_NO,
            GUARANTOR_PAN_NO,
            cif_no,
            AGREEMENTNO,
            CUSTOMERID,
            NPA_STAGEID,
            dob,
            EMP_ID,
            checksum1,
            checksum2
        FROM {DEST_DB}.{TEMP_TABLE}
    """)

    # =========================
    # INCREMENTAL LOAD
    # =========================
    else:
        logger.info("🔄 Incremental Load → checksum-based insert")

        execute_with_retry(dest, f"""
            INSERT INTO {DEST_DB}.{FINAL_TABLE}
            (
                CUSTOMERNAME,
                pan_no,
                RESIMOBILE,
                email,
                COAPPLICANT_PAN_NO,
                GUARANTOR_PAN_NO,
                cif_no,
                AGREEMENTNO,
                CUSTOMERID,
                NPA_STAGEID,
                dob,
                EMP_ID,
                checksum1,
                checksum2
            )
            SELECT
                t.CUSTOMERNAME,
                t.pan_no,
                t.RESIMOBILE,
                t.email,
                t.COAPPLICANT_PAN_NO,
                t.GUARANTOR_PAN_NO,
                t.cif_no,
                t.AGREEMENTNO,
                t.CUSTOMERID,
                t.NPA_STAGEID,
                t.dob,
                t.EMP_ID,
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

    # =========================
    # FINAL OPTIMIZE
    # =========================
    
    # execute_with_retry(dest, f"""
    #     OPTIMIZE TABLE {DEST_DB}.{FINAL_TABLE} FINAL
    # """)

    logger.info("🆔 Preparing PAN staging table...")

    execute_with_retry(
        dest,
        "TRUNCATE TABLE CUID.CUID_NEW_PAN_STAGE"
    )

    # ------------------------------------------
    # Create PAN Stage Snapshot
    # ------------------------------------------

    execute_with_retry(dest, f"""
        INSERT INTO CUID.CUID_NEW_PAN_STAGE
        SELECT DISTINCT
            upper(trimBoth(pan_no)) AS PAN_NO
        FROM {DEST_DB}.{FINAL_TABLE}
        FINAL
        WHERE
            pan_no IS NOT NULL
            AND trimBoth(pan_no) != ''
            AND match(
                upper(trimBoth(pan_no)),
                '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
            )
            AND upper(trimBoth(pan_no)) NOT IN
            (
                SELECT PAN_NO
                FROM CUID.CUID_REPOSITORY
            )
        """)

    # ------------------------------------------
    # Count Stage PANs
    # ------------------------------------------

    new_pan_count = dest.query("""
    SELECT count()
    FROM CUID.CUID_NEW_PAN_STAGE
    """).result_rows[0][0]

    logger.info(f"📊 New PAN Count : {new_pan_count}")
    print(f"📊 New PAN Count : {new_pan_count}")

    pending_repository = new_pan_count

    logger.info(
        f"📦 Repository rows pending insert : "
        f"{pending_repository}"
    )

    print(
        f"📦 Repository rows pending insert : "
        f"{pending_repository}"
    )

    inserted_repository = 0

    # ------------------------------------------
    # Reserve Valkey Range + Insert Repository
    # ------------------------------------------

    if pending_repository > 0:

        logger.info(
            f"🔒 Reserving {pending_repository} IDs from Valkey"
        )

        print(
            f"🔒 Reserving {pending_repository} IDs from Valkey"
        )

        start_id, end_id = reserve_cuid_range(
            pending_repository
        )

        logger.info(
            f"🆔 Reserved Range : "
            f"{start_id} -> {end_id}"
        )

        print(
            f"🆔 Reserved Range : "
            f"{start_id} -> {end_id}"
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
                    OVER (ORDER BY PAN_NO)
                    + {start_id} - 1
                ),
                16,
                '0'
            ) AS CUID,

            toString(now()) AS CUID_CREATION_DATE

        FROM
        (
            SELECT DISTINCT s.PAN_NO
            FROM CUID.CUID_NEW_PAN_STAGE s

            LEFT ANTI JOIN
            (
                SELECT DISTINCT PAN_NO
                FROM CUID.CUID_REPOSITORY
            ) r
                ON s.PAN_NO = r.PAN_NO
        ) t
        """)

    else:

        logger.info("✅ No new PANs found")
        print("✅ No new PANs found")

    # ------------------------------------------
    # Repository Count
    # ------------------------------------------

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

    logger.info(
        f"✅ Repository Inserts : "
        f"{inserted_repository}"
    )

    print(
        f"✅ Repository Inserts : "
        f"{inserted_repository}"
    )

    gap_count = pending_repository - inserted_repository

    summary_msg = (
        f"Pending={pending_repository}, "
        f"Inserted={inserted_repository}, "
        f"Gap={gap_count}"
    )

    logger.info(summary_msg)
    print(summary_msg)

    if gap_count > 0:

        warning_msg = (
            f"⚠ {gap_count} repository rows were not inserted. "
            f"Possible concurrent API creation."
        )

        logger.warning(warning_msg)
        print(warning_msg)

    # =====================================================
    # STEP 6: MASTER TABLE INSERT
    # =====================================================

    logger.info("📦 Inserting into CUID_MASTER_TABLE...")

    execute_with_retry(dest, f"""
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

        WITH
        source_filtered AS
        (
            SELECT
                upper(trimBoth(pan_no)) AS normalized_pan,
                CUSTOMERNAME,
                RESIMOBILE,
                email,
                COAPPLICANT_PAN_NO,
                GUARANTOR_PAN_NO,
                cif_no,
                AGREEMENTNO,
                CUSTOMERID,
                NPA_STAGEID,
                EMP_ID,
                dob,
                checksum1,
                version
            FROM {DEST_DB}.{FINAL_TABLE}
            PREWHERE
                LastUpdated >= today()
                AND LastUpdated < today() + INTERVAL 1 DAY
            WHERE
                pan_no IS NOT NULL
                AND trimBoth(pan_no) != ''
                AND match(
                    upper(trimBoth(pan_no)),
                    '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                )
        ),

        latest_source AS
        (
            SELECT
                normalized_pan,
                CUSTOMERNAME,
                RESIMOBILE,
                email,
                COAPPLICANT_PAN_NO,
                GUARANTOR_PAN_NO,
                cif_no,
                AGREEMENTNO,
                CUSTOMERID,
                NPA_STAGEID,
                EMP_ID,
                dob,
                checksum1,
                version
            FROM source_filtered
            ORDER BY
                checksum1,
                version DESC
            LIMIT 1 BY checksum1
        ),

        repository AS
        (
            SELECT
                upper(trimBoth(PAN_NO)) AS normalized_pan,
                any(CUID) AS CUID,
                any(CUID_CREATION_DATE) AS CUID_CREATION_DATE
            FROM CUID.CUID_REPOSITORY
            WHERE
                PAN_NO IS NOT NULL
                AND trimBoth(PAN_NO) != ''
            GROUP BY normalized_pan
        ),

        existing_poswaiver AS
        (
            SELECT DISTINCT
                upper(trimBoth(PAN_NO)) AS normalized_pan
            FROM CUID.CUID_MASTER_TABLE
            PREWHERE SOURCE = 'POSWAIVER'
            WHERE
                PAN_NO IS NOT NULL
                AND trimBoth(PAN_NO) != ''
        )

        SELECT
            'CUSTOMER'                                     AS TYPE,
            'POSWAIVER'                                    AS SOURCE,
            p.normalized_pan                               AS PAN_NO,
            ifNull(p.CUSTOMERNAME, '')                     AS NAME,
            ''                                             AS GST_NO,
            ''                                             AS AADHAAR_NO,
            ''                                             AS CKYCID,
            ''                                             AS TIN_NO,
            ''                                             AS VOTER_ID,
            ''                                             AS DRIVING_LICN_NO,
            ''                                             AS RATIONCARD_NO,
            ''                                             AS PASSPORT,
            ifNull(p.RESIMOBILE, '')                       AS MOBILE_NO,
            ifNull(p.email, '')                            AS EMAIL_ID,
            ''                                             AS APPLICATIONNO,
            ifNull(p.COAPPLICANT_PAN_NO, '')               AS COAPPLICANT_PAN_NO,
            ifNull(p.GUARANTOR_PAN_NO, '')                 AS GUARANTOR_PAN_NO,
            ifNull(p.cif_no, '')                           AS CIF_NO,
            ifNull(p.AGREEMENTNO, '')                      AS AGREEMENTNO,
            ''                                             AS CUST_UNQ_ID,
            ifNull(p.CUSTOMERID, '')                       AS CUSTID,
            ''                                             AS VENDOR_ID,
            ''                                             AS SOURCE_BASED_STATUS,
            ''                                             AS REGISTRATION_CERTIFICATE,
            ifNull(p.NPA_STAGEID, '')                      AS NPA_STAGEID,
            ''                                             AS LEAD_ID,
            ''                                             AS LEAD_STATUS,
            ''                                             AS CREATED_DATE,
            ''                                             AS SOURCE_BASED_LAST_UPDATED,
            ''                                             AS ASSET_VALUE,
            ''                                             AS DATE_OF_INCORPORATION,
            ifNull(p.EMP_ID, '')                           AS USER_EMPLOYEE_NO,
            ifNull(p.dob, '')                              AS DATE_OF_BIRTH,
            ''                                             AS DATE_OF_JOINING,
            ''                                             AS ALLOCATION_REGION_CODE,
            ''                                             AS ALLOCATION_REGION_TYPE,
            r.CUID                                         AS CUID,
            r.CUID_CREATION_DATE                           AS CUID_CREATION_DATE,
            'Loaded from POSWAIVER'                        AS REMARKS

        FROM latest_source AS p

        ANY INNER JOIN repository AS r
            ON p.normalized_pan = r.normalized_pan

        LEFT ANTI JOIN existing_poswaiver AS cm
            ON p.normalized_pan = cm.normalized_pan

        SETTINGS
            join_algorithm = 'grace_hash',
            max_bytes_before_external_sort = 2000000000,
            max_bytes_before_external_group_by = 2000000000,
            max_threads = 4,
            max_block_size = 65536
    """)

    inserted_master = dest.query(f"""
        SELECT count()
        FROM CUID.CUID_MASTER_TABLE
        WHERE LastUpdated >= toDateTime('{job_start.strftime("%Y-%m-%d %H:%M:%S")}')
    """).result_rows[0][0]

    # =====================================================
    # STEP 7: AUDIT LOG
    # =====================================================
    duration = round((time.time() - start_time) / 60, 2)

    execute_with_retry(dest, f"""
        INSERT INTO CUID.ETL_AUDIT_LOG
        VALUES
        (
            '{job_id}',
            'POSWAIVER',
            toDateTime('{job_start.strftime("%Y-%m-%d %H:%M:%S")}'),
            now(),
            {int(source_count)},
            {int(temp_count)},
            {int(inserted_master)},
            {int(inserted_repository)},
            'SUCCESS',
            ''
        )
    """)

    logger.info(f"⏱ Completed in {duration} minutes")


# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    run_etl()