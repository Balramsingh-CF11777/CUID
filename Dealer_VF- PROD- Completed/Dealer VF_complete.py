import os
import logging
import uuid
import oracledb
import clickhouse_connect
from dotenv import load_dotenv
import valkey
from valkey.connection import BlockingConnectionPool

# ============================================================
# ENV LOAD
# ============================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 50000))

# ============================================================
# ORACLE CONFIG
# ============================================================

ORACLE_HOST = os.getenv("ORACLE_HOST")
ORACLE_PORT = int(os.getenv("ORACLE_PORT"))
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICENAME")

# ============================================================
# CLICKHOUSE CONFIG
# ============================================================

ENV = os.getenv("ENVIRON", "UAT").upper()

DEST_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
DEST_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
DEST_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
DEST_PASSWORD = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD")
DEST_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE")

TEMP_TABLE = "VF_DEALER_TEMP"
FINAL_TABLE = "VF_DEALER"

MASTER_TABLE = "CUID.CUID_MASTER_TABLE"
REPOSITORY_TABLE = "CUID.CUID_REPOSITORY"
NEW_PAN_STAGE_TABLE = "CUID.CUID_NEW_PAN_STAGE"

SOURCE_NAME = "VF_DEALER"

# ============================================================
# VALKEY CONFIG
# ============================================================

VALKEY_HOST = os.getenv(f"{ENV}_VALKEY_HOST")
VALKEY_PORT = int(os.getenv(f"{ENV}_VALKEY_PORT"))
VALKEY_USERNAME = os.getenv(f"{ENV}_VALKEY_USERNAME", "")
VALKEY_PASSWORD = os.getenv(f"{ENV}_VALKEY_PASSWORD", "")
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

    logger.info(
        f"Current Valkey Counter: {current_counter}"
    )

    end_id = int(
        client.incrby(
            VALKEY_KEY,
            count
        )
    )

    start_id = end_id - count + 1

    logger.info(
        f"Reserved CUID Range: "
        f"{start_id} -> {end_id}"
    )

    return start_id, end_id


# ============================================================
# MAIN ETL
# ============================================================

def run_etl():
    oracle_conn = None
    oracle_cursor = None
    dest = None

    job_id = str(uuid.uuid4())

    try:
        logger.info(
            "============================================================"
        )

        logger.info(
            f"🚀 VF Dealer FULL REFRESH Started | Run ID: {job_id}"
        )

        logger.info(
            "============================================================"
        )

        # ====================================================
        # CONNECT ORACLE
        # ====================================================

        dsn = oracledb.makedsn(
            ORACLE_HOST,
            ORACLE_PORT,
            service_name=ORACLE_SERVICE_NAME
        )

        logger.info(
            "🔌 Connecting to Oracle..."
        )

        oracle_conn = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=dsn
        )

        oracle_cursor = oracle_conn.cursor()

        # ====================================================
        # CONNECT CLICKHOUSE
        # ====================================================

        logger.info(
            "🔌 Connecting to ClickHouse..."
        )

        dest = clickhouse_connect.get_client(
            host=DEST_HOST,
            port=DEST_PORT,
            username=DEST_USER,
            password=DEST_PASSWORD,
            database=DEST_DB
        )

        # ====================================================
        # STEP 1
        # TRUNCATE TEMP TABLE
        # ====================================================

        logger.info(
            f"🧹 Truncating {TEMP_TABLE}..."
        )

        dest.command(
            f"""
            TRUNCATE TABLE {DEST_DB}.{TEMP_TABLE}
            """
        )

        # ====================================================
        # STEP 2
        # FETCH COMPLETE VF DEALER DATA FROM ORACLE
        # ====================================================

        logger.info(
            "📊 Fetching complete VF Dealer data from Oracle..."
        )

        oracle_query = """
            SELECT DISTINCT
                b.PANNO,
                b.ACTIVE_STATUS,
                b.ACC_HOLD_NAME AS NAME_OF_MASTER,
                TO_CHAR(
                    b.MAKEDATE,
                    'YYYY-MM-DD HH24:MI:SS'
                ) AS MAKEDATE,
                TO_CHAR(
                    b.AUTHDATE,
                    'YYYY-MM-DD HH24:MI:SS'
                ) AS LASTUPDATED_NEW,
                'Broker Master' AS TYPE_OF_BROKER,
                'VF' AS VF_BK_Master
            FROM FINMAS.LOS_BROKER_MAS@finmas_rpt b

            UNION ALL

            SELECT DISTINCT
                sup.PAN_NO AS PANNO,
                sup.ACTIVE_STATUS,
                sup.PAYEE_NAME AS NAME_OF_MASTER,
                TO_CHAR(
                    sup.MAKEDATE,
                    'YYYY-MM-DD HH24:MI:SS'
                ) AS MAKEDATE,
                TO_CHAR(
                    sup.LASTUPDATED_NEW,
                    'YYYY-MM-DD HH24:MI:SS'
                ) AS LASTUPDATED_NEW,
                'Dealer Master' AS TYPE_OF_BROKER,
                'VF' AS VF_BK_Master
            FROM FINLEN.LEA_SUPPLIER_M sup

            UNION ALL

            SELECT DISTINCT
                BR1.PANNO,
                b.ACTIVE_STATUS,
                b.BROKERNAME AS NAME_OF_MASTER,
                TO_CHAR(
                    b.MAKEDATE,
                    'YYYY-MM-DD HH24:MI:SS'
                ) AS MAKEDATE,
                TO_CHAR(
                    b.LASTUPDATED_NEW,
                    'YYYY-MM-DD HH24:MI:SS'
                ) AS LASTUPDATED_NEW,
                'DSA Master' AS TYPE_OF_BROKER,
                'VF' AS VF_BK_Master
            FROM FINMAS.LEA_BROKER_M@finmas_rpt b
            JOIN FINMAS.LOS_BROKER_MAS@finmas_rpt BR1
                ON BR1.BROKERID = b.BROKERID
        """

        oracle_cursor.execute(
            oracle_query
        )

        total_rows = 0

        # ====================================================
        # STEP 3
        # ORACLE -> TEMP TABLE
        # ====================================================

        while True:
            rows = oracle_cursor.fetchmany(
                BATCH_SIZE
            )

            if not rows:
                break

            clean_rows = []

            for row in rows:
                clean_row = tuple(
                    None
                    if value is None
                    else str(value).strip()
                    for value in row
                )

                clean_rows.append(
                    clean_row
                )

            if not clean_rows:
                continue

            dest.insert(
                f"{DEST_DB}.{TEMP_TABLE}",
                clean_rows,
                column_names=[
                    "PANNO",
                    "ACTIVE_STATUS",
                    "NAME_OF_MASTER",
                    "MAKEDATE",
                    "LASTUPDATED_NEW",
                    "TYPE_OF_BROKER",
                    "VF_BK_MASTER"
                ]
            )

            batch_count = len(
                clean_rows
            )

            total_rows += batch_count

            logger.info(
                f"➡ TEMP Inserted: {batch_count:,} | "
                f"Total: {total_rows:,}"
            )

        logger.info(
            f"✅ Oracle -> TEMP load completed | "
            f"Rows: {total_rows:,}"
        )

        # ====================================================
        # TEMP COUNT
        # ====================================================

        temp_count = dest.query(
            f"""
            SELECT count()
            FROM {DEST_DB}.{TEMP_TABLE}
            """
        ).result_rows[0][0]

        logger.info(
            f"📊 TEMP Row Count: {temp_count:,}"
        )

        # ====================================================
        # SAFETY CHECK
        # ====================================================

        if temp_count == 0:
            raise RuntimeError(
                "VF_DEALER_TEMP contains 0 rows. "
                "Full refresh stopped to prevent accidental "
                "deletion of existing VF_DEALER data."
            )

        # ====================================================
        # STEP 4
        # TRUNCATE VF_DEALER SOURCE MASTER
        # ====================================================

        logger.info(
            f"🧹 Truncating source master "
            f"{DEST_DB}.{FINAL_TABLE}..."
        )

        dest.command(
            f"""
            TRUNCATE TABLE {DEST_DB}.{FINAL_TABLE}
            """
        )

        logger.info(
            f"✅ {FINAL_TABLE} truncated."
        )

        # ====================================================
        # STEP 5
        # TEMP -> VF_DEALER
        # FULL RELOAD
        # ====================================================

        logger.info(
            f"📦 Reloading complete {FINAL_TABLE} table..."
        )

        dest.command(
            f"""
            INSERT INTO {DEST_DB}.{FINAL_TABLE}
            (
                PANNO,
                ACTIVE_STATUS,
                NAME_OF_MASTER,
                MAKEDATE,
                LASTUPDATED_NEW,
                TYPE_OF_BROKER,
                VF_BK_MASTER,
                checksum1,
                checksum2,
                ETL_RUN_ID
            )
            SELECT
                PANNO,
                ACTIVE_STATUS,
                NAME_OF_MASTER,
                MAKEDATE,
                LASTUPDATED_NEW,
                TYPE_OF_BROKER,
                VF_BK_MASTER,
                checksum1,
                checksum2,
                '{job_id}' AS ETL_RUN_ID
            FROM
            (
                SELECT
                    upper(trimBoth(PANNO)) AS PANNO,
                    ACTIVE_STATUS,
                    NAME_OF_MASTER,
                    MAKEDATE,
                    LASTUPDATED_NEW,
                    TYPE_OF_BROKER,
                    VF_BK_MASTER,
                    checksum1,
                    checksum2
                FROM {DEST_DB}.{TEMP_TABLE}
                WHERE
                    PANNO IS NOT NULL
                    AND trimBoth(PANNO) != ''
                    AND match(
                        upper(trimBoth(PANNO)),
                        '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                    )
                ORDER BY
                    checksum1,
                    coalesce(
                        parseDateTimeBestEffortOrNull(
                            LASTUPDATED_NEW
                        ),
                        parseDateTimeBestEffortOrNull(
                            MAKEDATE
                        ),
                        toDateTime(0)
                    ) DESC,
                    checksum2 DESC
                LIMIT 1 BY checksum1
            ) t
            """
        )

        # ====================================================
        # FINAL COUNT
        # ====================================================

        final_count = dest.query(
            f"""
            SELECT count()
            FROM {DEST_DB}.{FINAL_TABLE}
            """
        ).result_rows[0][0]

        final_pan_count = dest.query(
            f"""
            SELECT countDistinct(
                upper(trimBoth(PANNO))
            )
            FROM {DEST_DB}.{FINAL_TABLE}
            """
        ).result_rows[0][0]

        logger.info(
            f"✅ {FINAL_TABLE} full reload completed | "
            f"Rows: {final_count:,} | "
            f"Distinct PANs: {final_pan_count:,}"
        )

        # ====================================================
        # STEP 6
        # PREPARE NEW PAN STAGING
        # ====================================================

        logger.info(
            "🆔 Preparing new PAN staging table..."
        )

        dest.command(
            f"""
            TRUNCATE TABLE {NEW_PAN_STAGE_TABLE}
            """
        )

        dest.command(
            f"""
            INSERT INTO {NEW_PAN_STAGE_TABLE}
            SELECT DISTINCT
                upper(trimBoth(d.PANNO)) AS PAN_NO
            FROM {DEST_DB}.{FINAL_TABLE} d
            LEFT ANTI JOIN {REPOSITORY_TABLE} r
                ON upper(trimBoth(d.PANNO))
                = upper(trimBoth(r.PAN_NO))
            WHERE
                d.PANNO IS NOT NULL
                AND trimBoth(d.PANNO) != ''
                AND match(
                    upper(trimBoth(d.PANNO)),
                    '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                )
            """
        )

        pending_repository = dest.query(
            f"""
            SELECT count()
            FROM {NEW_PAN_STAGE_TABLE}
            """
        ).result_rows[0][0]

        logger.info(
            f"🆕 New PANs requiring CUID: "
            f"{pending_repository:,}"
        )

        # ====================================================
        # STEP 7
        # CREATE CUID ONLY FOR NEW PAN
        # ====================================================

        inserted_repository = 0

        if pending_repository > 0:
            start_id, end_id = reserve_cuid_range(
                pending_repository
            )

            logger.info(
                f"Reserved CUID Range: "
                f"{start_id} -> {end_id}"
            )

            repository_sql = f"""
                INSERT INTO {REPOSITORY_TABLE}
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
                    now() AS CUID_CREATION_DATE
                FROM
                (
                    SELECT DISTINCT
                        s.PAN_NO
                    FROM {NEW_PAN_STAGE_TABLE} s
                    LEFT ANTI JOIN {REPOSITORY_TABLE} r
                        ON upper(trimBoth(s.PAN_NO))
                        = upper(trimBoth(r.PAN_NO))
                ) t
            """

            dest.command(
                repository_sql
            )

            inserted_repository = dest.query(
                f"""
                SELECT countDistinct(
                    s.PAN_NO
                )
                FROM {NEW_PAN_STAGE_TABLE} s
                INNER JOIN {REPOSITORY_TABLE} r
                    ON upper(trimBoth(s.PAN_NO))
                    = upper(trimBoth(r.PAN_NO))
                """
            ).result_rows[0][0]

        else:
            logger.info(
                "ℹ No new PANs. All VF Dealer PANs "
                "already have CUIDs."
            )

        logger.info(
            f"✅ Repository PANs available: "
            f"{inserted_repository:,}"
        )

        gap_count = (
            pending_repository
            - inserted_repository
        )

        logger.info(
            f"Repository Validation | "
            f"Pending={pending_repository:,} | "
            f"Available={inserted_repository:,} | "
            f"Gap={gap_count:,}"
        )

        if gap_count > 0:
            raise RuntimeError(
                f"{gap_count} PAN(s) from staging "
                f"were not found in CUID_REPOSITORY."
            )

        # ====================================================
        # STEP 8
        # VALIDATE EVERY FINAL PAN HAS A CUID
        # ====================================================

        missing_cuid_count = dest.query(
            f"""
            SELECT countDistinct(
                upper(trimBoth(d.PANNO))
            )
            FROM {DEST_DB}.{FINAL_TABLE} d
            LEFT ANTI JOIN {REPOSITORY_TABLE} r
                ON upper(trimBoth(d.PANNO))
                = upper(trimBoth(r.PAN_NO))
            WHERE
                d.PANNO IS NOT NULL
                AND trimBoth(d.PANNO) != ''
                AND match(
                    upper(trimBoth(d.PANNO)),
                    '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                )
            """
        ).result_rows[0][0]

        logger.info(
            f"🔎 PANs without CUID after repository processing: "
            f"{missing_cuid_count:,}"
        )

        if missing_cuid_count > 0:
            raise RuntimeError(
                f"{missing_cuid_count} valid PAN(s) "
                f"do not have CUIDs. "
                f"Master refresh stopped."
            )

        # ====================================================
        # STEP 9
        # COUNT EXISTING VF_DEALER MASTER ROWS
        # ====================================================

        existing_master_count = dest.query(
            f"""
            SELECT count()
            FROM {MASTER_TABLE}
            WHERE SOURCE = '{SOURCE_NAME}'
            """
        ).result_rows[0][0]

        logger.info(
            f"📊 Existing {SOURCE_NAME} rows in "
            f"CUID_MASTER_TABLE: {existing_master_count:,}"
        )

        # ====================================================
        # STEP 10
        # DELETE COMPLETE VF_DEALER SOURCE
        # FROM CUID_MASTER_TABLE
        #
        # IMPORTANT:
        # mutations_sync = 1 ensures delete completes
        # before we perform the new insert.
        # ====================================================

        logger.info(
            f"🗑️ Deleting ALL existing "
            f"SOURCE='{SOURCE_NAME}' rows "
            f"from CUID_MASTER_TABLE..."
        )

        dest.command(
            f"""
            ALTER TABLE {MASTER_TABLE}
            DELETE WHERE SOURCE = '{SOURCE_NAME}'
            SETTINGS mutations_sync = 1
            """
        )

        logger.info(
            f"✅ Existing {SOURCE_NAME} master rows deleted."
        )

        # ====================================================
        # VERIFY DELETE
        # ====================================================

        remaining_master_count = dest.query(
            f"""
            SELECT count()
            FROM {MASTER_TABLE}
            WHERE SOURCE = '{SOURCE_NAME}'
            """
        ).result_rows[0][0]

        logger.info(
            f"🔎 Remaining {SOURCE_NAME} rows after delete: "
            f"{remaining_master_count:,}"
        )

        if remaining_master_count != 0:
            raise RuntimeError(
                f"Master cleanup failed. "
                f"{remaining_master_count} "
                f"{SOURCE_NAME} rows still remain."
            )

        # ====================================================
        # STEP 11
        # INSERT COMPLETE VF_DEALER DATASET
        # INTO CUID_MASTER_TABLE
        # ====================================================

        logger.info(
            f"📦 Inserting COMPLETE {SOURCE_NAME} dataset "
            f"into CUID_MASTER_TABLE..."
        )

        dest.command(
            f"""
            INSERT INTO {MASTER_TABLE}
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
                d.TYPE_OF_BROKER AS TYPE,
                '{SOURCE_NAME}' AS SOURCE,
                upper(trimBoth(d.PANNO)) AS PAN_NO,
                ifNull(
                    d.NAME_OF_MASTER,
                    ''
                ) AS NAME,
                '' AS GST_NO,
                '' AS AADHAAR_NO,
                '' AS CKYCID,
                '' AS TIN_NO,
                '' AS VOTER_ID,
                '' AS DRIVING_LICN_NO,
                '' AS RATIONCARD_NO,
                '' AS PASSPORT,
                '' AS MOBILE_NO,
                '' AS EMAIL_ID,
                '' AS APPLICATIONNO,
                '' AS COAPPLICANT_PAN_NO,
                '' AS GUARANTOR_PAN_NO,
                '' AS CIF_NO,
                '' AS AGREEMENTNO,
                '' AS CUST_UNQ_ID,
                '' AS CUSTID,
                '' AS VENDOR_ID,
                ifNull(
                    d.ACTIVE_STATUS,
                    ''
                ) AS SOURCE_BASED_STATUS,
                '' AS REGISTRATION_CERTIFICATE,
                '' AS NPA_STAGEID,
                '' AS LEAD_ID,
                '' AS LEAD_STATUS,
                ifNull(
                    d.MAKEDATE,
                    ''
                ) AS CREATED_DATE,
                ifNull(
                    d.LASTUPDATED_NEW,
                    ''
                ) AS SOURCE_BASED_LAST_UPDATED,
                '' AS ASSET_VALUE,
                '' AS DATE_OF_INCORPORATION,
                '' AS USER_EMPLOYEE_NO,
                '' AS DATE_OF_BIRTH,
                '' AS DATE_OF_JOINING,
                '' AS ALLOCATION_REGION_CODE,
                '' AS ALLOCATION_REGION_TYPE,
                r.CUID,
                r.CUID_CREATION_DATE,
                'Loaded from {SOURCE_NAME}' AS REMARKS
            FROM {DEST_DB}.{FINAL_TABLE} d
            INNER JOIN {REPOSITORY_TABLE} r
                ON upper(trimBoth(d.PANNO))
                = upper(trimBoth(r.PAN_NO))
            WHERE
                d.PANNO IS NOT NULL
                AND trimBoth(d.PANNO) != ''
                AND match(
                    upper(trimBoth(d.PANNO)),
                    '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                )
            """
        )

        logger.info(
            "✅ CUID_MASTER_TABLE full source reload completed."
        )

        # ====================================================
        # STEP 12
        # FINAL MASTER VALIDATION
        # ====================================================

        master_count = dest.query(
            f"""
            SELECT count()
            FROM {MASTER_TABLE}
            WHERE SOURCE = '{SOURCE_NAME}'
            """
        ).result_rows[0][0]

        master_pan_count = dest.query(
            f"""
            SELECT countDistinct(PAN_NO)
            FROM {MASTER_TABLE}
            WHERE SOURCE = '{SOURCE_NAME}'
            """
        ).result_rows[0][0]

        logger.info(
            "============================================================"
        )

        logger.info(
            f"🎯 VF Dealer FULL REFRESH COMPLETED SUCCESSFULLY"
        )

        logger.info(
            f"Run ID               : {job_id}"
        )

        logger.info(
            f"Oracle/TEMP Rows      : {total_rows:,}"
        )

        logger.info(
            f"VF_DEALER Rows        : {final_count:,}"
        )

        logger.info(
            f"VF_DEALER PANs        : {final_pan_count:,}"
        )

        logger.info(
            f"New PANs              : {pending_repository:,}"
        )

        logger.info(
            f"Master Rows Deleted   : {existing_master_count:,}"
        )

        logger.info(
            f"Master Rows Inserted  : {master_count:,}"
        )

        logger.info(
            f"Master Distinct PANs  : {master_pan_count:,}"
        )

        logger.info(
            "============================================================"
        )

    except Exception as e:
        logger.exception(
            f"❌ VF Dealer ETL FAILED | "
            f"Run ID: {job_id} | "
            f"Error: {e}"
        )

        raise

    finally:
        try:
            if oracle_cursor:
                oracle_cursor.close()
        except Exception:
            pass

        try:
            if oracle_conn:
                oracle_conn.close()
        except Exception:
            pass

        try:
            if dest:
                dest.close()
        except Exception:
            pass


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_etl()