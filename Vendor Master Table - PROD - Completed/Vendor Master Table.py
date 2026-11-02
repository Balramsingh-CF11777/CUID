import os
import time
import logging
import uuid
from datetime import datetime

import clickhouse_connect
from dotenv import load_dotenv
from retrying import retry

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

ENV = os.getenv("ENVIRON", "UAT").upper()

CH_HOST = os.getenv(f"{ENV}_CUID_CLICKHOUSE_HOST")
CH_PORT = int(os.getenv(f"{ENV}_CUID_CLICKHOUSE_PORT"))
CH_USER = os.getenv(f"{ENV}_CUID_CLICKHOUSE_USER")
CH_PASSWORD = os.getenv(f"{ENV}_CUID_CLICKHOUSE_PASSWORD")
CH_DB = os.getenv(f"{ENV}_CUID_CLICKHOUSE_DATABASE", "CUID")

SOURCE_TABLE = "CUID_MASTER_TABLE"
TARGET_TABLE = "VENDOR_CUID_MASTER_TABLE"
VENDOR_MASTER_TABLE = "VENDOR_OUTSOURCING_MASTER"

SOURCES = [
    "VF_DEALER",
    "HL_AGENT",
    "LAP_AGENT",
    "BAZ_VENDOR",
    "HRMS_EMPLOYEE"
]


@retry(stop_max_attempt_number=3, wait_fixed=5000)
def execute_with_retry(client, query):
    return client.command(query)


def get_count(client, query):
    result = client.query(query)

    if not result.result_rows:
        return 0

    return result.result_rows[0][0]


def validate_source(client, source):
    source_count = get_count(
        client,
        f"""
        SELECT count()
        FROM {CH_DB}.{SOURCE_TABLE}
        WHERE SOURCE = '{source}'
          AND TYPE != 'CUSTOMER'
        """
    )

    target_count = get_count(
        client,
        f"""
        SELECT count()
        FROM {CH_DB}.{TARGET_TABLE}
        WHERE SOURCE = '{source}'
        """
    )

    return source_count, target_count


def delete_source(client, source):
    logger.info(
        f"🧹 Deleting existing rows for SOURCE={source}"
    )

    execute_with_retry(
        client,
        f"""
        ALTER TABLE {CH_DB}.{TARGET_TABLE}
        DELETE WHERE SOURCE = '{source}'
        SETTINGS mutations_sync = 1
        """
    )

    remaining_count = get_count(
        client,
        f"""
        SELECT count()
        FROM {CH_DB}.{TARGET_TABLE}
        WHERE SOURCE = '{source}'
        """
    )

    if remaining_count != 0:
        raise RuntimeError(
            f"Delete validation failed for {source}. "
            f"Remaining rows: {remaining_count}"
        )

    logger.info(
        f"✅ Existing {source} rows deleted successfully"
    )


def insert_source(client, source):
    logger.info(
        f"📥 Inserting SOURCE={source} into "
        f"{TARGET_TABLE}"
    )

    execute_with_retry(
        client,
        f"""
        INSERT INTO {CH_DB}.{TARGET_TABLE}
        (
            TYPE,
            SOURCE,
            PAN_NO,
            NAME,
            CATEGORY_SERVICES,
            CRITICALITY,
            OUTSOURCING_PARTNER,
            IT_VENDOR,
            GST_NO,
            MOBILE_NO,
            EMAIL_ID,
            VENDOR_ID,
            SOURCE_BASED_STATUS,
            CREATED_DATE,
            ALLOCATION_REGION_CODE,
            ALLOCATION_REGION_TYPE,
            CUID,
            CUID_CREATION_DATE,
            REMARKS
        )
        SELECT
            c.TYPE,
            c.SOURCE,
            c.PAN_NO,
            c.NAME,

            ifNull(
                v.CATEGORY_SERVICES,
                ''
            ) AS CATEGORY_SERVICES,

            ifNull(
                v.CRITICALITY,
                ''
            ) AS CRITICALITY,

            ifNull(
                v.OUTSOURCING_PARTNER,
                ''
            ) AS OUTSOURCING_PARTNER,

            toUInt8(
                notEmpty(
                    v.NORMALIZED_PAN_NO
                )
            ) AS IT_VENDOR,

            c.GST_NO,
            c.MOBILE_NO,
            c.EMAIL_ID,
            c.VENDOR_ID,
            c.SOURCE_BASED_STATUS,
            c.CREATED_DATE,
            c.ALLOCATION_REGION_CODE,
            c.ALLOCATION_REGION_TYPE,
            c.CUID,
            c.CUID_CREATION_DATE,
            c.REMARKS

        FROM {CH_DB}.{SOURCE_TABLE} AS c

        LEFT JOIN
        (
            SELECT
                NORMALIZED_PAN_NO,
                any(CATEGORY_SERVICES)
                    AS CATEGORY_SERVICES,
                any(CRITICALITY)
                    AS CRITICALITY,
                any(OUTSOURCING_PARTNER)
                    AS OUTSOURCING_PARTNER
            FROM
            (
                SELECT
                    upper(
                        trim(PAN_NO)
                    ) AS NORMALIZED_PAN_NO,

                    CATEGORY_SERVICES,
                    CRITICALITY,
                    OUTSOURCING_PARTNER

                FROM {CH_DB}.{VENDOR_MASTER_TABLE}

                WHERE
                    PAN_NO IS NOT NULL
                    AND notEmpty(
                        trim(PAN_NO)
                    )
            )
            GROUP BY NORMALIZED_PAN_NO
        ) AS v

            ON upper(
                trim(
                    ifNull(
                        c.PAN_NO,
                        ''
                    )
                )
            ) = v.NORMALIZED_PAN_NO

        WHERE
            c.SOURCE = '{source}'
            AND c.TYPE != 'CUSTOMER'
        """
    )

    logger.info(
        f"✅ Insert completed for SOURCE={source}"
    )


def refresh_source(client, source):
    source_start = time.time()

    logger.info(
        "============================================================"
    )
    logger.info(
        f"🚀 Processing SOURCE={source}"
    )
    logger.info(
        "============================================================"
    )

    source_count = get_count(
        client,
        f"""
        SELECT count()
        FROM {CH_DB}.{SOURCE_TABLE}
        WHERE SOURCE = '{source}'
          AND TYPE != 'CUSTOMER'
        """
    )

    existing_target_count = get_count(
        client,
        f"""
        SELECT count()
        FROM {CH_DB}.{TARGET_TABLE}
        WHERE SOURCE = '{source}'
        """
    )

    logger.info(
        f"Source rows available : {source_count}"
    )

    logger.info(
        f"Existing target rows  : {existing_target_count}"
    )

    delete_source(
        client,
        source
    )

    insert_source(
        client,
        source
    )

    final_source_count, final_target_count = validate_source(
        client,
        source
    )

    duration_seconds = round(
        time.time() - source_start,
        2
    )

    logger.info(
        f"Source Rows : {final_source_count}"
    )

    logger.info(
        f"Target Rows : {final_target_count}"
    )

    if final_source_count != final_target_count:
        logger.warning(
            f"⚠ Row count mismatch for {source}: "
            f"Source={final_source_count}, "
            f"Target={final_target_count}"
        )
    else:
        logger.info(
            f"✅ Row count validated for {source}"
        )

    logger.info(
        f"⏱ SOURCE={source} completed in "
        f"{duration_seconds} seconds"
    )

    return {
        "source": source,
        "source_count": final_source_count,
        "target_count": final_target_count,
        "duration_seconds": duration_seconds
    }


def run_refresh():
    start_time = time.time()

    job_id = (
        f"VENDOR_CUID_REFRESH_"
        f"{uuid.uuid4()}"
    )

    job_start = datetime.now()

    client = None

    results = []

    logger.info(
        "============================================================"
    )

    logger.info(
        "🚀 VENDOR CUID MASTER REFRESH STARTED"
    )

    logger.info(
        f"Job ID : {job_id}"
    )

    logger.info(
        f"Started: "
        f"{job_start.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    logger.info(
        "============================================================"
    )

    try:
        logger.info(
            "🔌 Connecting to ClickHouse..."
        )

        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DB
        )

        logger.info(
            "✅ ClickHouse connected successfully"
        )

        for source in SOURCES:
            try:
                result = refresh_source(
                    client,
                    source
                )

                results.append(result)

            except Exception as source_error:
                logger.exception(
                    f"❌ Failed processing "
                    f"SOURCE={source}: "
                    f"{source_error}"
                )

                raise

        total_source_rows = sum(
            item["source_count"]
            for item in results
        )

        total_target_rows = sum(
            item["target_count"]
            for item in results
        )

        total_duration = round(
            (time.time() - start_time) / 60,
            2
        )

        logger.info(
            "============================================================"
        )

        logger.info(
            "🎯 VENDOR CUID MASTER REFRESH COMPLETED"
        )

        logger.info(
            f"Job ID            : {job_id}"
        )

        logger.info(
            f"Total Source Rows : {total_source_rows}"
        )

        logger.info(
            f"Total Target Rows : {total_target_rows}"
        )

        logger.info(
            f"Duration          : "
            f"{total_duration} minutes"
        )

        logger.info(
            "============================================================"
        )

        for result in results:
            logger.info(
                f"{result['source']} | "
                f"Source={result['source_count']} | "
                f"Target={result['target_count']} | "
                f"Duration={result['duration_seconds']} sec"
            )

    except Exception as e:
        logger.exception(
            f"❌ VENDOR CUID MASTER REFRESH FAILED | "
            f"Job ID={job_id} | "
            f"Error={e}"
        )

        raise

    finally:
        if client:
            try:
                client.close()
            except Exception:
                pass


if __name__ == "__main__":
    run_refresh()