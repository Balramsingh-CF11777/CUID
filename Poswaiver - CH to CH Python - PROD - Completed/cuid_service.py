import os
import logging
import valkey
from dotenv import load_dotenv
from valkey.connection import BlockingConnectionPool

load_dotenv()

logger = logging.getLogger(__name__)


class CUIDService:

    def __init__(self):

        self.env = os.getenv(
            "ENVIRON",
            "UAT"
        ).upper()

        self.valkey_host = os.getenv(
            f"{self.env}_VALKEY_HOST"
        )

        self.valkey_port = int(
            os.getenv(
                f"{self.env}_VALKEY_PORT"
            )
        )

        self.valkey_username = os.getenv(
            f"{self.env}_VALKEY_USERNAME",
            ""
        )

        self.valkey_password = os.getenv(
            f"{self.env}_VALKEY_PASSWORD",
            ""
        )

        self.valkey_key = "cuid_counter"

    # ==========================================
    # VALKEY CONNECTION
    # ==========================================
    def get_valkey_client(self):

        try:

            if self.env == "UAT":

                client = valkey.Valkey(
                    host=self.valkey_host,
                    port=self.valkey_port,
                    decode_responses=True
                )

            else:

                pool = BlockingConnectionPool(
                    host=self.valkey_host,
                    port=self.valkey_port,
                    username=self.valkey_username,
                    password=self.valkey_password,
                    db=0,
                    max_connections=100,
                    timeout=5,
                    decode_responses=True
                )

                client = valkey.Valkey(
                    connection_pool=pool
                )

            logger.info(
                f"Connected Valkey "
                f"{self.valkey_host}:"
                f"{self.valkey_port}"
            )

            return client

        except Exception:

            logger.exception(
                "Valkey connection failed"
            )

            raise

    # ==========================================
    # GET CURRENT COUNTER
    # ==========================================
    def get_counter(self):

        client = self.get_valkey_client()

        counter = client.get(
            self.valkey_key
        )

        logger.info(
            f"Current Counter : "
            f"{counter}"
        )

        return int(counter or 0)

    # ==========================================
    # RESERVE RANGE
    # ==========================================
    def reserve_range(self, count):

        client = self.get_valkey_client()

        current_counter = client.get(
            self.valkey_key
        )

        logger.info(
            f"Current Counter : "
            f"{current_counter}"
        )

        end_id = int(
            client.incrby(
                self.valkey_key,
                count
            )
        )

        start_id = (
            end_id
            - count
            + 1
        )

        logger.info(
            f"Reserved Range : "
            f"{start_id} -> {end_id}"
        )

        return start_id, end_id
    
    def create_cuids(self, dest, source_table):
        dest.command("""
            TRUNCATE TABLE
            CUID.CUID_NEW_PAN_STAGE
        """)

        dest.command(f"""
            INSERT INTO
            CUID.CUID_NEW_PAN_STAGE

            SELECT DISTINCT
                upper(trimBoth(pan_no))
            FROM {source_table}

            WHERE
                pan_no IS NOT NULL
                AND trimBoth(pan_no) != ''
                AND match(
                    upper(trimBoth(pan_no)),
                    '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
                )
                AND upper(trimBoth(pan_no))
                NOT IN
                (
                    SELECT PAN_NO
                    FROM CUID.CUID_REPOSITORY
                )
        """)

        pan_count = dest.query("""
            SELECT count()
            FROM CUID.CUID_NEW_PAN_STAGE
        """).result_rows[0][0]

        if pan_count == 0:
            return 0

        start_id, end_id = self.reserve_range(
            pan_count
        )

        dest.command(f"""
            INSERT INTO
            CUID.CUID_REPOSITORY
            (
                PAN_NO,
                CUID,
                CUID_CREATION_DATE
            )
            SELECT
                s.PAN_NO,

                lpad(
                    toString(
                        row_number()
                        OVER (
                            ORDER BY s.PAN_NO
                        )
                        + {start_id}
                        - 1
                    ),
                    16,
                    '0'
                ),

                now()

            FROM
            (
                SELECT PAN_NO
                FROM
                CUID.CUID_NEW_PAN_STAGE
            ) s

            LEFT JOIN
            CUID.CUID_REPOSITORY r
                ON s.PAN_NO = r.PAN_NO

            WHERE r.PAN_NO IS NULL
        """)

        return pan_count