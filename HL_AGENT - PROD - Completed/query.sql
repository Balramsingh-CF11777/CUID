-- `CUID`.`HL_AGENT_TEMP` definition
CREATE TABLE
  CUID.HL_AGENT_TEMP (
    `TYPE` Nullable(String),
    `NAME` Nullable(String),
    `DATE_OF_BIRTH` Nullable(String),
    `PAN_NO` Nullable(String),
    `MOBILE_NO` Nullable(String),
    `EMAIL_ID` Nullable(String),
    `ALLOCATION_REGION_CODE` Nullable(String),
    `SOURCE_BASED_STATUS` Nullable(String),
    `CREATED_DATE` Nullable(String),
    `SOURCE_BASED_LAST_UPDATED` Nullable(String),
    `checksum1` UInt64 MATERIALIZED cityHash64(
      (
        coalesce(PAN_NO, ''),
        coalesce(NAME, ''),
        coalesce(DATE_OF_BIRTH, ''),
        coalesce(MOBILE_NO, ''),
        coalesce(EMAIL_ID, ''),
        coalesce(ALLOCATION_REGION_CODE, ''),
        coalesce(SOURCE_BASED_STATUS, '')
      )
    ),
    `checksum2` UInt64 MATERIALIZED cityHash64(
      (
        coalesce(PAN_NO, ''),
        coalesce(NAME, ''),
        coalesce(DATE_OF_BIRTH, ''),
        coalesce(MOBILE_NO, ''),
        coalesce(EMAIL_ID, ''),
        coalesce(ALLOCATION_REGION_CODE, ''),
        coalesce(SOURCE_BASED_STATUS, ''),
        coalesce(CREATED_DATE, ''),
        coalesce(SOURCE_BASED_LAST_UPDATED, '')
      )
    )
  ) ENGINE = MergeTree
ORDER BY
  checksum1 SETTINGS allow_nullable_key = 1,
  index_granularity = 8192;

----

-- `CUID`.`HL_AGENT` definition
CREATE TABLE
  CUID.HL_AGENT (
    `TYPE` Nullable(String),
    `NAME` Nullable(String),
    `DATE_OF_BIRTH` Nullable(String),
    `PAN_NO` Nullable(String),
    `MOBILE_NO` Nullable(String),
    `EMAIL_ID` Nullable(String),
    `ALLOCATION_REGION_CODE` Nullable(String),
    `SOURCE_BASED_STATUS` Nullable(String),
    `CREATED_DATE` Nullable(String),
    `SOURCE_BASED_LAST_UPDATED` Nullable(String),
    `checksum1` UInt64,
    `checksum2` UInt64,
    `version` UInt64 DEFAULT toUnixTimestamp64Milli(now64()),
    `LastUpdated` DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(version)
ORDER BY
  checksum1 SETTINGS allow_nullable_key = 1,
  index_granularity = 8192;