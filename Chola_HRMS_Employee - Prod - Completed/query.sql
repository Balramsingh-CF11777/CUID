-- `CUID`.`HRMS_EMPLOYEE_TEMP` definition
CREATE TABLE
  CUID.HRMS_EMPLOYEE_TEMP (
    `NAME` Nullable(String),
    `PAN_NO` Nullable(String),
    `AADHAAR_NO` Nullable(String),
    `MOBILE_NO` Nullable(String),
    `EMAIL_ID` Nullable(String),
    `USER_EMPLOYEE_NO` Nullable(String),
    `DATE_OF_BIRTH` Nullable(String),
    `DATE_OF_JOINING` Nullable(String),
    `SOURCE_BASED_STATUS` Nullable(String),
    `SOURCE_BASED_LAST_UPDATED` Nullable(String),
    `ALLOCATION_REGION_CODE` Nullable(String),
    `ALLOCATION_REGION_TYPE` Nullable(String),
    `checksum1` UInt64 MATERIALIZED cityHash64(
      tuple(
        coalesce(NAME, ''),
        coalesce(PAN_NO, ''),
        coalesce(AADHAAR_NO, ''),
        coalesce(MOBILE_NO, ''),
        coalesce(EMAIL_ID, ''),
        coalesce(USER_EMPLOYEE_NO, ''),
        coalesce(DATE_OF_BIRTH, ''),
        coalesce(DATE_OF_JOINING, ''),
        coalesce(ALLOCATION_REGION_CODE, ''),
        coalesce(ALLOCATION_REGION_TYPE, '')
      )
    )
  ),
  `checksum2` UInt64 MATERIALIZED cityHash64(
    tuple(
      coalesce(NAME, ''),
      coalesce(PAN_NO, ''),
      coalesce(AADHAAR_NO, ''),
      coalesce(MOBILE_NO, ''),
      coalesce(EMAIL_ID, ''),
      coalesce(USER_EMPLOYEE_NO, ''),
      coalesce(DATE_OF_BIRTH, ''),
      coalesce(DATE_OF_JOINING, ''),
      coalesce(SOURCE_BASED_STATUS, ''),
      coalesce(SOURCE_BASED_LAST_UPDATED, ''),
      coalesce(ALLOCATION_REGION_CODE, ''),
      coalesce(ALLOCATION_REGION_TYPE, '')
    )
  ) ENGINE = MergeTree
ORDER BY
  checksum1 SETTINGS allow_nullable_key = 1,
  index_granularity = 8192;


CREATE TABLE
  CUID.HRMS_EMPLOYEE (
    `NAME` Nullable(String),
    `PAN_NO` Nullable(String),
    `AADHAAR_NO` Nullable(String),
    `MOBILE_NO` Nullable(String),
    `EMAIL_ID` Nullable(String),
    `USER_EMPLOYEE_NO` Nullable(String),
    `DATE_OF_BIRTH` Nullable(String),
    `DATE_OF_JOINING` Nullable(String),
    `SOURCE_BASED_STATUS` Nullable(String),
    `SOURCE_BASED_LAST_UPDATED` Nullable(String),
    `ALLOCATION_REGION_CODE` Nullable(String),
    `ALLOCATION_REGION_TYPE` Nullable(String),
    `checksum1` UInt64 ,
    `checksum2` UInt64,
    `version` UInt64 DEFAULT toUnixTimestamp64Milli(now64()),
    `LastUpdated` DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(version)
ORDER BY
  checksum1 SETTINGS allow_nullable_key = 1,
  index_granularity = 8192;