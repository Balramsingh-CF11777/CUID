-- =========================================================
-- FINAL TABLE
-- =========================================================
CREATE TABLE
  CUID.LAP_LOS (
    `PAN_NO` Nullable(String),
    `GST_NO` Nullable(String),
    `AADHAAR_NO` Nullable(String),
    `VOTER_ID` Nullable(String),
    `DRIVING_LICN_NO` Nullable(String),
    `PASSPORT` Nullable(String),
    `MOBILE_NO` Nullable(String),
    `EMAIL_ID` Nullable(String),
    `APPLICATIONNO` Nullable(String),
    `COAPPLICANT_PAN_NO` Nullable(String),
    `GUARANTOR_PAN_NO` Nullable(String),
    `AGREEMENTNO` Nullable(String),
    `CUST_UNQ_ID` Nullable(String),
    `CUSTID` Nullable(String),
    `LEAD_ID` Nullable(String),
    `LEAD_STATUS` Nullable(String),
    `SOURCE_BASED_LAST_UPDATED` Nullable(String),
    `DATE_OF_INCORPORATION` Nullable(String),
    `DATE_OF_BIRTH` Nullable(String),
    `ALLOCATION_REGION_CODE` Nullable(String),
    `ALLOCATION_REGION_TYPE` Nullable(String),
    `checksum` UInt64,
    `version` UInt64 DEFAULT toUnixTimestamp64Milli(now64()),
    `LastUpdated` DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(version)
ORDER BY
  checksum SETTINGS allow_nullable_key = 1,
  index_granularity = 8192;


-- =========================================================
-- TEMP TABLE
-- =========================================================
CREATE TABLE
  CUID.LAP_LOS_TEMP (
    `PAN_NO` Nullable(String),
    `GST_NO` Nullable(String),
    `AADHAAR_NO` Nullable(String),
    `VOTER_ID` Nullable(String),
    `DRIVING_LICN_NO` Nullable(String),
    `PASSPORT` Nullable(String),
    `MOBILE_NO` Nullable(String),
    `EMAIL_ID` Nullable(String),
    `APPLICATIONNO` Nullable(String),
    `COAPPLICANT_PAN_NO` Nullable(String),
    `GUARANTOR_PAN_NO` Nullable(String),
    `AGREEMENTNO` Nullable(String),
    `CUST_UNQ_ID` Nullable(String),
    `CUSTID` Nullable(String),
    `LEAD_ID` Nullable(String),
    `LEAD_STATUS` Nullable(String),
    `SOURCE_BASED_LAST_UPDATED` Nullable(String),
    `DATE_OF_INCORPORATION` Nullable(String),
    `DATE_OF_BIRTH` Nullable(String),
    `ALLOCATION_REGION_CODE` Nullable(String),
    `ALLOCATION_REGION_TYPE` Nullable(String),
    `checksum` UInt64 MATERIALIZED cityHash64(
      concat(
        coalesce(PAN_NO, ''),
        coalesce(GST_NO, ''),
        coalesce(AADHAAR_NO, ''),
        coalesce(VOTER_ID, ''),
        coalesce(DRIVING_LICN_NO, ''),
        coalesce(PASSPORT, ''),
        coalesce(MOBILE_NO, ''),
        coalesce(EMAIL_ID, ''),
        coalesce(APPLICATIONNO, ''),
        coalesce(COAPPLICANT_PAN_NO, ''),
        coalesce(GUARANTOR_PAN_NO, ''),
        coalesce(AGREEMENTNO, ''),
        coalesce(CUST_UNQ_ID, ''),
        coalesce(CUSTID, ''),
        coalesce(LEAD_ID, ''),
        coalesce(DATE_OF_INCORPORATION, ''),
        coalesce(DATE_OF_BIRTH, ''),
        coalesce(ALLOCATION_REGION_CODE, ''),
        coalesce(ALLOCATION_REGION_TYPE, '')
      )
    )
  ) ENGINE = MergeTree
ORDER BY
  checksum SETTINGS allow_nullable_key = 1,
  index_granularity = 8192;