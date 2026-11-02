-- `CUID`.idnty_dg_blk_trg_sec_temp definition
CREATE TABLE
  CUID.idnty_dg_blk_trg_sec_temp (
    `UCIC_NUMBER` Nullable(String),
    `CUST_UNQ_ID` String,
    `CUSTID` Nullable(String),
    `CUSTOMER_NAME` Nullable(String),
    `DOB` Nullable(String),
    `DATE_OF_INCORPORATION` Nullable(String),
    `PAN` Nullable(String),
    `AADHAAR_NO` Nullable(String),
    `ckycid` Nullable(String),
    `TIN_NO` Nullable(String),
    `VOTER_ID` Nullable(String),
    `DRIVING_LICN_NO` Nullable(String),
    `RATIONCARD_NO` Nullable(String),
    `PASSPORT` Nullable(String),
    `REGISTRATION_CERTIFICATE` Nullable(String),
    `checksum1` UInt64 MATERIALIZED cityHash64(
      concat(
        coalesce(UCIC_NUMBER, ''),
        coalesce(CUST_UNQ_ID, ''),
        coalesce(CUSTID, ''),
        coalesce(CUSTOMER_NAME, ''),
        coalesce(toString(DOB), ''),
        coalesce(PAN, ''),
        coalesce(AADHAAR_NO, ''),
        coalesce(ckycid, ''),
        coalesce(TIN_NO, ''),
        coalesce(VOTER_ID, ''),
        coalesce(DRIVING_LICN_NO, ''),
        coalesce(RATIONCARD_NO, ''),
        coalesce(PASSPORT, ''),
        coalesce(toString(DATE_OF_INCORPORATION), ''),
        coalesce(REGISTRATION_CERTIFICATE, '')
      )
    ),
    `checksum2` UInt64 MATERIALIZED cityHash64(
      concat(
        coalesce(UCIC_NUMBER, ''),
        coalesce(CUST_UNQ_ID, ''),
        coalesce(CUSTID, ''),
        coalesce(CUSTOMER_NAME, ''),
        coalesce(toString(DOB), ''),
        coalesce(PAN, ''),
        coalesce(AADHAAR_NO, ''),
        coalesce(ckycid, ''),
        coalesce(TIN_NO, ''),
        coalesce(VOTER_ID, ''),
        coalesce(DRIVING_LICN_NO, ''),
        coalesce(RATIONCARD_NO, ''),
        coalesce(PASSPORT, ''),
        coalesce(toString(DATE_OF_INCORPORATION), ''),
        coalesce(REGISTRATION_CERTIFICATE, '')
      )
    )
  ) ENGINE = MergeTree
ORDER BY
  (UCIC_NUMBER, CUST_UNQ_ID) SETTINGS allow_nullable_key = 1,
  index_granularity = 8192;


-- `CUID`.idnty_dg_blk_trg_sec definition
CREATE TABLE
  CUID.idnty_dg_blk_trg_sec (
    `UCIC_NUMBER` Nullable(String),
    `CUST_UNQ_ID` String,
    `CUSTID` Nullable(String),
    `CUSTOMER_NAME` Nullable(String),
    `DOB` Nullable(String),
    `DATE_OF_INCORPORATION` Nullable(String),
    `PAN` Nullable(String),
    `AADHAAR_NO` Nullable(String),
    `ckycid` Nullable(String),
    `TIN_NO` Nullable(String),
    `VOTER_ID` Nullable(String),
    `DRIVING_LICN_NO` Nullable(String),
    `RATIONCARD_NO` Nullable(String),
    `PASSPORT` Nullable(String),
    `REGISTRATION_CERTIFICATE` Nullable(String),
    `checksum1` UInt64,
    `checksum2` UInt64,
    `version` UInt64 DEFAULT toUnixTimestamp64Milli(now64()),
    `LastUpdated` DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(version)
ORDER BY
  checksum SETTINGS index_granularity = 8192;