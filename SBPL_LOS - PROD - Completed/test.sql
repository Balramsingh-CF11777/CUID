WITH
  aggregated_data AS (
    SELECT
      anyIf(fslcd.PAN_NO, fslcd.CUST_TYPE = 'APPLICANT') AS raw_PAN_NO,
      anyIf(
        trimBoth(ifNull(fslcd.AADHAAR_NO, '')),
        fslcd.CUST_TYPE = 'APPLICANT'
      ) AS raw_AADHAAR_NO,
      anyIf(fslcd.VOTER_ID, fslcd.CUST_TYPE = 'APPLICANT') AS raw_VOTER_ID,
      anyIf(
        fslcd.DRIVINGLICENSENO,
        fslcd.CUST_TYPE = 'APPLICANT'
      ) AS raw_DRIVING_LICN_NO,
      anyIf(fslcd.PASSPORTNO, fslcd.CUST_TYPE = 'APPLICANT') AS raw_PASSPORT,
      anyIf(fslcd.EMP_PH_NO, fslcd.CUST_TYPE = 'APPLICANT') AS raw_MOBILE_NO,
      anyIf(fslcd.EMAIL_ID, fslcd.CUST_TYPE = 'APPLICANT') AS raw_EMAIL_ID,
      anyIf(
        replaceAll(toString(fslcd.UCIC), ',', ''),
        fslcd.CUST_TYPE = 'APPLICANT'
      ) AS raw_CIF_NO,
      anyIf(fslcd.CUSTOMERID, fslcd.CUST_TYPE = 'APPLICANT') AS raw_CUSTID,
      'APPLICANT' AS Customer_Type,
      replaceAll(toString(flldv.LEADID), ',', '') AS raw_APPLICATIONNO,
      flldv.AGREEMENTNO AS raw_AGREEMENTNO,
      arrayStringConcat(
        arrayFilter(
          x - > trimBoth(x) != ''
          AND trimBoth(x) != ',',
          groupUniqArrayIf(
            upper(
              replaceRegexpAll(
                trimBoth(assumeNotNull(fslcd.PAN_NO)),
                '^,+|,+$',
                ''
              )
            ),
            fslcd.CUST_TYPE = 'COAPPLICANT'
          )
        ),
        ', '
      ) AS raw_COAPPLICANT_PAN_NO,
      replaceAll(toString(flldv.LEADID), ',', '') AS raw_LEAD_ID,
      flldv.STATUS AS raw_LEAD_STATUS,
      ifNull(
        formatDateTime(
          toDateTime(flldv.LEAD_GENERATED_DATE),
          '%Y-%m-%d %H:%i:%S'
        ),
        ''
      ) AS raw_CREATED_DATE,
      formatDateTime(flldv.DISBURSALDATE, '%Y-%m-%d %H:%i:%s') AS raw_SOURCE_BASED_LAST_UPDATED,
      flldv.TOTAL_ESTIMATE_VALUE AS raw_ASSET_VALUE,
      any(sbm.BRANCH_REGION) AS raw_BRANCH_REGION
    FROM
      `sbpl`.FACT_LEAD_LOAN_DTLS_VW flldv
      LEFT JOIN `sbpl`.FACT_SBPL_LOS_CUST_DTLS fslcd ON flldv.LEADID = fslcd.LEADID
      LEFT JOIN `sbpl`.sbpl_Branch_Master sbm ON sbm.BRANCHID = flldv.BRANCHID
    GROUP BY
      flldv.LEADID,
      flldv.AGREEMENTNO,
      flldv.STATUS,
      flldv.LEAD_GENERATED_DATE,
      flldv.DISBURSALDATE,
      flldv.TOTAL_ESTIMATE_VALUE
  )
SELECT
  ifNull(toString(raw_PAN_NO), '') AS PAN_NO,
  ifNull(toString(raw_AADHAAR_NO), '') AS AADHAAR_NO,
  ifNull(toString(raw_VOTER_ID), '') AS VOTER_ID,
  ifNull(toString(raw_DRIVING_LICN_NO), '') AS DRIVING_LICN_NO,
  ifNull(toString(raw_PASSPORT), '') AS PASSPORT,
  ifNull(toString(raw_MOBILE_NO), '') AS MOBILE_NO,
  ifNull(toString(raw_EMAIL_ID), '') AS EMAIL_ID,
  ifNull(toString(raw_CIF_NO), '') AS CIF_NO,
  ifNull(toString(raw_CUSTID), '') AS CUSTID,
  Customer_Type,
  ifNull(toString(raw_APPLICATIONNO), '') AS APPLICATIONNO,
  ifNull(toString(raw_AGREEMENTNO), '') AS AGREEMENTNO,
  ifNull(toString(raw_COAPPLICANT_PAN_NO), '') AS COAPPLICANT_PAN_NO,
  ifNull(toString(raw_LEAD_ID), '') AS LEAD_ID,
  ifNull(toString(raw_LEAD_STATUS), '') AS LEAD_STATUS,
  ifNull(toString(raw_CREATED_DATE), '') AS CREATED_DATE,
  ifNull(toString(raw_SOURCE_BASED_LAST_UPDATED), '') AS SOURCE_BASED_LAST_UPDATED,
  ifNull(toString(raw_ASSET_VALUE), '') AS ASSET_VALUE,
  ifNull(toString(raw_BRANCH_REGION), '') AS BRANCH_REGION
FROM
  aggregated_data;


-- `CUID`.`SBPL_LOS_TEMP` definition
CREATE TABLE
  CUID.SBPL_LOS_TEMP (
    `PAN_NO` Nullable(String),
    `AADHAAR_NO` Nullable(String),
    `VOTER_ID` Nullable(String),
    `DRIVING_LICN_NO` Nullable(String),
    `PASSPORT` Nullable(String),
    `MOBILE_NO` Nullable(String),
    `EMAIL_ID` Nullable(String),
    `CIF_NO` Nullable(String),
    `CUSTID` Nullable(String),
    `Customer_Type` Nullable(String),
    `APPLICATIONNO` Nullable(String),
    `AGREEMENTNO` Nullable(String),
    `COAPPLICANT_PAN_NO` Nullable(String),
    `LEAD_ID` Nullable(String),
    `LEAD_STATUS` Nullable(String),
    `SOURCE_BASED_LAST_UPDATED` Nullable(String),
    `ASSET_VALUE` Nullable(String),
    `USER_EMPLOYEE_NO` Nullable(String),
    `BRANCH_REGION` Nullable(String),
    `checksum1` UInt64 MATERIALIZED cityHash64(
      tuple(
        coalesce(PAN_NO, ''),
        coalesce(AADHAAR_NO, ''),
        coalesce(VOTER_ID, ''),
        coalesce(DRIVING_LICN_NO, ''),
        coalesce(PASSPORT, ''),
        coalesce(MOBILE_NO, ''),
        coalesce(EMAIL_ID, ''),
        coalesce(CIF_NO, ''),
        coalesce(CUSTID, ''),
        coalesce(Customer_Type, ''),
        coalesce(APPLICATIONNO, ''),
        coalesce(AGREEMENTNO, ''),
        coalesce(COAPPLICANT_PAN_NO, ''),
        coalesce(LEAD_ID, ''),
        coalesce(ASSET_VALUE, ''),
        coalesce(BRANCH_REGION, '')
      )
    ),
    `checksum2` UInt64 MATERIALIZED cityHash64(
      tuple(
        coalesce(PAN_NO, ''),
        coalesce(AADHAAR_NO, ''),
        coalesce(VOTER_ID, ''),
        coalesce(DRIVING_LICN_NO, ''),
        coalesce(PASSPORT, ''),
        coalesce(MOBILE_NO, ''),
        coalesce(EMAIL_ID, ''),
        coalesce(CIF_NO, ''),
        coalesce(CUSTID, ''),
        coalesce(Customer_Type, ''),
        coalesce(APPLICATIONNO, ''),
        coalesce(AGREEMENTNO, ''),
        coalesce(COAPPLICANT_PAN_NO, ''),
        coalesce(LEAD_ID, ''),
        coalesce(CREATED_DATE, ''),
        coalesce(LEAD_STATUS, ''),
        coalesce(SOURCE_BASED_LAST_UPDATED, ''),
        coalesce(ASSET_VALUE, ''),
        coalesce(BRANCH_REGION, '')
      )
    )
  ) ENGINE = MergeTree
ORDER BY
  checksum SETTINGS allow_nullable_key = 1,
  index_granularity = 8192;


-- `CUID`.`SBPL_LOS` definition
CREATE TABLE
  CUID.SBPL_LOS (
    `PAN_NO` Nullable(String),
    `AADHAAR_NO` Nullable(String),
    `VOTER_ID` Nullable(String),
    `DRIVING_LICN_NO` Nullable(String),
    `PASSPORT` Nullable(String),
    `MOBILE_NO` Nullable(String),
    `EMAIL_ID` Nullable(String),
    `CIF_NO` Nullable(String),
    `CUSTID` Nullable(String),
    `Customer_Type` Nullable(String),
    `APPLICATIONNO` Nullable(String),
    `AGREEMENTNO` Nullable(String),
    `COAPPLICANT_PAN_NO` Nullable(String),
    `LEAD_ID` Nullable(String),
    `LEAD_STATUS` Nullable(String),
    
    `SOURCE_BASED_LAST_UPDATED` Nullable(String),
    `ASSET_VALUE` Nullable(String),
    `BRANCH_REGION` Nullable(String),
    `checksum1` UInt64,
    `checksum2` UInt64,
    `version` UInt64 DEFAULT toUnixTimestamp64Milli(now64()),
    `LastUpdated` DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(version)
ORDER BY
  checksum SETTINGS allow_nullable_key = 1,
  index_granularity = 8192;