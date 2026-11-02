SELECT
  OA.name AS NAME,
  OA.panNumber AS PAN_NO,
  OA.gstNumber AS GST_NO,
  OA.aadhar AS AADHAR_NO,
  OA.voterId AS VOTER_ID,
  OA.drivingLicense AS DRIVING_LICN_NO,
  OA.passport AS PASSPORT,
  OA.mobile AS MOBILE_NO,
  OA.email AS EMAIL_ID,
  OA.application_id AS APPLICATIONNO,
  SEC.COAPPLICANT_PAN_NO,
  SEC.GUARANTOR_PAN_NO,
  OA.agreementNumber AS AGREEMENTNO,
  OA.customerId AS CUST_UNQ_ID,
  OA.customerId AS CUSTID,
  OA.application_id AS LEAD_ID,
  OA.application_status AS LEAD_STATUS,
  OA.cre_time AS CREATED_DATE,
  OA.mod_time AS SOURCE_BASED_LAST_UPDATED,
  TRY_CONVERT(DATE, OA.cre_time) AS DATE_OF_INCORPORATION,
  TRY_CONVERT(DATE, OA.dob) AS DATE_OF_BIRTH,
  OA.[branchInfo.regionName] AS ALLOCATION_REGION_CODE,
  OA.[branchInfo.zoneName] AS ALLOCATION_REGION_TYPE
FROM
  LOSDATA..oaoapplicants_full_v1 OA
WITH
  (NOLOCK)
  LEFT JOIN (
    SELECT
      primaryApplicantId,
      STRING_AGG(
        CASE
          WHEN applicant <> 'guarantor'
          AND NULLIF(LTRIM(RTRIM(panNumber)), '') IS NOT NULL THEN UPPER(LTRIM(RTRIM(panNumber)))
        END,
        ','
      ) AS COAPPLICANT_PAN_NO,
      STRING_AGG(
        CASE
          WHEN applicant = 'guarantor'
          AND NULLIF(LTRIM(RTRIM(panNumber)), '') IS NOT NULL THEN UPPER(LTRIM(RTRIM(panNumber)))
        END,
        ','
      ) AS GUARANTOR_PAN_NO
    FROM
      LOSDATA..oaoapplicants_full_v1
    WITH
      (NOLOCK)
    GROUP BY
      primaryApplicantId
  ) SEC ON SEC.primaryApplicantId = OA.application_id
WHERE
  OA.agreementNumber IS NOT NULL
UNION ALL
SELECT
  DISTINCT CU.CBSI_CUSFISNAM AS NAME,
  UPPER(LTRIM(RTRIM(CU.CBSI_PAN))) AS PAN_NO,
  CU.CBSI_GSTNUM AS GST_NO,
  CU.CBSI_AADHAR AS AADHAAR_NO,
  CU.CBSI_VOTERID AS VOTER_ID,
  CU.CBSI_DRIVLICNS AS DRIVING_LICN_NO,
  CU.CBSI_PASSPORT AS PASSPORT,
  CA.CADI_MOBNOI AS MOBILE_NO,
  CA.CADI_EMAILID AS EMAIL_ID,
  CU.CBSI_APPLCNTNO AS APPLICATIONNO,
  CO.COAPPLICANT_PAN_NO,
  GU.GUARANTOR_PAN_NO,
  T.AcctID AS AGREEMENTNO,
  CU.CBSI_CUSID AS CUST_UNQ_ID,
  CU.CBSI_CUSID AS CUSTID,
  CU.CBSI_APPLCNTNO AS LEAD_ID,
  LON.LBSI_APPSTATUS AS LEAD_STATUS,
  ISNULL(CONVERT(VARCHAR, LON.LBSI_DTCREATED, 120), '') AS CREATED_DATE,
  ISNULL(
    CONVERT(
      VARCHAR,
      COALESCE(LON.LBSI_DISBBUSDT, LON.LBSI_DTMODIFIED),
      120
    ),
    ''
  ) AS SOURCE_BASED_LAST_UPDATED,
  ISNULL(CONVERT(VARCHAR, CU.CBSI_DTCREATED, 120), '') AS DATE_OF_INCORPORATION,
  ISNULL(CONVERT(VARCHAR, CU.CBSI_DOB, 120), '') AS DATE_OF_BIRTH,
  BR.REGION AS ALLOCATION_REGION_CODE,
  BR.ZONE AS ALLOCATION_REGION_TYPE
FROM
  TPLSW..LSW_TCUSBASICINFO CU
  LEFT JOIN TPLSW..LSW_TCUSADDRINFO CA ON CA.CADI_PRCSID = CU.CBSI_PRCSID
  LEFT JOIN (
    SELECT
      COBI_PRCSID,
      STRING_AGG(UPPER(LTRIM(RTRIM(COBI_PAN))), ',') AS COAPPLICANT_PAN_NO
    FROM
      TPLSW..LSW_TCOCUSBASICINFO
    WHERE
      NULLIF(LTRIM(RTRIM(COBI_PAN)), '') IS NOT NULL
    GROUP BY
      COBI_PRCSID
  ) CO ON CO.COBI_PRCSID = CU.CBSI_PRCSID
  LEFT JOIN (
    SELECT
      GRBI_PRCSID,
      STRING_AGG(UPPER(LTRIM(RTRIM(GRBI_PAN))), ',') AS GUARANTOR_PAN_NO
    FROM
      TPLSW..LSW_TGURCUSBASICINFO
    WHERE
      NULLIF(LTRIM(RTRIM(GRBI_PAN)), '') IS NOT NULL
    GROUP BY
      GRBI_PRCSID
  ) GU ON GU.GRBI_PRCSID = CU.CBSI_PRCSID
  LEFT JOIN TPLSW..LSW_TLONBASICINFO LON ON LON.LBSI_PRCSID = CU.CBSI_PRCSID
  LEFT JOIN DEVHFC02..SAM_TBRMST BR ON BR.BrId = CU.CBSI_BRANCH
  INNER JOIN DEVHFC02..LON_TINSTENT T ON T.AlterAcctID = CU.CBSI_APPLCNTNO
  AND T.AcctID <> CU.CBSI_APPLCNTNO
WHERE
  CU.CBSI_APPLCNTNO IS NOT NULL;


-- `CUID`.`HL_LOS_TEMP` definition
CREATE TABLE
  CUID.HL_LOS_TEMP (
    `NAME` Nullable(String),
    `PAN_NO` Nullable(String),
    `GST_NO` Nullable(String),
    `AADHAR_NO` Nullable(String),
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
    `CREATED_DATE` Nullable(String),
    `SOURCE_BASED_STATUS` Nullable(String),
    `DATE_OF_INCORPORATION` Nullable(String),
    `DATE_OF_BIRTH` Nullable(String),
    `ALLOCATION_REGION_CODE` Nullable(String),
    `ALLOCATION_REGION_TYPE` Nullable(String),
    `checksum1` UInt64 MATERIALIZED cityHash64(
      tuple(
        coalesce(PAN_NO, ''),
        coalesce(GST_NO, ''),
        coalesce(AADHAR_NO, ''),
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
    ),
    `checksum1` UInt64 MATERIALIZED cityHash64(
      tuple(
        coalesce(PAN_NO, ''),
        coalesce(GST_NO, ''),
        coalesce(AADHAR_NO, ''),
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
        coalesce(LEAD_STATUS, ''),
        coalesce(CREATED_DATE, ''),
        coalesce(SOURCE_BASED_STATUS, ''),
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


-- `CUID`.`HL_LOS` definition
CREATE TABLE
  CUID.HL_LOS (
    `NAME` Nullable(String),
    `PAN_NO` Nullable(String),
    `GST_NO` Nullable(String),
    `AADHAR_NO` Nullable(String),
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
    `CREATED_DATE` Nullable(String),
    `SOURCE_BASED_STATUS` Nullable(String),
    `DATE_OF_INCORPORATION` Nullable(String),
    `DATE_OF_BIRTH` Nullable(String),
    `ALLOCATION_REGION_CODE` Nullable(String),
    `ALLOCATION_REGION_TYPE` Nullable(String),
    `checksum1` UInt64,
    `checksum2` UInt64,
    `version` UInt64 DEFAULT toUnixTimestamp64Milli(now()),
    `LastUpdated` DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(version)
ORDER BY
  checksum1 SETTINGS allow_nullable_key = 1,
  index_granularity = 8192;