SELECT DISTINCT
    upper(trim(KF.pan_no)) AS PAN_NO,
    gl.aadhaar_no AS AADHAR_NO,
    KF.voterid_no AS VOTER_ID,
    KF.driving_license_no AS DRIVING_LICN_NO,
    MO.maskedvalue AS MOBILE_NO,
    ad.email_id AS EMAIL_ID,
    toString (gl.loan_application_id) AS APPLICATIONNO,
    gl.ucic AS CIF_NO,
    gl.loan_agreement_number AS AGREEMENTNO,
    gl.npastatus AS NPA_STAGEID,
    gl.lead_id AS LEAD_ID,
    gl.status AS LEAD_STATUS,
    bd.branchcode AS ALLOCATION_REGION_CODE,
    bd.branchdesc AS ALLOCATION_REGION_TYPE
FROM
    GOLD.gl_lead gl
    LEFT JOIN GOLD.gl_lead_branch_details bd ON bd.id = gl.id
    LEFT JOIN GOLD.gl_lead_additional_details ad ON ad.id = gl.id
    LEFT JOIN GOLD.gl_lead_mobile_no MO ON MO.id = gl.id
    LEFT JOIN GOLD.gl_lead_kyc_flags KF ON KF._id = gl._id
WHERE
    KF.pan_no IS NOT NULL
    AND trim(KF.pan_no) != ''
    AND length (trim(KF.pan_no)) = 10
    AND match (
        upper(trim(KF.pan_no)),
        '^[A-Z]{{5}}[0-9]{{4}}[A-Z]$'
    )
ORDER BY
    gl.loan_application_id
DROP TABLE IF EXISTS CUID.EDLS_GOLD_LEAD;

CREATE TABLE
    CUID.EDLS_GOLD_LEAD (
        PAN_NO Nullable (String),
        AADHAR_NO Nullable (String),
        VOTER_ID Nullable (String),
        DRIVING_LICN_NO Nullable (String),
        MOBILE_NO Nullable (String),
        EMAIL_ID Nullable (String),
        APPLICATIONNO Nullable (String),
        CIF_NO Nullable (String),
        AGREEMENTNO Nullable (String),
        NPA_STAGEID Nullable (String),
        LEAD_ID Nullable (String),
        LEAD_STATUS Nullable (String),
        ALLOCATION_REGION_CODE Nullable (String),
        ALLOCATION_REGION_TYPE Nullable (String),
        checksum UInt64,
        version UInt64 DEFAULT toUnixTimestamp64Milli (now64 ()),
        LastUpdated DateTime DEFAULT now ()
    ) ENGINE = ReplacingMergeTree (version)
ORDER BY
    (PAN_NO, APPLICATIONNO) SETTINGS allow_nullable_key = 1;

DROP TABLE IF EXISTS CUID.EDLS_GOLD_LEAD_TEMP;

CREATE TABLE
    CUID.EDLS_GOLD_LEAD_TEMP (
        PAN_NO Nullable (String),
        AADHAR_NO Nullable (String),
        VOTER_ID Nullable (String),
        DRIVING_LICN_NO Nullable (String),
        MOBILE_NO Nullable (String),
        EMAIL_ID Nullable (String),
        APPLICATIONNO Nullable (String),
        CIF_NO Nullable (String),
        AGREEMENTNO Nullable (String),
        NPA_STAGEID Nullable (String),
        LEAD_ID Nullable (String),
        LEAD_STATUS Nullable (String),
        ALLOCATION_REGION_CODE Nullable (String),
        ALLOCATION_REGION_TYPE Nullable (String),
        checksum UInt64 MATERIALIZED cityHash64 (
            concat (
                coalesce(PAN_NO, ''),
                coalesce(AADHAR_NO, ''),
                coalesce(VOTER_ID, ''),
                coalesce(DRIVING_LICN_NO, ''),
                coalesce(MOBILE_NO, ''),
                coalesce(EMAIL_ID, ''),
                coalesce(APPLICATIONNO, ''),
                coalesce(CIF_NO, ''),
                coalesce(AGREEMENTNO, ''),
                coalesce(NPA_STAGEID, ''),
                coalesce(LEAD_ID, ''),
                coalesce(LEAD_STATUS, ''),
                coalesce(ALLOCATION_REGION_CODE, ''),
                coalesce(ALLOCATION_REGION_TYPE, '')
            )
        )
    ) ENGINE = MergeTree
ORDER BY
    (PAN_NO, APPLICATIONNO) SETTINGS allow_nullable_key = 1;

INSERT INTO
    CUID.EDLS_GOLD_LEAD (
        PAN_NO,
        AADHAR_NO,
        VOTER_ID,
        DRIVING_LICN_NO,
        MOBILE_NO,
        EMAIL_ID,
        APPLICATIONNO,
        CIF_NO,
        AGREEMENTNO,
        NPA_STAGEID,
        LEAD_ID,
        LEAD_STATUS,
        ALLOCATION_REGION_CODE,
        ALLOCATION_REGION_TYPE,
        checksum
    )
SELECT
    PAN_NO,
    AADHAR_NO,
    VOTER_ID,
    DRIVING_LICN_NO,
    MOBILE_NO,
    EMAIL_ID,
    APPLICATIONNO,
    CIF_NO,
    AGREEMENTNO,
    NPA_STAGEID,
    LEAD_ID,
    LEAD_STATUS,
    ALLOCATION_REGION_CODE,
    ALLOCATION_REGION_TYPE,
    checksum
FROM
    CUID.EDLS_GOLD_LEAD_TEMP
    ---- Incremental update
INSERT INTO
    CUID.EDLS_GOLD_LEAD (
        PAN_Number,
        Customer_Name,
        Mobile_No,
        APPLICATION_ID,
        Agreement_No,
        DOB,
        BRANCH_ID,
        Region,
        checksum
    )
SELECT
    t.PAN_Number,
    t.Customer_Name,
    t.Mobile_No,
    t.APPLICATION_ID,
    t.Agreement_No,
    t.DOB,
    t.BRANCH_ID,
    t.Region,
    t.checksum
FROM
    CUID.EDLS_GOLD_LEAD_TEMP t
    LEFT JOIN CUID.EDLS_GOLD_LEAD m ON t.checksum = m.checksum
WHERE
    m.checksum IS NULL;

------
SELECT DISTINCT
    SOURCE,
    COUNT() as count_row
FROM
    CUID.CUID_MASTER_TABLE
GROUP BY
    SOURCE
ORDER BY
    count_row DESC;

66310
ALTER TABLE CUID.CUID_MASTER_TABLE DELETE
WHERE
    SOURCE = 'EDLS_GOLD';

-------
INSERT INTO
    CUID.CUID_REPOSITORY (PAN_NO, CUID, CUID_CREATION_DATE)
SELECT
    new_pans.PAN_NO,
    lpad (
        toString (
            row_number() OVER (
                ORDER BY
                    new_pans.PAN_NO
            ) + ifNull (offset_table.max_cuid, 0)
        ),
        16,
        '0'
    ) AS CUID,
    now () AS CUID_CREATION_DATE
FROM
    (
        SELECT DISTINCT
            PAN_NO
        FROM
            CUID.EDLS_GOLD_LEAD
        WHERE
            PAN_NO IS NOT NULL
            AND trim(PAN_NO) != ''
            AND length (trim(PAN_NO)) = 10
            AND match (upper(trim(PAN_NO)), '^[A-Z]{5}[0-9]{4}[A-Z]$')
            AND PAN_NO NOT IN (
                SELECT
                    PAN_NO
                FROM
                    CUID.CUID_REPOSITORY
            )
    ) AS new_pans
    CROSS JOIN (
        SELECT
            max(toUInt64 (CUID)) AS max_cuid
        FROM
            CUID.CUID_REPOSITORY
    ) AS offset_table;

-------
INSERT INTO
    CUID.CUID_MASTER_TABLE (
        TYPE,
        SOURCE,
        NAME,
        PAN_NO,
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
        REGISTRATION_CERTIFICATE,
        NPA_STAGEID,
        LEAD_ID,
        LEAD_STATUS,
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
    'CUSTOMER',
    'EDLS_GOLD',
    '',
    p.PAN_NO,
    '',
    p.AADHAR_NO,
    '',
    '',
    p.VOTER_ID,
    p.DRIVING_LICN_NO,
    '',
    '',
    p.MOBILE_NO,
    p.EMAIL_ID,
    p.APPLICATIONNO,
    '',
    '',
    p.CIF_NO,
    p.AGREEMENTNO,
    '',
    '',
    '',
    '',
    p.NPA_STAGEID,
    p.LEAD_ID,
    p.LEAD_STATUS,
    '',
    '',
    '',
    '',
    '',
    p.ALLOCATION_REGION_CODE,
    p.ALLOCATION_REGION_TYPE,
    r.CUID,
    r.CUID_CREATION_DATE,
    'Loaded from EDLS_GOLD_LEAD'
FROM
    CUID.EDLS_GOLD_LEAD p
    INNER JOIN CUID.CUID_REPOSITORY r ON p.PAN_NO = r.PAN_NO
WHERE
    p.PAN_NO IS NOT NULL
    AND trim(p.PAN_NO) != '' OPTIMIZE TABLE CUID.CUID_MASTER_TABLE FINAL;