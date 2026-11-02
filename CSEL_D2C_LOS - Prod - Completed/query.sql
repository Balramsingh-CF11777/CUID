DROP TABLE IF EXISTS CUID.HL_CHANNEL_PARTNER;

CREATE TABLE
    CUID.HL_CHANNEL_PARTNER (
        PAN_Number Nullable (String),
        Customer_Name Nullable (String),
        Mobile_No Nullable (String),
        APPLICATION_ID Nullable (String),
        Agreement_No Nullable (String),
        DOB Nullable (String),
        BRANCH_ID Nullable (String),
        Region Nullable (String),
        checksum UInt64,
        version UInt64 DEFAULT toUnixTimestamp64Milli (now64 ()),
        LastUpdated DateTime DEFAULT now ()
    ) ENGINE = ReplacingMergeTree (version)
ORDER BY
    (APPLICATION_ID, PAN_Number) SETTINGS allow_nullable_key = 1;

CREATE TABLE
    CUID.HL_CHANNEL_PARTNER_TEMP (
        PAN_Number Nullable (String),
        Customer_Name Nullable (String),
        Mobile_No Nullable (String),
        APPLICATION_ID Nullable (String),
        Agreement_No Nullable (String),
        DOB Nullable (String),
        BRANCH_ID Nullable (String),
        Region Nullable (String),
        -- 🔎 Change Detection Checksum
        checksum UInt64 MATERIALIZED cityHash64 (
            concat (
                coalesce(PAN_Number, ''),
                coalesce(Customer_Name, ''),
                coalesce(Mobile_No, ''),
                coalesce(APPLICATION_ID, ''),
                coalesce(Agreement_No, ''),
                coalesce(DOB, ''),
                coalesce(BRANCH_ID, ''),
                coalesce(Region, '')
            )
        )
    ) ENGINE = MergeTree
ORDER BY
    (APPLICATION_ID, PAN_Number) SETTINGS allow_nullable_key = 1,
    index_granularity = 8192;

-- CUID.HL_CHANNEL_PARTNER_TEMP definition
--- First time insert
INSERT INTO
    CUID.HL_CHANNEL_PARTNER_TEMP (
        PAN_Number,
        Customer_Name,
        Mobile_No,
        APPLICATION_ID,
        Agreement_No,
        DOB,
        BRANCH_ID,
        Region
    )
SELECT
    PAN_Number,
    Customer_Name,
    toString (Mobile_No) AS Mobile_No,
    toString (APPLICATION_ID) AS APPLICATION_ID,
    Agreement_No,
    DOB,
    toString (BRANCH_ID) AS BRANCH_ID,
    Region
FROM
    CUID.HL_CHANNEL_PARTNER_TEMP_backup;

--- Incremental Insert
INSERT INTO
    CUID.HL_CHANNEL_PARTNER (
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
    CUID.HL_CHANNEL_PARTNER_TEMP t
    LEFT JOIN CUID.HL_CHANNEL_PARTNER m ON t.checksum = m.checksum
WHERE
    m.checksum IS NULL;

SELECT
    *
FROM
    CUID.HL_CHANNEL_PARTNER_TEMP_backup -- 54124
INSERT INTO
    CUID.HL_CHANNEL_PARTNER (
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
    PAN_Number,
    Customer_Name,
    Mobile_No,
    APPLICATION_ID,
    Agreement_No,
    DOB,
    BRANCH_ID,
    Region,
    checksum
FROM
    CUID.HL_CHANNEL_PARTNER_TEMP;

SELECT
    *
FROM
    CUID.CUID_MASTER_TABLE
WHERE
    SOURCE = 'HL_CHANNEL_PARTNER'
ALTER TABLE CUID.CUID_MASTER_TABLE DELETE
WHERE
    SOURCE = 'HL_CHANNEL_PARTNER';

INSERT INTO
    CUID.CUID_REPOSITORY (PAN_NO, CUID, CUID_CREATION_DATE)
SELECT
    new_pans.PAN_Number AS PAN_NO,
    lpad (
        toString (
            row_number() OVER (
                ORDER BY
                    new_pans.PAN_Number
            ) + ifNull (offset_table.max_cuid, 0)
        ),
        16,
        '0'
    ) AS CUID,
    formatDateTime (now (), '%Y-%m-%d %H:%i:%S') AS CUID_CREATION_DATE
FROM
    (
        SELECT DISTINCT
            PAN_Number
        FROM
            CUID.HL_CHANNEL_PARTNER
        WHERE
            PAN_Number IS NOT NULL
            AND trim(PAN_Number) != ''
            AND length (trim(PAN_Number)) = 10
            AND match (
                upper(trim(PAN_Number)),
                '^[A-Z]{5}[0-9]{4}[A-Z]$'
            )
            AND PAN_Number NOT IN (
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
    'CHANNEL PARTNER HL' AS TYPE,
    'HL_CHANNEL_PARTNER' AS SOURCE,
    p.Customer_Name AS NAME,
    p.PAN_Number AS PAN_NO,
    '' AS GST_NO,
    '' AS AADHAAR_NO,
    '' AS CKYCID,
    '' AS TIN_NO,
    '' AS VOTER_ID,
    '' AS DRIVING_LICN_NO,
    '' AS RATIONCARD_NO,
    '' AS PASSPORT,
    toString (p.Mobile_No) AS MOBILE_NO,
    '' AS EMAIL_ID,
    toString (p.APPLICATION_ID) AS APPLICATIONNO,
    '' AS COAPPLICANT_PAN_NO,
    '' AS GUARANTOR_PAN_NO,
    '' AS CIF_NO,
    p.Agreement_No AS AGREEMENTNO,
    '' AS CUST_UNQ_ID,
    '' AS CUSTID,
    '' AS VENDOR_ID,
    '' AS REGISTRATION_CERTIFICATE,
    '' AS NPA_STAGEID,
    '' AS LEAD_ID,
    '' AS LEAD_STATUS,
    '' AS ASSET_VALUE,
    '' AS DATE_OF_INCORPORATION,
    '' AS USER_EMPLOYEE_NO,
    p.DOB AS DATE_OF_BIRTH,
    '' AS DATE_OF_JOINING,
    toString (p.BRANCH_ID) AS ALLOCATION_REGION_CODE,
    p.Region AS ALLOCATION_REGION_TYPE,
    r.CUID,
    r.CUID_CREATION_DATE,
    'Loaded from HL_CHANNEL_PARTNER' AS REMARKS
FROM
    CUID.HL_CHANNEL_PARTNER p
    LEFT JOIN CUID.CUID_REPOSITORY r ON p.PAN_Number = r.PAN_NO
WHERE
    p.PAN_Number IS NOT NULL
    AND trim(p.PAN_Number) != '';

OPTIMIZE TABLE CUID.CUID_MASTER_TABLE FINAL;