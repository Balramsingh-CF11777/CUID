-----------
INSERT INTO
    CUID.poswaiver_productlines_mysql (
        CUSTOMERNAME,
        pan_no,
        RESIMOBILE,
        email,
        COAPPLICANT_PAN_NO,
        GUARANTOR_PAN_NO,
        cif_no,
        AGREEMENTNO,
        CUSTOMERID,
        NPA_STAGEID,
        dob,
        EMP_ID,
        checksum
    )
SELECT
    CUSTOMERNAME,
    pan_no,
    RESIMOBILE,
    email,
    COAPPLICANT_PAN_NO,
    GUARANTOR_PAN_NO,
    cif_no,
    AGREEMENTNO,
    CUSTOMERID,
    NPA_STAGEID,
    dob,
    EMP_ID,
    checksum
FROM
    CUID.poswaiver_productlines_mysql_temp;

-- Incremental 
INSERT INTO
    CUID.poswaiver_productlines_mysql (
        CUSTOMERNAME,
        pan_no,
        RESIMOBILE,
        email,
        COAPPLICANT_PAN_NO,
        GUARANTOR_PAN_NO,
        cif_no,
        AGREEMENTNO,
        CUSTOMERID,
        NPA_STAGEID,
        dob,
        EMP_ID,
        checksum
    )
SELECT
    t.CUSTOMERNAME,
    t.pan_no,
    t.RESIMOBILE,
    t.email,
    t.COAPPLICANT_PAN_NO,
    t.GUARANTOR_PAN_NO,
    t.cif_no,
    t.AGREEMENTNO,
    t.CUSTOMERID,
    t.NPA_STAGEID,
    t.dob,
    t.EMP_ID,
    t.checksum
FROM
    CUID.poswaiver_productlines_mysql_temp t
    LEFT JOIN CUID.poswaiver_productlines_mysql m ON t.checksum = m.checksum
WHERE
    m.checksum IS NULL;

----
OPTIMIZE TABLE CUID.poswaiver_productlines_mysql FINAL;

---
INSERT INTO
    CUID.CUID_REPOSITORY
SELECT
    pan_no AS PAN_NO,
    lpad (
        toString (
            toUInt64 (
                row_number() OVER (
                    ORDER BY
                        pan_no
                )
            ) + ifNull (max_cuid, 0)
        ),
        16,
        '0'
    ) AS CUID,
    formatDateTime (now (), '%Y-%m-%d %H:%i:%S') AS CUID_CREATION_DATE
FROM
    (
        SELECT DISTINCT
            pan_no
        FROM
            CUID.poswaiver_productlines_mysql AS p
        WHERE
            pan_no IS NOT NULL
            AND length (pan_no) = 10
            AND match (pan_no, '^[A-Z]{5}[0-9]{4}[A-Z]$')
            AND pan_no NOT IN (
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

--- Insert to the Master table
SELECT DISTINCT
    SOURCE
FROM
    CUID.CUID_MASTER_TABLE
WHERE
    `SOURCE` = 'POSWAIVER' -- 14,849,250
    ---
ALTER TABLE CUID.CUID_MASTER_TABLE DELETE
WHERE
    SOURCE = 'POSWAIVER';

----
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
    'CUSTOMER' AS TYPE,
    'POSWAIVER' AS SOURCE,
    p.CUSTOMERNAME AS NAME,
    p.pan_no AS PAN_NO,
    '' AS GST_NO,
    '' AS AADHAAR_NO,
    '' AS CKYCID,
    '' AS TIN_NO,
    '' AS VOTER_ID,
    '' AS DRIVING_LICN_NO,
    '' AS RATIONCARD_NO,
    '' AS PASSPORT,
    p.RESIMOBILE AS MOBILE_NO,
    p.email AS EMAIL_ID,
    '' AS APPLICATIONNO,
    p.COAPPLICANT_PAN_NO AS COAPPLICANT_PAN_NO,
    p.GUARANTOR_PAN_NO AS GUARANTOR_PAN_NO,
    p.cif_no AS CIF_NO,
    p.AGREEMENTNO AS AGREEMENTNO,
    '' AS CUST_UNQ_ID,
    p.CUSTOMERID AS CUSTID,
    '' AS VENDOR_ID,
    '' AS REGISTRATION_CERTIFICATE,
    p.NPA_STAGEID AS NPA_STAGEID,
    '' AS LEAD_ID,
    '' AS LEAD_STATUS,
    '' AS ASSET_VALUE,
    '' AS DATE_OF_INCORPORATION,
    p.EMP_ID AS USER_EMPLOYEE_NO,
    p.dob AS DATE_OF_BIRTH,
    '' AS DATE_OF_JOINING,
    '' AS ALLOCATION_REGION_CODE,
    '' AS ALLOCATION_REGION_TYPE,
    r.CUID,
    r.CUID_CREATION_DATE,
    '' AS REMARKS
FROM
    CUID.poswaiver_productlines_mysql p
    INNER JOIN CUID.CUID_REPOSITORY r ON p.pan_no = r.PAN_NO
WHERE
    p.pan_no IS NOT NULL
    AND trim(p.pan_no) != '';