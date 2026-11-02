SELECT
  count()
FROM
  (
    SELECT
      ifNull(any(c.full_name), '') AS NAME,
      ifNull(any(a.entity_pan), '') AS PAN_NO,
      ifNull(any(g.gstin), '') AS GST_NO,
      ifNull(any(a.aadhar_last_4_digits), '') AS AADHAR_NO,
      ifNull(any(c.voter_id_no), '') AS VOTER_ID,
      ifNull(any(c.driving_license_no), '') AS DRIVING_LICN_NO,
      ifNull(any(c.passport_no), '') AS PASSPORT,
      ifNull(any(c.mobile_no), '') AS MOBILE_NO,
      ifNull(any(c.personal_email_id), '') AS EMAIL_ID,
      ifNull(a.loan_application_no, '') AS APPLICATIONNO,
      ifNull(any(c.co_app_pan_no), '') AS COAPPLICANT_PAN_NO,
      ifNull(any(c.guarantor_pan_no), '') AS GUARANTOR_PAN_NO,
      ifNull(any(a.loan_id_lms), '') AS AGREEMENTNO,
      ifNull(any(u.uniqueid), '') AS CUST_UNQ_ID,
      ifNull(any(u.ucic), '') AS CUSTID,
      ifNull(any(p.group_id), '') AS NPA_STAGEID,
      ifNull(a.loan_application_no, '') AS LEAD_ID,
      ifNull(any(a.application_stage), '') AS LEAD_STATUS,
      ifNull(
        parseDateTime64BestEffortOrNull(any(a.modified_time)),
        toDateTime64('1970-01-01 00:00:00', 3)
      ) AS SOURCE_BASED_LAST_UPDATED,
      ifNull(
        toString(any(c.date_of_incorporation_inception)),
        ''
      ) AS DATE_OF_INCORPORATION,
      ifNull(toString(any(c.date_of_birth)), '') AS DATE_OF_BIRTH,
      ifNull(any(a.region), '') AS ALLOCATION_REGION_CODE,
      ifNull(any(a.zone), '') AS ALLOCATION_REGION_TYPE
    FROM
      CSEL.accounts AS a
      LEFT JOIN (
        SELECT
          parent_id,
          any(gstin) AS gstin
        FROM
          CSEL.gst_detail
        GROUP BY
          parent_id
      ) AS g ON a.id = toString(g.parent_id)
      LEFT JOIN (
        SELECT
          lan_no,
          any(full_name) AS full_name,
          any(voter_id_no) AS voter_id_no,
          any(driving_license_no) AS driving_license_no,
          any(passport_no) AS passport_no,
          any(mobile_no) AS mobile_no,
          any(personal_email_id) AS personal_email_id,
          any(co_app_pan_no) AS co_app_pan_no,
          any(guarantor_pan_no) AS guarantor_pan_no,
          any(date_of_incorporation_inception) AS date_of_incorporation_inception,
          any(date_of_birth) AS date_of_birth
        FROM
          CSEL.contacts
        WHERE
          applicants_type = 'Applicant'
        GROUP BY
          lan_no
      ) AS c ON a.loan_application_no = c.lan_no
      LEFT JOIN (
        SELECT
          name,
          any(uniqueid) AS uniqueid,
          any(ucic) AS ucic
        FROM
          CSEL.coe_ucic
        WHERE
          type_of_applicant = 'Applicant'
        GROUP BY
          name
      ) AS u ON a.loan_application_no = u.name
      LEFT JOIN (
        SELECT
          name,
          any(id) AS id
        FROM
          CSEL.applicants_details
        WHERE
          type_of_applicant = 'Applicant'
        GROUP BY
          name
      ) AS apd ON apd.name = a.loan_application_no
      LEFT JOIN (
        SELECT
          parent_id,
          any(group_id) AS group_id
        FROM
          CSEL.posidex
        GROUP BY
          parent_id
      ) AS p ON apd.id = p.parent_id
    GROUP BY
      a.loan_application_no
  )