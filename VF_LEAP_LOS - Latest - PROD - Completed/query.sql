SELECT
  CASE
    WHEN pd.isIndividual = 1 THEN concat(
      ifNull(pd.firstName, ''),
      ' ',
      ifNull(pd.middleName, ''),
      ' ',
      ifNull(pd.lastName, '')
    )
    ELSE ifNull(pd.companyName, '')
  END AS NAME,
  MAX(
    CASE
      WHEN doc.subcategory = 'PAN CARD' THEN trimBoth(doc.kycProofValue)
    END
  ) AS PAN_NO,
  pd.partyType AS PARTY_TYPE,
  MAX(
    CASE
      WHEN doc.subcategory = 'GST' THEN trimBoth(doc.kycProofValue)
    END
  ) AS GST_NO,
  MAX(
    CASE
      WHEN doc.subcategory = 'AADHAR CARD(U-ID)' THEN trimBoth(doc.kycProofValue)
    END
  ) AS AADHAAR_NO,
  MAX(
    CASE
      WHEN doc.subcategory = 'VOTER ID' THEN trimBoth(doc.kycProofValue)
    END
  ) AS VOTER_ID,
  MAX(
    CASE
      WHEN doc.subcategory = 'DL' THEN trimBoth(doc.kycProofValue)
    END
  ) AS DRIVING_LICN_NO,
  MAX(
    CASE
      WHEN doc.subcategory = 'PASSPORT' THEN trimBoth(doc.kycProofValue)
    END
  ) AS PASSPORT,
  pd.mobileNos AS MOBILE_NO,
  ld.vehicleDetail_agreementNo AS AGREEMENTNO,
  doc.leadID AS LEAD_ID,
  ls.currentWorkStatus AS LEAD_STATUS,
  ld.createdDate AS CREATED_DATE,
  ld.modifiedDate AS SOURCE_BASED_LAST_UPDATED,
  pd.dateOfIncorporation AS DATE_OF_INCORPORATION,
  pd.customerID AS CUSTID,
  encmm.CIF_NO AS CIF_NO,
  pd.dateOfBirth AS DATE_OF_BIRTH
FROM
  VF_LOS.SalesDB_Lead_Inflight_documents doc
  LEFT JOIN VF_LOS.SalesDB_Lead_Inflight_partyDetails pd ON doc.partyID = pd.partyID
  LEFT JOIN VF_LOS.SalesDB_Lead_Inflight ls ON ls.leadID = pd.leadID
  INNER JOIN VF_LOS.SalesDB_Lead_Inflight_LoanDetails ld ON doc.leadID = ld.leadID
  INNER JOIN Operations_EDLS.EDLS_NBFC_CUSTOMER_M_MAIN encmm ON toString(pd.customerID) = toString(encmm.CUSTOMERID)
GROUP BY
  doc.leadID,
  pd.partyType,
  pd.mobileNos,
  pd.dateOfBirth,
  pd.dateOfIncorporation,
  ld.vehicleDetail_agreementNo,
  ld.modifiedDate,
  ld.createdDate,
  pd.isIndividual,
  pd.firstName,
  pd.middleName,
  pd.lastName,
  pd.companyName,
  ls.currentWorkStatus,
  pd.customerID,
  encmm.CIF_NO