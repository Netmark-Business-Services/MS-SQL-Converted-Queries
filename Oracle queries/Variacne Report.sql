-- Variance Report 1
WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (
            PARTITION BY cf.CLAIM_HCC_ID
            ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC
        ) AS row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N'
        AND cf.IS_TRIAL_CLAIM = 'N'
        AND cf.IS_CURRENT = 'Y'
)
 
SELECT   
    rc.CLAIM_HCC_ID,
    rc.CLAIM_STATUS,
    EF.flag_action,
    EF.flag_message,
    EF.flag_mnemonic,
    dd.DATE_VALUE AS RECEIPT_DATE,
    rc.MOST_RECENT_PROCESS_TIME
FROM
    RecentClaims rc
LEFT JOIN
    payor_dw.CLAIM_SOURCE_CODE csc
    ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
LEFT JOIN
    payor_dw.EXT_EDITOR_CLAIM_RES_TO_FLAGS eecrtf
    ON rc.EXT_EDITOR_CLAIM_RESULT_KEY = eecrtf.EXT_EDITOR_CLAIM_RESULT_KEY
LEFT JOIN
    payor_dw.EDIT_FLAG EF
    ON eecrtf.EXT_EDITOR_EDIT_FLAG_KEY = EF.EXT_EDITOR_EDIT_FLAG_KEY
LEFT JOIN
    payor_dw.DATE_DIMENSION dd
    ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
WHERE
    rc.row_num = 1
   -- AND CLAIM_STATUS = 'Denied'
  --  AND EF.FLAG_MESSAGE IN ('[Pattern 5364] The type of bill code is invalid or missing.','[Pattern 19619] The admitting diagnosis code is missing.')
   AND EF.FLAG_MNEMONIC IN ('gcCCSD')
   
------------------------------------------------------------------------------------   
 -- Variance Report with Supplier ID
   WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS row_num
    FROM
        PAYOR_DW.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N'
        AND cf.IS_TRIAL_CLAIM = 'N'
        AND cf.IS_CURRENT = 'Y'
),
DateFilteredClaims AS (
    SELECT
        rc.CLAIM_FACT_KEY,
        rc.CLAIM_HCC_ID,
        rc.CLAIM_STATUS,
        rc.SUPPLIER_KEY,
        ssd.DATE_VALUE AS SERVICE_START_DATE,
        clf.REVENUE_CODE,
        clf.SERVICE_CODE,
        vcd.VALUE_CODE,
        vcd.VALUE_CODE_AMOUNT
    FROM
        RecentClaims rc
    JOIN PAYOR_DW.CLAIM_LINE_FACT clf ON rc.CLAIM_FACT_KEY = clf.CLAIM_FACT_KEY
    LEFT JOIN PAYOR_DW.DATE_DIMENSION ssd ON clf.SERVICE_START_DATE_KEY = ssd.DATE_KEY
    LEFT JOIN PAYOR_DW.PAYMENT_REQUEST_VALUE_CODE vcd ON rc.CLAIM_FACT_KEY = vcd.CLAIM_FACT_KEY
    WHERE
        rc.row_num = 1
        AND ssd.DATE_VALUE >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
),
FilteredClaims AS (
    SELECT DISTINCT *
    FROM DateFilteredClaims
   -- WHERE
     --   REVENUE_CODE = '0762'
),
AggregatedClaimLines AS (
   SELECT
       CLAIM_FACT_KEY,
       SUM(BILLED_AMOUNT) AS TOTAL_BILLED_AMOUNT,
       SUM(PAID_AMOUNT) AS TOTAL_PAID_AMOUNT
   FROM
       PAYOR_DW.ALL_CLAIM_LINE_FACT
   GROUP BY
       CLAIM_FACT_KEY
)



SELECT
    fc.CLAIM_HCC_ID,
    s.SUPPLIER_HCC_ID,
    fc.CLAIM_STATUS,
    fc.REVENUE_CODE,
    fc.SERVICE_CODE,
    TO_CHAR(fc.SERVICE_START_DATE, 'YYYY-MM-DD') AS SERVICE_START_DATE,
    fc.VALUE_CODE,
    fc.VALUE_CODE_AMOUNT,
    aclf_agg.TOTAL_BILLED_AMOUNT,
    aclf_agg.TOTAL_PAID_AMOUNT
FROM
    FilteredClaims fc
JOIN PAYOR_DW.SUPPLIER s ON fc.SUPPLIER_KEY = s.SUPPLIER_KEY
LEFT JOIN
   AggregatedClaimLines aclf_agg ON fc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
WHERE
    s.SUPPLIER_HCC_ID = '1013459'
    
    
---------------------------------------------------------------------------------------------------

 -- Variance Report with Message Code included
 WITH RecentClaims AS (
   SELECT
      cf.CLAIM_FACT_KEY,
      cf.CLAIM_HCC_ID,
      cf.SI_SUPPLIER_ID,
      cf.SI_SUPPLIER_NAME,
      cf.SUPPLIER_KEY,
      cf.CLAIM_STATUS,
      ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS row_num
   FROM
      PAYOR_DW.CLAIM_FACT cf
	WHERE
        cf.IS_CONVERTED = 'N'
        AND cf.IS_TRIAL_CLAIM = 'N'
        AND cf.IS_CURRENT ='Y'
        ),
AggregatedClaimLines AS (
   SELECT
       CLAIM_FACT_KEY,
       SUM(BILLED_AMOUNT) AS TOTAL_BILLED_AMOUNT,
       SUM(PAID_AMOUNT) AS TOTAL_PAID_AMOUNT
   FROM
       PAYOR_DW.ALL_CLAIM_LINE_FACT
   GROUP BY
       CLAIM_FACT_KEY
)   
SELECT  
   rc.CLAIM_HCC_ID,
   clf.CLAIM_LINE_HCC_ID,
   ashf.SUPPLIER_HCC_ID,
   ashf.SUPPLIER_NAME,
   rc.CLAIM_STATUS,
   CLF.SERVICE_CODE,
   am.ADJUDICATION_MESSAGE_CODE AS DenialCodes,
   am.ADJUDICATION_MESSAGE_DESC AS DenialReasons,
   aclf_agg.TOTAL_BILLED_AMOUNT,
   aclf_agg.TOTAL_PAID_AMOUNT
FROM
   RecentClaims rc
LEFT JOIN
   AggregatedClaimLines aclf_agg ON rc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
LEFT JOIN
   PAYOR_DW.SUPPLIER ASHF ON rc.SUPPLIER_KEY = ashf.SUPPLIER_KEY
LEFT JOIN
	PAYOR_DW.claim_line_fact clf ON rc.CLAIM_FACT_KEY = clf.CLAIM_FACT_KEY
LEFT JOIN
   PAYOR_DW.CLAIM_FACT_TO_SERVICE CFS ON CLF.CLAIM_FACT_KEY = CFS.CLAIM_FACT_KEY
LEFT JOIN
   PAYOR_DW.CLAIM_LINE_FACT_TO_MODIFIER clftm ON clf.CLAIM_LINE_FACT_KEY = CLFTM.CLAIM_LINE_FACT_KEY
LEFT JOIN   
   PAYOR_DW.CLAIM_LINE_FACT_TO_ADJD_MSG clfam ON clf.CLAIM_LINE_FACT_KEY = clfam.CLAIM_LINE_FACT_KEY
LEFT JOIN   
   PAYOR_DW.ADJUDICATION_MESSAGE am ON clfam.ADJUDICATION_MESSAGE_KEY = am.ADJUDICATION_MESSAGE_KEY
WHERE
   rc.row_num = 1
   AND am.ADJUDICATION_MESSAGE_CODE = '147'
   AND ashf.SUPPLIER_HCC_ID = '1013459'   
-------------------------------------------------------------------------------------------------------
   -- Variance Report with value code amount & Revenue code
   
   WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS row_num
    FROM
        PAYOR_DW.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N' AND
        cf.IS_TRIAL_CLAIM = 'N' AND
        cf.IS_CURRENT = 'Y'
),
ClaimLineRollup AS (
SELECT
    clf.CLAIM_FACT_KEY,
    TO_CHAR(ssd.DATE_VALUE, 'YYYY-MM-DD') AS SERVICE_START_DATES,
    LISTAGG(DISTINCT clf.REVENUE_CODE, ', ') WITHIN GROUP (ORDER BY clf.REVENUE_CODE) AS REVENUE_CODES,
    LISTAGG(DISTINCT prvc.VALUE_CODE, ',') WITHIN GROUP (ORDER BY prvc.VALUE_CODE) AS VALUE_CODE,
    LISTAGG(DISTINCT prvc.VALUE_CODE_AMOUNT, ',') WITHIN GROUP (ORDER BY prvc.VALUE_CODE_AMOUNT) AS VALUE_CODE_AMOUNT
FROM
    PAYOR_DW.CLAIM_LINE_FACT clf
LEFT JOIN
    PAYOR_DW.DATE_DIMENSION ssd ON clf.SERVICE_START_DATE_KEY = ssd.DATE_KEY
LEFT JOIN
    PAYOR_DW.PAYMENT_REQUEST_VALUE_CODE prvc ON clf.CLAIM_FACT_KEY = prvc.CLAIM_FACT_KEY
GROUP BY
    clf.CLAIM_FACT_KEY, ssd.DATE_VALUE
)
SELECT
      rc.claim_hcc_id,
      m.MEMBER_HCC_ID,
      m.MEMBER_FULL_NAME,
      s.SUPPLIER_HCC_ID,
      cla.REVENUE_CODES,
      cla.SERVICE_START_DATES AS Service_Start_Dates,
      clf2.BILLED_AMOUNT,
      clf2.PAID_AMOUNT,
      dd.DATE_VALUE AS Receipt_Date,
      cla.VALUE_CODE,
      cla.VALUE_CODE_AMOUNT
FROM
       RecentClaims rc
LEFT JOIN
    ClaimLineRollup cla ON rc.CLAIM_FACT_KEY = cla.CLAIM_FACT_KEY
LEFT JOIN MEMBER M ON rc.MEMBER_KEY = M.MEMBER_KEY
LEFT JOIN CLAIM_LINE_FACT clf2 ON rc.CLAIM_FACT_KEY = clf2.CLAIM_FACT_KEY
JOIN
    PAYOR_DW.SUPPLIER S ON rc.supplier_key = S.supplier_key
LEFT JOIN
    PAYOR_DW.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
WHERE
	rc.row_num =1
	AND dd.DATE_VALUE BETWEEN TO_DATE('2024-07-01', 'YYYY-MM-DD') AND TO_DATE('2024-12-31', 'YYYY-MM-DD')
AND (
		--(cla.REVENUE_CODES IN ('0001','0002','0003','0011','0012')) OR
        --(cla.REVENUE_CODES IN ('%75%', '%76%) AND cla.VALUE_CODE = '24' AND cla.VALUE_CODE_AMOUNT = '.07') OR
        (cla.REVENUE_CODES LIKE '%0190%' AND cla.VALUE_CODE = '24'  ) OR
        (cla.REVENUE_CODES LIKE '%0185%' AND cla.VALUE_CODE = '24' ) OR
        (cla.REVENUE_CODES LIKE '%0180%' AND cla.VALUE_CODE = '24')
    )
 