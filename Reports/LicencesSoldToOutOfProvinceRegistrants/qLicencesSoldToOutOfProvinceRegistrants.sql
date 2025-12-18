SELECT DISTINCT
    licence_info.Management_Unit,
    licence_info.Forest_File_Id,
    licence_info.BCTS_Category_Code,
    licence_info.legal_effective_dt AS Legal_Effective_Date,
    licence_info.auction_date,
    licence_info.sale_volume,
    br.client_number,
    CASE
        WHEN fc.legal_first_name IS NOT NULL
        AND fc.legal_middle_name IS NOT NULL THEN fc.legal_first_name | | ' ' | | fc.legal_middle_name | | ' ' | | FC.CLIENT_NAME
        WHEN fc.legal_first_name IS NOT NULL THEN fc.legal_first_name | | ' ' | | FC.CLIENT_NAME
        ELSE FC.CLIENT_NAME
    END AS Licensee_Name,
    (
        CL.ADDRESS_1 | | Decode (CL.ADDRESS_2, NULL, NULL, ' ' | | CL.ADDRESS_2) | | Decode (CL.ADDRESS_3, NULL, NULL, ' ' | | CL.ADDRESS_3)
    ) AS Licensee_Address,
    CL.POSTAL_CODE,
    CL.City,
    CL.PROVINCE,
    CL.COUNTRY,
    br.CLIENT_LOCN_CODE,
    FFC.FOREST_FILE_CLIENT_TYPE_CODE,
    FC.REGISTRY_COMPANY_TYPE_CODE,
    br.registrant_expiry_date,
    FC.CLIENT_COMMENT,
    licence_info.org_unit_code,
    licence_info.MGMT_UNIT_TYPE,
    licence_info.mgmt_unit_id,
    licence_info.Category,
    licence_info.file_status_st,
    licence_info.Fiscal_Issued,
    licence_info.UPSET_RATE,
    licence_info.TOTAL_UPSET_VALUE,
    licence_info.BONUS_BID,
    licence_info.bonus_offer
FROM
    THE.bcts_registrant br,
    THE.CLIENT_LOCATION cl,
    the.forest_file_client ffc,
    the.forest_client fc,
    (
        SELECT DISTINCT
            ou.org_unit_code,
            pfu.MGMT_UNIT_TYPE,
            pfu.mgmt_unit_id,
            decode (
                pfu.mgmt_unit_type,
                'U',
                ta.DESCRIPTION,
                tf.DESCRIPTION
            ) AS Management_Unit,
            ts.forest_file_id,
            ts.BCTS_CATEGORY_CODE,
            c.DESCRIPTION AS Category,
            pfu.file_status_st,
            tt.legal_effective_dt,
            Extract (
                Year
                FROM
                    Add_Months (tt.legal_effective_dt, 9)
            ) AS Fiscal_Issued,
            ts.auction_date,
            ts.sale_volume,
            ts.UPSET_RATE,
            ts.TOTAL_UPSET_VALUE,
            btb.BONUS_BID,
            btb.bonus_offer,
            FFC.CLIENT_NUMBER,
            ffc.client_locn_code
        FROM
            THE.bcts_timber_sale ts,
            the.bcts_tenure_bidder btb,
            THE.BCTS_CATEGORY_CODE c,
            THE.prov_forest_use pfu,
            THE.org_unit ou,
            THE.tenure_term tt,
            THE.tsa_number_code ta,
            THE.tfl_number_code tf,
            THE.FOREST_FILE_CLIENT FFC,
            THE.CLIENT_LOCATION CL,
            THE.FOREST_CLIENT FC
        WHERE
            ts.BCTS_CATEGORY_CODE = c.BCTS_CATEGORY_CODE (+)
            AND ts.forest_file_id = btb.forest_file_id (+)
            AND ts.auction_date = btb.auction_date (+)
            AND btb.SALE_AWARDED_IND = 'Y'
            AND btb.client_number = fc.client_number (+)
            AND ts.forest_file_id = FFC.forest_file_id
            AND FFC.CLIENT_NUMBER = CL.CLIENT_NUMBER
            AND FFC.CLIENT_LOCN_CODE = CL.CLIENT_LOCN_CODE
            AND CL.CLIENT_NUMBER = FC.CLIENT_NUMBER
            AND ts.forest_file_id = pfu.forest_file_id
            AND pfu.bcts_org_unit = ou.org_unit_no
            AND pfu.mgmt_unit_id = ta.tsa_number (+)
            AND pfu.mgmt_unit_id = tf.tfl_number (+)
            AND pfu.forest_file_id = tt.forest_file_id (+)
            AND pfu.file_status_st IN ('HI', 'HC', 'LC', 'HX', 'HS', 'HRS')
            AND ts.no_sale_rationale_code IS NULL
            AND tt.legal_effective_dt BETWEEN To_Date ('2023-04-01', 'YYYY-MM-DD') AND To_Date  ('2024-03-31', 'YYYY-MM-DD')
        ORDER BY
            tt.legal_effective_dt DESC
    ) licence_info
WHERE
    br.client_number = cl.client_number
    AND br.CLIENT_LOCN_CODE = cl.CLIENT_LOCN_CODE
    AND br.client_number = ffc.client_number
    AND br.client_locn_code = ffc.client_locn_code
    AND br.client_number = fc.client_number
    AND br.client_number = licence_info.client_number
    AND br.client_locn_code = licence_info.client_locn_code
    /* Registrant criteria */
    AND ffc.forest_file_client_type_code = 'A' -- Licensee client type
    AND CL.PROVINCE <> 'BC' -- Registrants outside BC
ORDER BY
    licence_info.legal_effective_dt DESC,
    licensee_name,
    licence_info.auction_date DESC,
    licence_info.forest_file_id;