SELECT
    issued_active.*,
    hbs.cruise_based_ind,
    hbs.billed_volume,
    Nvl (issued_active.sale_volume, 0) - Nvl (hbs.Billed_Volume, 0) AS Unbilled_Volume,
    round(
        (
            Nvl (issued_active.sale_volume, 0) - Nvl (hbs.Billed_Volume, 0)
        ) / issued_active.sale_volume * 100,
        1
    ) Percent_Unbilled
FROM
    /*

    Licences issued in report period

    that are not closed, logging complete, cancelled, or surrendered.

    */
    (
        SELECT
            CASE
                WHEN ou.org_unit_code IN (
                    'TBA',
                    'TPL',
                    'TPG',
                    'TSK',
                    'TSN',
                    'TCC',
                    'TKA',
                    'TKO',
                    'TOC'
                ) THEN 'Interior'
                WHEN ou.org_unit_code IN ('TCH', 'TST', 'TSG') THEN 'Coast'
            END AS Business_Area_Region_Category,
            CASE
                WHEN ou.org_unit_code IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN') THEN 'North Interior'
                WHEN ou.org_unit_code IN ('TCC', 'TKA', 'TKO', 'TOC') THEN 'South Interior'
                WHEN ou.org_unit_code IN ('TCH', 'TST', 'TSG') THEN 'Coast'
            END AS Business_Area_Region,
            decode (
                ou.org_unit_code,
                NULL,
                NULL,
                replace(
                    decode (
                        ou.org_unit_name,
                        'Seaward Timber Sales Office',
                        'Seaward-Tlasta',
                        ou.org_unit_name
                    ) | | ' (' | | ou.org_unit_code | | ')',
                    ' Timber Sales Office',
                    ''
                )
            ) AS Business_Area,
            ts.forest_file_id,
            pfu.file_type_code,
            ts.bcts_category_code,
            tt.legal_effective_dt AS legal_effective_date_fta,
            /* report need, flags added   2025-3-12 BD*/
            SYSDATE AS report_date,
            CASE
                WHEN add_months (tt.legal_effective_dt, 20) < SYSDATE THEN 1
                ELSE 0
            END AS effective_date_add_20_flag,
            CASE
                WHEN add_months (
                    decode (
                        tt.current_expiry_dt,
                        NULL,
                        tt.initial_expiry_dt,
                        tt.current_expiry_dt
                    ),
                    -6
                ) < SYSDATE THEN 1
                ELSE 0
            END AS expire_date_minus_6_flag,
            /* report need, 3 dates added     */
            decode (
                tt.legal_effective_dt,
                NULL,
                NULL,
                'Fiscal ' | | to_number (
                    extract (
                        year
                        FROM
                            (add_months (tt.legal_effective_dt, 9))
                    ) - 1
                ) | | '/' | | extract (
                    year
                    FROM
                        (add_months (tt.legal_effective_dt, 9))
                )
            ) AS Legal_Effective_Fiscal_FTA,
            tt.initial_expiry_dt AS Initial_Expiry_FTA,
            tt.current_expiry_dt AS Current_Expiry_FTA,
            decode (
                tt.current_expiry_dt,
                NULL,
                tt.initial_expiry_dt,
                tt.current_expiry_dt
            ) AS Expiry_FTA,
            tt.tenure_term AS Advertised_Licence_Term,
            round(
                months_between (current_expiry_dt, initial_expiry_dt)
            ) AS Extension_Term,
            round(
                months_between (
                    decode (
                        tt.current_expiry_dt,
                        NULL,
                        tt.initial_expiry_dt,
                        tt.current_expiry_dt
                    ),
                    tt.legal_effective_dt
                )
            ) Total_Tenure_Term,
            bid_info.client_number,
            bid_info.client,
            decode (
                tfsc.description,
                NULL,
                pfu.file_status_st,
                tfsc.description | | ' (' | | pfu.file_status_st | | ')'
            ) AS fta_file_status,
            pfu.file_status_date AS fta_file_status_date,
            ts.sale_volume
        FROM
            the.bcts_timber_sale ts,
            the.prov_forest_use pfu,
            the.tenure_term tt,
            the.tenure_file_status_code tfsc,
            the.org_unit ou,
            /*

            Some historic auction data has multiple sale_awarded_ind = 'Y'

            for the same forest_file_id. This subquery looks at the winning

            bid info for the most recent auction for each successful auction.

            */
            (
                SELECT DISTINCT
                    auction_with_winner.forest_file_id,
                    auction_with_winner.auction_date,
                    client_number,
                    client
                FROM
                    (
                        SELECT
                            tb.forest_file_id,
                            tb.auction_date,
                            tb.client_number,
                            (
                                decode (
                                    fc.legal_first_name,
                                    NULL,
                                    NULL,
                                    fc.legal_first_name | | ' '
                                ) | | decode (
                                    fc.legal_middle_name,
                                    NULL,
                                    NULL,
                                    fc.legal_middle_name | | ' '
                                ) | | fc.client_name
                            ) AS client
                        FROM
                            the.bcts_tenure_bidder tb,
                            the.forest_client fc
                        WHERE
                            upper(tb.sale_awarded_ind) = 'Y' -- Only look at the winning bid
                            AND tb.client_number = fc.client_number
                    ) auction_with_winner,
                    (
                        SELECT
                            forest_file_id,
                            max(auction_date) AS latest_auction_date
                        FROM
                            the.bcts_tenure_bidder tb
                        WHERE
                            upper(tb.sale_awarded_ind) = 'Y'
                        GROUP BY
                            forest_file_id
                    ) latest_auction_with_winner
                WHERE
                    auction_with_winner.forest_file_id = latest_auction_with_winner.forest_file_id
                    AND auction_with_winner.auction_date = latest_auction_with_winner.latest_auction_date
            ) bid_info
        WHERE
            pfu.forest_file_id = ts.forest_file_id
            AND pfu.forest_file_id = tt.forest_file_id
            AND ts.forest_file_id = bid_info.forest_file_id
            AND ts.auction_date = bid_info.auction_date
            AND pfu.bcts_org_unit = ou.org_unit_no
            AND pfu.file_status_st = tfsc.tenure_file_status_code (+)
            AND ts.no_sale_rationale_code IS NULL -- This statement should be redundant and is included as a failsafe.
            AND pfu.file_type_code = 'B20'
            /* For this Licence Issued and Unharvested timber report, exclude the

            FTA statuses that can indicate a licence was once issued,

            and has since been closed (HC), logging complete (LC),

            cancelled (HX), surrendered (HRS).

            This report is concerned with currently active licences

            with unbilled volume. */
            AND pfu.file_status_st IN (
                'HI', -- Issued
                'HS' -- Suspended
            )
            /* Tenure term legal effective date has begun */
            AND tt.legal_effective_dt < sysdate
        ORDER BY
            business_area_region_category DESC,
            business_area_region,
            business_area,
            legal_effective_fiscal_FTA DESC,
            forest_file_id
    ) issued_active,
    /* Harvest Billing System (HBS) */
    (
        SELECT
            P.BCTS_ORG_UNIT,
            P.FOREST_FILE_ID,
            M.TIMBER_MARK,
            M.CRUISE_BASED_IND,
            S.SALE_VOLUME,
            Sum(H.VOLUME_SCALED) AS Billed_Volume,
            Sum(
                DECODE (H.BILLING_TYPE_CODE, 'WU', H.VOLUME_SCALED, NULL)
            ) AS Billed_WU_Volume,
            Sum(
                DECODE (H.BILLING_TYPE_CODE, 'WA', H.VOLUME_SCALED, NULL)
            ) AS Billed_WA_Volume,
            SUM(H.TOTAL_AMOUNT) AS BILLED_AMOUNT,
            SUM(H.ROYALTY_AMOUNT) AS ROYALTY_AMOUNT,
            SUM(H.RESERVE_STMPG_AMT) AS RESERVE_STUMPAGE_AMOUNT,
            SUM(H.BONUS_STUMPAGE_AMT) AS BONUS_STUMPAGE_AMOUNT,
            SUM(H.DEV_LEVY_AMOUNT) AS DEV_LEVY_AMOUNT
        FROM
            THE.HARVEST_SALE S,
            THE.PROV_FOREST_USE P,
            THE.TIMBER_MARK M,
            THE.SCALING_HISTORY H
        WHERE
            S.FOREST_FILE_ID = P.FOREST_FILE_ID
            AND P.FOREST_FILE_ID = M.FOREST_FILE_ID
            AND M.TIMBER_MARK = H.TIMBER_MARK (+)
            AND P.BCTS_ORG_UNIT IS NOT NULL
        GROUP BY
            P.BCTS_ORG_UNIT,
            P.FOREST_FILE_ID,
            M.TIMBER_MARK,
            M.CRUISE_BASED_IND,
            S.SALE_VOLUME
    ) hbs
WHERE
    issued_active.forest_file_id = hbs.forest_file_id (+)
    AND issued_active.Expiry_FTA > sysdate
    AND Nvl (issued_active.sale_volume, 0) - Nvl (hbs.Billed_Volume, 0) > 100 -- Look at licences where more than 100 cubic metres is still unbilled
;