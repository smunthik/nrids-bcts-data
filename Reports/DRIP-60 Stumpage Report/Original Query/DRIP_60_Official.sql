SELECT
    data.*,
    CASE
        WHEN TOTAL_UPSET_VALUE IS NULL THEN data.upset_rate + data.bonus_bid
    END AS Totsl_stumpage_M3,
    CASE
        WHEN TOTAL_UPSET_VALUE IS NOT NULL THEN data.TOTAL_UPSET_VALUE + data.BONUS_OFFER
    END AS Totsl_stumpage_Value
FROM
    (
        SELECT DISTINCT
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
            replace(
                decode (
                    ou.org_unit_name,
                    'Seaward Timber Sales Office',
                    'Seaward-Tlasta',
                    ou.org_unit_name
                ) | | ' (' | | ou.org_unit_code | | ')',
                ' Timber Sales Office',
                ''
            ) AS Business_Area,
            ou.org_unit_code AS Business_Area_Code,
            ts.forest_file_id,
            ts.BCTS_CATEGORY_CODE,
            ts.auction_date,
            tt.legal_effective_dt,
            ts.sale_volume,
            ts.UPSET_RATE,
            ts.TOTAL_UPSET_VALUE,
            bid_info.BONUS_BID,
            bid_info.bonus_offer,
            bid_info.client_count,
            pfu.file_status_st
        FROM
            bcts_timber_sale ts,
            BCTS_CATEGORY_CODE c,
            prov_forest_use pfu,
            org_unit ou,
            tenure_term tt,
            tsa_number_code ta,
            tfl_number_code tf,
            timber_mark tm,
            org_unit ou1,
            (
                SELECT
                    bd.forest_file_id,
                    bd.auction_date,
                    Max(
                        Decode (bd.SALE_AWARDED_IND, 'Y', bd.bonus_bid, 0)
                    ) AS bonus_bid,
                    Max(
                        Decode (bd.SALE_AWARDED_IND, 'Y', bd.bonus_offer, 0)
                    ) AS bonus_offer,
                    Count(DISTINCT bd.client_number) AS client_count
                FROM
                    the.bcts_timber_sale ts0,
                    the.bcts_tenure_bidder bd
                WHERE
                    ts0.forest_file_id = bd.forest_file_id
                    AND bd.INELIGIBLE_IND = 'N'
                GROUP BY
                    bd.forest_file_id,
                    bd.auction_date
            ) bid_info
        WHERE
            --                ts.forest_file_id = 'A95209'
            ts.auction_date BETWEEN To_Date ('2025-04-01', 'YYYY-MM-DD') -- Date: beginning of current fiscal
 AND To_Date  ('2025-10-31', 'YYYY-MM-DD') -- Date: end of reporting period
            AND ts.forest_file_id = bid_info.forest_file_id (+)
            AND ts.BCTS_CATEGORY_CODE = c.BCTS_CATEGORY_CODE (+)
            AND ts.auction_date = bid_info.auction_date (+)
            AND ts.forest_file_id = pfu.forest_file_id
            AND pfu.bcts_org_unit = ou.org_unit_no
            AND pfu.mgmt_unit_id = ta.tsa_number (+)
            AND pfu.mgmt_unit_id = tf.tfl_number (+)
            AND pfu.forest_file_id = tt.forest_file_id (+)
            AND ts.forest_file_id = tm.forest_file_id (+)
            AND tm.FOREST_DISTRICT = ou1.org_unit_no (+)
            AND ts.no_sale_rationale_code IS NULL
            AND pfu.file_status_st IN ('HI', 'HC', 'LC', 'HX', 'HS', 'HRS')
    ) data