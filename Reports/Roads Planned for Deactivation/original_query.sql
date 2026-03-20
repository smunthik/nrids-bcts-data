SELECT
    CASE
        WHEN TSO_CODE IN (
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
        WHEN TSO_CODE IN ('TCH', 'TST', 'TSG') THEN 'Coast'
    END AS BUSINESS_AREA_REGION_CATEGORY,
    CASE
        WHEN TSO_CODE IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN') THEN 'North Interior'
        WHEN TSO_CODE IN ('TCC', 'TKA', 'TKO', 'TOC') THEN 'South Interior'
        WHEN TSO_CODE IN ('TCH', 'TST', 'TSG') THEN 'Coast'
    END AS BUSINESS_AREA_REGION,
    decode (
        DIVI_DIVISION_NAME,
        'Seaward',
        'Seaward-Tlasta',
        DIVI_DIVISION_NAME
    ) | | ' (' | | TSO_CODE | | ')' AS BUSINESS_AREA,
    TSO_CODE AS BUSINESS_AREA_CODE,
    road_seq_nbr,
    R.DEAC_SEQ_NBR,
    uri,
    road_road_name,
    FIELD_TEAM_DESC,
    POC,
    POT,
    total_Length,
    CASE
        WHEN Nvl (deac_budgeted_cost, 0) >= Nvl (deac_budgeted_item_cost, 0) THEN deac_budgeted_cost
        ELSE deac_budgeted_item_cost
    END AS Effective_Planned_Cost,
    deac_budgeted_cost,
    deac_budgeted_item_cost,
    rcls_accounting_type,
    rdst_steward_name,
    deac_planned_date,
    deac_end_date,
    deac_method_type,
    deac_level_type,
    Extract (
        Year
        FROM
            Add_Months (deac_planned_date, 9)
    ) AS Fiscal_Year,
    CASE
        WHEN Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) < Extract (
            Year
            FROM
                Add_Months (
                    To_Date ('2026-02-28', 'YYYY-MM-DD'), -- Date: end of reporting period
                    9
                )
        ) THEN 'Past Fiscals (Pre-' | | Extract (
            Year
            FROM
                Add_Months (
                    To_Date ('2026-02-28', 'YYYY-MM-DD'), -- Date: end of reporting period
                    9
                )
        ) | | ')'
        WHEN Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) = Extract (
            Year
            FROM
                Add_Months (
                    To_Date ('2026-02-28', 'YYYY-MM-DD'), -- Date: end of reporting period
                    9
                )
        ) THEN 'Current Fiscal (' | | Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) | | ')'
        WHEN Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) - Extract (
            Year
            FROM
                Add_Months (
                    To_Date ('2026-02-28', 'YYYY-MM-DD'), -- Date: end of reporting period
                    9
                )
        ) = 1 THEN '1st Fiscal (' | | Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) | | ')'
        WHEN Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) - Extract (
            Year
            FROM
                Add_Months (
                    To_Date ('2026-02-28', 'YYYY-MM-DD'), -- Date: end of reporting period
                    9
                )
        ) = 2 THEN '2nd Fiscal (' | | Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) | | ')'
        WHEN Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) - Extract (
            Year
            FROM
                Add_Months (
                    To_Date ('2026-02-28', 'YYYY-MM-DD'), -- Date: end of reporting period
                    9
                )
        ) = 3 THEN '3rd Fiscal (' | | Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) | | ')'
        WHEN Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) - Extract (
            Year
            FROM
                Add_Months (
                    To_Date ('2026-02-28', 'YYYY-MM-DD'), -- Date: end of reporting period
                    9
                )
        ) = 4 THEN '4th Fiscal (' | | Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) | | ')'
        WHEN Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) - Extract (
            Year
            FROM
                Add_Months (
                    To_Date ('2026-02-28', 'YYYY-MM-DD'), -- Date: end of reporting period
                    9
                )
        ) = 5 THEN '5th Fiscal (' | | Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) | | ')'
        WHEN Extract (
            Year
            FROM
                Add_Months (deac_planned_date, 9)
        ) - Extract (
            Year
            FROM
                Add_Months (
                    To_Date ('2026-02-28', 'YYYY-MM-DD'), -- Date: end of reporting period
                    9
                )
        ) > 5 THEN '6th Fiscal-Onwards (' | | (
            Extract (
                Year
                FROM
                    Add_Months (
                        To_Date ('2026-02-28', 'YYYY-MM-DD'), -- Date: end of reporting period
                        9
                    )
            ) + 6
        ) | | '+)'
    END AS Fiscal
FROM
    (
        SELECT
            divi_division_name,
            tso_code,
            road_seq_nbr,
            DEAC_SEQ_NBR,
            uri,
            road_road_name,
            FIELD_TEAM_DESC,
            Min(poc) AS POC,
            Max(pot) AS POT,
            (Max(pot) - Min(poc)) / 1000 AS total_Length,
            deac_budgeted_cost,
            rcls_accounting_type,
            rdst_steward_name,
            deac_planned_date,
            deac_end_date,
            deac_method_type,
            deac_level_type
        FROM
            (
                SELECT
                    divi_division_name,
                    tso_code,
                    road_seq_nbr,
                    DEAC_SEQ_NBR,
                    uri,
                    road_road_name,
                    FIELD_TEAM_DESC,
                    poc,
                    pot,
                    Lead(POC, 1) OVER (
                        PARTITION BY
                            DEAC_SEQ_NBR
                        ORDER BY
                            DEAC_SEQ_NBR,
                            POC
                    ) AS POC_Next,
                    Lag(POT, 1) OVER (
                        PARTITION BY
                            DEAC_SEQ_NBR
                        ORDER BY
                            DEAC_SEQ_NBR,
                            POC
                    ) AS POT_Prev,
                    deac_budgeted_cost,
                    rcls_accounting_type,
                    rdst_steward_name,
                    Trunc (deac_planned_date) AS deac_planned_date,
                    deac_end_date,
                    deac_method_type,
                    deac_level_type
                FROM
                    forestview.v_road_gap_analysis G
                WHERE
                    G.rdst_steward_name IN ('BCTS', 'former BCTS')
                    AND G.uri IS NOT NULL
                    AND G.deac_end_date IS NULL
                    AND G.rcls_accounting_type IN (
                        '1 Sale = 5 yrs',
                        'S.Term = 10 yrs',
                        'Perm = 40 yrs'
                    )
                    -- AND  UPPER(G.DEAC_LEVEL_TYPE) = 'PERMANENT'
                    AND G.deac_method_type IN ('DEACT', 'REHAB', 'TRANSFER OUT')
                ORDER BY
                    road_seq_nbr,
                    DEAC_SEQ_NBR,
                    poc
            ) GAP
        GROUP BY
            divi_division_name,
            tso_code,
            road_seq_nbr,
            DEAC_SEQ_NBR,
            uri,
            road_road_name,
            FIELD_TEAM_DESC,
            deac_budgeted_cost,
            rcls_accounting_type,
            rdst_steward_name,
            deac_planned_date,
            deac_end_date,
            deac_method_type,
            deac_level_type,
            CASE
                WHEN POC_Next IS NULL
                AND POT_PREV IS NULL THEN 'N'
                WHEN POT < POC_Next THEN 'Before'
                WHEN POC > POT_Prev THEN 'After'
                WHEN POC = POC_Next
                OR POC = POT_PREV
                OR POT = POC_NEXT
                OR POT = POT_PREV THEN 'Y'
                ELSE 'G'
            END
    ) R,
    (
        SELECT
            Sum(C.RACO_ITEM_COST) AS deac_budgeted_item_cost,
            C.DEAC_SEQ_NBR
        FROM
            FORESTVIEW.V_ROAD_ACTIVITY_COST C
        WHERE
            (
                (C.DEAC_SEQ_NBR IS NOT NULL)
                AND (UPPER(C.RACO_COST_TYPE) = 'BUDGETED_COST')
            )
        GROUP BY
            C.DEAC_SEQ_NBR
    ) DC
WHERE
    R.DEAC_SEQ_NBR = DC.DEAC_SEQ_NBR (+)
ORDER BY
    BUSINESS_AREA_REGION_CATEGORY DESC,
    BUSINESS_AREA_REGION,
    BUSINESS_AREA,
    FIELD_TEAM_DESC,
    URI,
    POC,
    POT,
    R.DEAC_SEQ_NBR;