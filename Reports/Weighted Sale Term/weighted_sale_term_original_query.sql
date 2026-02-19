--weighted term
select
    Business_Area_Region_Category,
    Business_Area_Region,
    Business_Area,
    Auction_Fiscal,
    awarded_licence_volume_class,
    sum(awarded_licence_volume),
    sum(awarded_licence_volume_X_tenure_term),
    round(
        sum(awarded_licence_volume_X_tenure_term) / sum(awarded_licence_volume),
        1
    ) as weighted_tenure_term,
    count(awarded_licence_volume) as count_awarded_licences

from (
    select distinct
        case
            when
                ou.org_unit_code in ('TBA', 'TPL', 'TPG', 'TSK', 'TSN', 'TCC', 'TKA', 'TKO', 'TOC')
            then
                'Interior'
            when
                ou.org_unit_code in ('TCH', 'TST', 'TSG')
            then
                'Coast'
            end as Business_Area_Region_Category,
        case
            when
                ou.org_unit_code in ('TBA', 'TPL', 'TPG', 'TSK', 'TSN')
            then
                'North Interior'
            when
                ou.org_unit_code in ('TCC', 'TKA', 'TKO', 'TOC')
            then
                'South Interior'
            when
                ou.org_unit_code in ('TCH', 'TST', 'TSG')
            then
                'Coast'
            end as Business_Area_Region,
        decode(
            ou.org_unit_code,
            null,
            null,
            replace(
                decode(
                    ou.org_unit_name,
                    'Seaward Timber Sales Office',
                    'Seaward-Tlasta',
                    ou.org_unit_name
                ) || ' (' || ou.org_unit_code || ')',
                ' Timber Sales Office',
                ''
            )
        ) as Business_Area,
        ou.org_unit_code as Business_Area_Code,
        ts.forest_file_id,
        pfu.file_type_code,
        case
            when
                cc.description is null
            then
                ts.bcts_category_code
            else
                cc.description || ' (' || ts.bcts_category_code || ')'
            end as BCTS_Category,
        ts.auction_date as BCTS_Admin_Auction_Date,
        extract(
            year from(add_months(ts.auction_date, 9))
        ) as Auction_Fiscal,
        decode(
            ts.auction_date,
            null,
            null,
            'Q' || Ceil((EXTRACT(Month From Add_Months(ts.auction_date, -3))) / 3)
        ) AS Auction_Quarter,
        tt.legal_effective_dt as FTA_Legal_Effective_Date,
        extract(
            year from(add_months(tt.legal_effective_dt, 9))
        ) as Legal_Effective_Fiscal,
        decode(
            tt.legal_effective_dt,
            null,
            null,
            'Q' || Ceil((EXTRACT(Month From Add_Months(tt.legal_effective_dt, -3))) / 3)
        ) AS Legal_Effective_Quarter,
        tt.tenure_term,
        sold_licence_bid_info.sale_volume as sold_licence_volume,
        tt.tenure_term * sold_licence_bid_info.sale_volume as sold_licence_volume_X_tenure_term,

        fc_sold.client_number as sold_licence_client_number,
        (
            decode(fc_sold.legal_first_name, null, null, fc_sold.legal_first_name || ' ')
            || decode(fc_sold.legal_middle_name, null, null, fc_sold.legal_middle_name || ' ')
            || fc_sold.client_name
        ) as sold_licence_client_name,
        awarded_sale_info.sale_volume as awarded_licence_volume,
        case
            when
                awarded_sale_info.sale_volume <= 5000
            then
                '0.0 to 5,000.0 m3'
            when
                awarded_sale_info.sale_volume <= 15000
            then
                '5,000.1 to 15,000.0 m3'
            when
                awarded_sale_info.sale_volume <= 30000
            then
                '15,000.1 to 30,000.0 m3'
            when
                awarded_sale_info.sale_volume <= 75000
            then
                '30,000.1 to 75,000.0 m3'
            else
                '75,000.1 m3 and above'
            end as awarded_licence_volume_class,
        case
            when
                awarded_sale_info.sale_volume <= 5000
            then
                1
            when
                awarded_sale_info.sale_volume <= 15000
            then
                2
            when
                awarded_sale_info.sale_volume <= 30000
            then
                3
            when
                awarded_sale_info.sale_volume <= 75000
            then
                4
            else
                5
            end as awarded_licence_volume_class_sort_order,
        tt.tenure_term * awarded_sale_info.sale_volume as awarded_licence_volume_X_tenure_term,

        fc_awarded.client_number as awarded_licence_client_number,
        (
            decode(fc_awarded.legal_first_name, null, null, fc_awarded.legal_first_name || ' ')
            || decode(fc_awarded.legal_middle_name, null, null, fc_awarded.legal_middle_name || ' ')
            || fc_awarded.client_name
        ) as awarded_licence_client_name,
     
        case
            when
                pfu.file_status_st is not NULL
            THEN
                tfsc.description || ' (' || pfu.file_status_st || ')'
            ELSE
                NULL
            end as FTA_File_Status,
        pfu.file_status_date as FTA_File_Status_Date,
        case
            when
                ts.no_sale_rationale_code is null

                AND pfu.file_status_st IN (
                    'HI',  -- Issued
                    'HC',  -- Closed
                    'LC',  -- Logging Complete
                    'HX',  -- Cancelled
                    'HS',  -- Suspended
                    'HRS'  -- Harvesting Rights Surrendered
                )

                /* Tenure term legal effective date in reporting period*/
                and tt.legal_effective_dt
                    between To_Date('2015-04-01', 'YYYY-MM-DD')  -- Date: beginning of reporting period
                    and To_Date('2025-03-31', 'YYYY-MM-DD')  -- Date: end of reporting period
            then
                'Y'
            else
                'N'
            end as Sold_in_Report_Period,
        case
            when
                ts.auction_date
                    between To_Date('2015-04-01', 'YYYY-MM-DD')  -- Date: beginning of reporting period
                    and To_Date('2025-03-31', 'YYYY-MM-DD')  -- Date: end of reporting period
            then
                'Y'
            else
                'N'
            end as Auction_in_Report_Period,
        decode(pfu.file_type_code, 'B20', null, pfu.file_type_code) as QA_non_B20_licence,
        case
            when
                ts.auction_date > sysdate
            then
                'Future auction (BCTS Admin)'
            when
                (
                    sold_licence_maximum_value is null
                    or sold_licence_maximum_value = 0
                )
                and
                (
                    awarded_licence_maximum_value is null
                    or awarded_licence_maximum_value = 0
                )
                and ts.no_sale_rationale_code is null
            then
                'Auction result data missing (BCTS Admin)'
            end as QA_auction_results_missing_bcts_admin
 
    FROM
        the.tenure_term tt,
        the.bcts_timber_sale ts,
        the.bcts_category_code cc,
        the.prov_forest_use pfu,
        the.tenure_file_status_code tfsc,
        the.org_unit ou,
        the.forest_client fc_sold,
        the.forest_client fc_awarded,

        /* Bid Info for Sold Licences (Licences issued within reporting period) */
        (
            select
                ts0.forest_file_id,
                ts0.auction_date,
                ts0.total_upset_value as cruise_total_upset_value,
                ts0.UPSET_RATE as scale_upset_rate,
                ts0.sale_volume as sale_volume,
                tb.bonus_bid AS sold_licence_bonus_bid,
                tb.bonus_offer AS sold_licence_bonus_offer,
                case
                    when
                        ts0.TOTAL_UPSET_VALUE > 0
                    then
                            round(
                                ts0.TOTAL_UPSET_VALUE + tb.bonus_offer,  -- Cruise-based licence pricing
                                2
                            )
                    else
                            round(
                                (ts0.UPSET_RATE + tb.BONUS_BID) * ts0.sale_volume,  -- Scale-based licence pricing
                                2
                            )
                    end as sold_licence_maximum_value,
                tb.client_number as sold_licence_client_number

            from
                the.bcts_timber_sale ts0,
                the.bcts_tenure_bidder tb,
                the.prov_forest_use pfu,
                the.tenure_term tt

            where
                pfu.forest_file_id = ts0.forest_file_id
                and pfu.forest_file_id = tt.forest_file_id
                and ts0.forest_file_id = tb.forest_file_id
                and ts0.auction_date = tb.auction_date
                and upper(tb.sale_awarded_ind) = 'Y'  -- Only look at the winning bid
                and ts0.no_sale_rationale_code is null

                AND pfu.file_status_st IN (
                    'HI',  -- Issued
                    'HC',  -- Closed
                    'LC',  -- Logging Complete
                    'HX',  -- Cancelled
                    'HS',  -- Suspended
                    'HRS'  -- Harvesting Rights Surrendered
                )

                /* Tenure term legal effective date in reporting period*/
                AND tt.legal_effective_dt
                    between To_Date('2015-04-01', 'YYYY-MM-DD')  -- Date: beginning of reporting period
                    and To_Date('2025-03-31', 'YYYY-MM-DD')  -- Date: end of reporting period
        ) sold_licence_bid_info,


        /* Bid Info for Successful Auctions (Licences awarded within reporting period) */
        (
            select
                ts1.forest_file_id,
                ts1.auction_date,
                ts1.total_upset_value as cruise_total_upset_value,
                ts1.UPSET_RATE as scale_upset_rate,
                ts1.sale_volume as sale_volume,
                tb.bonus_bid AS awarded_sale_bonus_bid,
                tb.bonus_offer AS awarded_sale_bonus_offer,
                case
                    when
                        ts1.TOTAL_UPSET_VALUE > 0
                    then
                            round(
                                ts1.TOTAL_UPSET_VALUE + tb.bonus_offer,  -- Cruise-based licence pricing
                                2
                            )
                    else
                            round(
                                (ts1.UPSET_RATE + tb.BONUS_BID) * ts1.sale_volume,  -- Scale-based licence pricing
                                2
                            )
                    end as awarded_licence_maximum_value,
                tb.client_number as awarded_licence_client_number

            from
                the.bcts_timber_sale ts1,
                the.bcts_tenure_bidder tb

            where
                ts1.forest_file_id = tb.forest_file_id
                and ts1.auction_date = tb.auction_date
                and upper(tb.sale_awarded_ind) = 'Y'  -- Only look at the winning bid
                and ts1.auction_date
                    between To_Date('2015-04-01', 'YYYY-MM-DD')  -- Date: beginning of reporting period
                    and To_Date('2025-03-31', 'YYYY-MM-DD')  -- Date: end of reporting period
        ) awarded_sale_info

    WHERE
        ts.forest_file_id = tt.forest_file_id (+)
        AND ts.forest_file_id = pfu.forest_file_id (+)
        AND pfu.bcts_org_unit = ou.org_unit_no (+)
        AND pfu.file_status_st = tfsc.tenure_file_status_code (+)
        and ts.bcts_category_code = cc.bcts_category_code (+)
        AND ts.forest_file_id = sold_licence_bid_info.forest_file_id (+)
        AND ts.auction_date = sold_licence_bid_info.auction_date (+)
        and ts.forest_file_id = awarded_sale_info.forest_file_id (+)
        and ts.auction_date = awarded_sale_info.auction_date (+)

        and sold_licence_bid_info.sold_licence_client_number = fc_sold.client_number (+)
        and awarded_sale_info.awarded_licence_client_number = fc_awarded.client_number (+)
        and (
            /* Criteria for Licences Sold in reporting period*/
            (
                ts.no_sale_rationale_code is null

                AND pfu.file_status_st IN (
                    'HI',  -- Issued
                    'HC',  -- Closed
                    'LC',  -- Logging Complete
                    'HX',  -- Cancelled
                    'HS',  -- Suspended
                    'HRS'  -- Harvesting Rights Surrendered
                )

                /* Tenure term legal effective date in reporting period*/
                AND tt.legal_effective_dt
                    between to_date('2015-04-01', 'YYYY-MM-DD')  -- Date: beginning of reporting period
                    and to_date('2025-03-31', 'YYYY-MM-DD')  -- Date: end of reporting period
            )
            /* Criteria for auctions within the reporting period */
            or ts.auction_date
                between to_date('2015-04-01', 'YYYY-MM-DD')  -- Date: beginning of reporting period
                and to_date('2025-03-31', 'YYYY-MM-DD')  -- Date: end of reporting period
        )

    ORDER BY
        business_area_region_category desc,
        business_area_region,
        business_area,
        bcts_category,
        forest_file_id,
        bcts_admin_auction_date desc

) per_licence

group by
    Business_Area_Region_Category,
    Business_Area_Region,
    Business_Area,
    Auction_Fiscal,
    awarded_licence_volume_class,
    awarded_licence_volume_class_sort_order

ORDER BY
    business_area_region_category desc,
    business_area_region,
    business_area,
    auction_fiscal desc,
    awarded_licence_volume_class_sort_order
;