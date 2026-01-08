
SELECT DISTINCT
    ou.org_unit_code,
    pfu.mgmt_unit_type,
    pfu.mgmt_unit_id,
    CASE
        WHEN pfu.mgmt_unit_type = 'U' THEN ta.description
        ELSE tf.description
    END AS management_unit,
    ts.forest_file_id,
    ts.bcts_category_code,
    c.description AS category,
    pfu.file_status_st,
    tt.legal_effective_dt,
    EXTRACT(YEAR FROM (tt.legal_effective_dt + INTERVAL '9 months')) AS fiscal_issued,
    ts.auction_date,
    ts.sale_volume,
    ts.upset_rate,
    ts.total_upset_value,
    btb.bonus_bid,
    btb.bonus_offer,
    ffc.client_number,
    ffc.client_locn_code
FROM THE.bcts_timber_sale        AS ts
LEFT JOIN THE.bcts_category_code AS c
       ON ts.bcts_category_code = c.bcts_category_code
LEFT JOIN THE.bcts_tenure_bidder AS btb
       ON ts.forest_file_id = btb.forest_file_id
      AND ts.auction_date   = btb.auction_date
      AND btb.sale_awarded_ind = 'Y'
LEFT JOIN THE.forest_client      AS fc
       ON btb.client_number = fc.client_number
JOIN THE.forest_file_client      AS ffc
       ON ts.forest_file_id = ffc.forest_file_id
JOIN THE.client_location         AS cl
       ON ffc.client_number   = cl.client_number
      AND ffc.client_locn_code = cl.client_locn_code
JOIN THE.forest_client           AS fc2
       ON cl.client_number = fc2.client_number
JOIN THE.prov_forest_use         AS pfu
       ON ts.forest_file_id = pfu.forest_file_id
JOIN THE.org_unit                AS ou
       ON pfu.bcts_org_unit = ou.org_unit_no
LEFT JOIN THE.tsa_number_code    AS ta
       ON pfu.mgmt_unit_id = ta.tsa_number
LEFT JOIN THE.tfl_number_code    AS tf
       ON pfu.mgmt_unit_id = tf.tfl_number
LEFT JOIN THE.tenure_term        AS tt
       ON pfu.forest_file_id = tt.forest_file_id
      AND tt.legal_effective_dt BETWEEN DATE '2023-04-01' AND DATE '2024-03-31'
WHERE pfu.file_status_st IN ('HI', 'HC', 'LC', 'HX', 'HS', 'HRS')
  AND ts.no_sale_rationale_code IS NULL
ORDER BY tt.legal_effective_dt DESC;
``
