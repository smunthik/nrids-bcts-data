SELECT
    official.*,
    lrm.*
FROM
    DRIP_60_Official AS official
    LEFT JOIN DRIP_60_LRM AS lrm ON official.forest_file_id = lrm.LICENCE_ID
ORDER BY
    official.auction_date DESC;