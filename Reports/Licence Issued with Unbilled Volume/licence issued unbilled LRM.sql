SELECT
    licence.licn_seq_nbr,
    licence.business_area,
    licence.licence_id,
    logging_started.Logging_Started_Date AS Last_Logging_Started_Date,
    logging_completed.Logging_Completed_Date AS Last_Logging_Completed_Date,
    CASE
        WHEN logging_started.Logging_Started_Date IS NULL
        AND logging_completed.Logging_Completed_Date IS NULL THEN NULL
        ELSE 'Active_Harvesting'
    END AS Harvesting_Status,
    licence.field_team,
    licence.tenure,
    issued.Issued_Done_LRM, -- 1.
    substantial_completion.Substantial_Completion_Done_LRM, -- 3.
    decode (
        issued.Issued_Done_LRM,
        NULL,
        NULL,
        'Fiscal ' | | to_number (
            extract (
                year
                FROM
                    (add_months (issued.Issued_Done_LRM, 9))
            ) - 1
        ) | | '/' | | extract (
            year
            FROM
                (add_months (issued.Issued_Done_LRM, 9))
        )
    ) Issued_Done_Fiscal_LRM,
    expire_extend.Expire_Extend_LRM -- 6.
FROM
    /* 0. Licence info */
    (
        SELECT
            licn_seq_nbr,
            max(tso_name) AS business_area,
            max(licence_id) AS licence_id,
            max(field_team) AS field_team,
            max(tenure) AS tenure
        FROM
            forestview.v_licence
        GROUP BY
            licn_seq_nbr
    ) licence,
    /* 1. Licence Issued Done */
    (
        SELECT
            licn_seq_nbr,
            max(activity_date) AS Issued_Done_LRM
        FROM
            forestview.v_licence_activity_all
        WHERE
            activity_class = 'CML' -- Corporate Mandatory Licence activity
            AND actt_key_ind = 'HI' -- Licence Issued
            AND acti_status_ind = 'D' -- Done
        GROUP BY
            licn_seq_nbr
    ) issued,
    /* 2. Licence Closed Done */
    (
        SELECT
            licn_seq_nbr,
            max(activity_date) AS Closed_Done_LRM
        FROM
            forestview.v_licence_activity_all
        WHERE
            activity_class = 'CML' -- Corporate Mandatory Licence activity
            AND actt_key_ind = 'HC' -- Licence Closed
            AND acti_status_ind = 'D' -- Done
        GROUP BY
            licn_seq_nbr
    ) closed,
    /* 3. Licence Substantial Completion Done */
    (
        SELECT
            licn_seq_nbr,
            max(activity_date) AS Substantial_Completion_Done_LRM
        FROM
            forestview.v_licence_activity_all
        WHERE
            activity_class = 'CML' -- Corporate Mandatory Licence activity
            AND actt_key_ind = 'LC' -- Substantial Completion
            AND acti_status_ind = 'D' -- Done
        GROUP BY
            licn_seq_nbr
    ) substantial_completion,
    /* 4. Licence Surrendered Done */
    (
        SELECT
            licn_seq_nbr,
            max(activity_date) AS Surrendered_Done_LRM
        FROM
            forestview.v_licence_activity_all
        WHERE
            activity_class = 'CML' -- Corporate Mandatory Licence activity
            AND actt_key_ind = 'HS' -- Licence Surrendered
            AND acti_status_ind = 'D' -- Done
        GROUP BY
            licn_seq_nbr
    ) surrendered,
    /* 5. Licence Cancelled Done */
    (
        SELECT
            licn_seq_nbr,
            max(activity_date) AS Cancelled_Done_LRM
        FROM
            forestview.v_licence_activity_all
        WHERE
            activity_class = 'CML' -- Corporate Mandatory Licence activity
            AND actt_key_ind = 'HX' -- Licence Cancelled
            AND acti_status_ind = 'D' -- Done
        GROUP BY
            licn_seq_nbr
    ) cancelled,
    /* 6. Licence Expiry: the later of the EXPIRE and EXTEND activities */
    (
        SELECT
            licn_seq_nbr,
            max(activity_date) AS Expire_Extend_LRM -- The later of the two dates
        FROM
            forestview.v_licence_activity_all
        WHERE
            activity_class = 'CML' -- Corporate Mandatory Licence activity
            AND actt_key_ind IN (
                'EXPIRE', -- Licence Expiry Date
                'EXTEND' -- Licence Extension Date
            )
        GROUP BY
            licn_seq_nbr
    ) expire_extend,
    (
        SELECT
            LICN_SEQ_NBR,
            MAX(activity_date) AS Logging_Started_Date
        FROM
            forestview.v_block_activity_all
        WHERE
            actt_key_ind = 'HVS' -- Logging Started
            AND acti_status_ind = 'D' -- Done
        GROUP BY
            LICN_SEQ_NBR
    ) logging_started,
    (
        SELECT
            LICN_SEQ_NBR,
            MAX(activity_date) AS Logging_Completed_Date
        FROM
            forestview.v_block_activity_all
        WHERE
            actt_key_ind = 'HVC' -- Logging Completed
            AND acti_status_ind = 'D' -- Done
        GROUP BY
            LICN_SEQ_NBR
    ) logging_completed
WHERE
    /*  1. Licence Issued (HI) activity */
    licence.licn_seq_nbr = issued.licn_seq_nbr (+)
    /* 2. Licence Closed (HC) activity */
    AND licence.licn_seq_nbr = closed.licn_seq_nbr (+)
    /* 3. Substantial Completion (LC) activity */
    AND licence.licn_seq_nbr = substantial_completion.licn_seq_nbr (+)
    /* 4. Licence Surrendered (HS) activity */
    AND licence.licn_seq_nbr = surrendered.licn_seq_nbr (+)
    /* 5. Licence Cancelled (HX) activity */
    AND licence.licn_seq_nbr = cancelled.licn_seq_nbr (+)
    /* 6. The later of the licence expiry or extension dates */
    AND licence.licn_seq_nbr = expire_extend.licn_seq_nbr (+)
    AND licence.licn_seq_nbr = logging_started.licn_seq_nbr (+)
    AND licence.licn_seq_nbr = logging_completed.licn_seq_nbr (+)