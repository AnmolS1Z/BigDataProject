INSERT OVERWRITE TABLE anmolsandhu_quake_state_month_hbase
SELECT
    concat(
            state, '#',
            cast(year as string), '#',
            lpad(cast(month as string), 2, '0')
    ) AS rowkey,
    CAST(quake_count AS BIGINT) AS quake_count,
    max_mag,
    label_quake_ge_4,
    pred_prob_ge_4
FROM anmolsandhu_quake_state_month_scored;
