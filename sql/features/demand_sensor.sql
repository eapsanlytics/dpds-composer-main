SELECT
    *
FROM
    (
        SELECT
            COUNTIF(last_order < DATE '{{ (dag_run.logical_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=+1)).strftime("%Y-%m-%d") }}') = 0 AS all_last_orders_today_or_later
        FROM (
        SELECT
            oper_aff_id,
            MAX(DATE(ord_dt)) last_order
        FROM
            `amw-dna-coe-curated.demand.ds_det_daily_v`
        WHERE
            ( ord_mo_yr_id >= {{ ds_nodash[0:6] }}
            AND oper_aff_id IN {{ get_affiliates('ALL') }} )
        GROUP BY
            oper_aff_id
    )
)
WHERE all_last_orders_today_or_later = TRUE