SELECT
  *,
  num_orders / DATE_DIFF(month_end, month_start, DAY) AS frequency,
  DATE_DIFF(month_end, last_order_of_month, DAY) AS recency_monthly
FROM
(
  SELECT
    oper_aff_id,
    oper_cntry_id,
    global_account_id,
    account_type_ord,
    DATE_TRUNC(DATE(ord_dt), month) month_start,
    LAST_DAY(DATE(ord_dt), month) month_end,
    MAX(DATE(ord_dt)) last_order_of_month,
    COUNT (ord_id) AS num_orders,
    SUM (adj_ln_lc_net) AS total_month_spend
  FROM
    `amw-dna-coe-curated.demand.ds_det_daily_v`
  WHERE
    ( ord_mo_yr_id >= {{ get_start_month("RENEWAL_START_MONTH") }}
      AND oper_aff_id IN {{ get_affiliates('ALL') }} )
  GROUP BY
    oper_aff_id,
    oper_cntry_id,
    global_account_id,
    account_type_ord,
    month_start,
    month_end
)