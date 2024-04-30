SELECT
   oper_aff_id,
   global_vol_account_id,
   date_trunc(date(ord_dt), month) month_start, -- first day of the month
   last_day(date(ord_dt), month) month_end, -- last day of the month
   SUM (order_bv_usd) AS total_month_bv -- how much was spent in local currency each month?
FROM `{{ params.ds_det_daily_summary_vol_abo_tablespec }}`
WHERE
   ord_mo_yr_id >= {{ get_start_month("RFM_START_MONTH") }}
   AND oper_aff_id IN {{ get_affiliates("RENEWAL") }}
GROUP BY
   oper_aff_id,
   global_vol_account_id,
   month_start,
   month_end
ORDER BY global_vol_account_id, month_end