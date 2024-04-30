SELECT DISTINCT
    a.aff_id,
    a.global_account_id,
    a.current_entry_dt,
    a.renewal_dt,
    a.auto_renewal_flag,
    a.status_desc,
    a.account_type_desc,
    DATE_ADD(a.current_entry_dt, INTERVAL 1 YEAR) first_year_end_date,
    max(DATE_DIFF(b.ord_dt, a.current_entry_dt,DAY)) days_to_last_order
FROM `amw-dna-coe-curated.account_master.account_master_snapshot` a
INNER JOIN `amw-dna-coe-curated.demand.ds_det_daily_v` b
    ON a.global_account_id = b.global_account_id
WHERE
    a.current_entry_dt >= DATE '{{ get_start_date("RENEWAL_START_MONTH") }}'
    AND b.ord_mo_yr_id >= {{ get_start_month("RENEWAL_START_MONTH") }}
    AND a.aff_id IN {{ get_affiliates('RENEWAL') }}
    AND a.account_type_desc like '%ABO%'
GROUP BY
    a.aff_id,
    a.current_entry_dt,
    first_year_end_date,
    a.renewal_dt,
    a.global_account_id,
    a.auto_renewal_flag,
    a.status_desc,
    a.account_type_desc