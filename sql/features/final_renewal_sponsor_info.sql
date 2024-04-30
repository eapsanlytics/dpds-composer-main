SELECT
    aff_id,
    global_account_id,
    app_entry_date,
    date_trunc(date(app_entry_date), month) month_start, -- first day of the month
    last_day(date(app_entry_date), month) month_end, -- last day of the month
    sponsoring_count_0_to_13_days_tenure,
    sponsoring_count_14_to_30_days_tenure,
    sponsoring_count_31_to_60_days_tenure,
    sponsoring_count_61_to_180_days_tenure,
    sponsoring_count_more_than_180_days_tenure
FROM `{{ params.sponsor_info_tablespec }}`
WHERE
    app_entry_date >= TIMESTAMP '{{ get_start_date("RENEWAL_START_MONTH") }}'
    AND aff_id IN {{ get_affiliates("RENEWAL") }}
ORDER BY app_entry_date