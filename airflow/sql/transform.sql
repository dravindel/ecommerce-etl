INSERT INTO processed_walmart (
    store, date, weekly_sales, holiday_flag, temperature, fuel_price, cpi, unemployment,
    store_contribution_pct, holiday_multiplier, adjusted_sales, unemployment_impact, holiday_impact, sales_per_store
)
SELECT 
    store, date, weekly_sales, holiday_flag, temperature, fuel_price, cpi, unemployment,
    (weekly_sales / SUM(weekly_sales) OVER ()) * 100 AS store_contribution_pct,
    1 + 0.1 * holiday_flag AS holiday_multiplier,
    weekly_sales * (1 + 0.1 * holiday_flag) AS adjusted_sales,
    weekly_sales * (unemployment / 100) AS unemployment_impact,
    holiday_flag * (weekly_sales * (1 + 0.1 * holiday_flag)) AS holiday_impact,
    weekly_sales AS sales_per_store
FROM raw_walmart
WHERE date = :execution_date
ON CONFLICT (store, date) DO NOTHING;