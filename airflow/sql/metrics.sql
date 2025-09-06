INSERT INTO metrics (
    date, total_sales, avg_sales_per_store, holiday_sales_increase,
    max_sales, min_sales, avg_holiday_multiplier, avg_unemployment_impact
)
SELECT 
    date,
    SUM(weekly_sales) AS total_sales,
    AVG(weekly_sales) / (SELECT COUNT(DISTINCT store) FROM processed_walmart) AS avg_sales_per_store,
    SUM(adjusted_sales) - SUM(weekly_sales) AS holiday_sales_increase,
    MAX(weekly_sales) AS max_sales,
    MIN(weekly_sales) AS min_sales,
    AVG(holiday_multiplier) AS avg_holiday_multiplier,
    AVG(unemployment_impact) AS avg_unemployment_impact
FROM processed_walmart
GROUP BY date
ON CONFLICT (date) DO UPDATE
SET total_sales = EXCLUDED.total_sales,
    avg_sales_per_store = EXCLUDED.avg_sales_per_store,
    holiday_sales_increase = EXCLUDED.holiday_sales_increase,
    max_sales = EXCLUDED.max_sales,
    min_sales = EXCLUDED.min_sales,
    avg_holiday_multiplier = EXCLUDED.avg_holiday_multiplier,
    avg_unemployment_impact = EXCLUDED.avg_unemployment_impact;
