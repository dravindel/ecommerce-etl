CREATE TABLE IF NOT EXISTS raw_walmart (
    store INTEGER,
    date DATE,
    weekly_sales NUMERIC,
    holiday_flag INTEGER,
    temperature NUMERIC,
    fuel_price NUMERIC,
    cpi NUMERIC,
    unemployment NUMERIC
);

CREATE TABLE IF NOT EXISTS processed_walmart (
    store INTEGER,
    date DATE,
    weekly_sales NUMERIC,
    holiday_flag INTEGER,
    temperature NUMERIC,
    fuel_price NUMERIC,
    cpi NUMERIC,
    unemployment NUMERIC,
    sales_per_store NUMERIC,
    holiday_impact NUMERIC,
    store_contribution_pct NUMERIC,
    holiday_multiplier NUMERIC,
    adjusted_sales NUMERIC,
    unemployment_impact NUMERIC
);

CREATE TABLE IF NOT EXISTS metrics (
    date DATE PRIMARY KEY,
    total_sales NUMERIC,
    avg_sales_per_store NUMERIC,
    holiday_sales_increase NUMERIC,
    max_sales NUMERIC,
    min_sales NUMERIC,
    avg_holiday_multiplier NUMERIC,
    avg_unemployment_impact NUMERIC
);