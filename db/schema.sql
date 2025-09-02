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
    holiday_impact NUMERIC
);

CREATE TABLE IF NOT EXISTS metrics (
    date DATE,
    total_sales NUMERIC,
    avg_sales_per_store NUMERIC,
    holiday_sales_increase NUMERIC
);