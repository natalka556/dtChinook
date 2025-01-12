CREATE VIEW v_customers_per_city AS
SELECT city, COUNT(*) AS customer_count
FROM dm_customer
GROUP BY city;

CREATE VIEW v_monthly_sales AS
SELECT dd.month, dd.year, SUM(fi.total) AS total_sales
FROM facts_invoice fi
JOIN dm_date dd ON fi.dm_date_iddm_date = dd.iddm_date
GROUP BY dd.month, dd.year;

CREATE VIEW v_total_sales_per_track AS
SELECT dt.iddm_tracks, dt.name, SUM(fi.total) AS total_sales
FROM facts_invoice fi
JOIN dm_tracks dt ON fi.dm_tracks_iddm_tracks = dt.iddm_tracks
GROUP BY dt.iddm_tracks, dt.name;

CREATE VIEW v_top_genres AS
SELECT dt.genre_name, SUM(fi.quantity) AS total_sold
FROM facts_invoice fi
JOIN dm_tracks dt ON fi.dm_tracks_iddm_tracks = dt.iddm_tracks
GROUP BY dt.genre_name
ORDER BY total_sold DESC;

CREATE VIEW v_top_selling_tracks AS
SELECT dt.name, SUM(fi.quantity) AS total_sold
FROM facts_invoice fi
JOIN dm_tracks dt ON fi.dm_tracks_iddm_tracks = dt.iddm_tracks
GROUP BY dt.name
ORDER BY total_sold DESC
LIMIT 10;