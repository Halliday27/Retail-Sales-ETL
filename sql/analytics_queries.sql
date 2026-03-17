-- 1) Total sales by day
SELECT d.full_date, SUM(f.total_amount) AS total_sales
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date;

-- 2) Top-selling products
SELECT p.product_name, SUM(f.quantity) AS total_qty
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_qty DESC;

-- 3) Sales by customer city
SELECT c.city, SUM(f.total_amount) AS total_sales
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.city
ORDER BY total_sales DESC;
