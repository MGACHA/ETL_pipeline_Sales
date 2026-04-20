
-- TOP 10 customers
SELECT TOP 10 customer_name, SUM(sales) AS total_sales
FROM [dbo].[Superstore_clean_data]
GROUP BY customer_name
ORDER BY total_sales DESC;


-- Monthy sales
SELECT 
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    SUM(sales) AS total_sales
FROM clean_data
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;

-- Most profitable category
SELECT category, SUM(profit) AS total_profit
FROM clean_data
GROUP BY category
ORDER BY total_profit DESC;


--Average delivery time
SELECT AVG(DATEDIFF(day, order_date, ship_date)) AS avg_delivery_days
FROM clean_data;
