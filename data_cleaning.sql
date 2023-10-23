-- This SQL code performed creation of fact table from a single source of truth. 

-- Cleaning the orders_table and changing the data type this table will serve as a single source of truth.

-- Alter the data type of the 'date_uuid' column

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID
USING date_uuid::UUID;

-- Alter the data type of the 'user_uuid' column
ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID
USING user_uuid::UUID;

-- Alter the data type of the 'card_number' column
ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(20);

-- Alter the data type of the 'store_code' column
ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(20);

-- Alter the data type of the 'product_code' column
ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(20);

-- Alter the data type of the 'product_quantity' column
ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT;


-- Add a foreign key constraint to the 'user_uuid' column in 'orders_table'
ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users_table (user_uuid);

-- Add a foreign key constraint to the 'store_code' column in 'orders_table'
ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

-- Add a foreign key constraint to the 'product_code' column in 'orders_table'
ALTER TABLE order_new
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);

-- Add a foreign key constraint to the 'date_uuid' column in 'orders_table'
ALTER TABLE order_new
ADD CONSTRAINT fk_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);

-- Add a foreign key constraint to the 'card_number' column in 'orders_table'
ALTER TABLE order_new_card
ADD CONSTRAINT fk_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);


-- Cleaning the dim_users_table and changing the data type.

-- Rename the table "dim_users" to "dim_users_table"
ALTER TABLE dim_users
RENAME TO dim_users_table;

-- Alter the data type of the 'last_name' column
ALTER TABLE dim_users_table
ALTER COLUMN first_name TYPE VARCHAR(225);

-- Alter the data type of the 'user_uuid' column
ALTER TABLE dim_users_table
ALTER COLUMN last_name TYPE VARCHAR(225);

-- Alter the data type of the 'date_of_birth' column
ALTER TABLE dim_users_table
ALTER COLUMN date_of_birth TYPE DATE;

-- Alter the data type of the 'country_code' column
ALTER TABLE dim_users_table
ALTER COLUMN country_code TYPE VARCHAR(20);

-- Alter the data type of the 'user_uuid' column
ALTER TABLE dim_users_table
ALTER COLUMN user_uuid TYPE UUID
USING user_uuid::UUID;

-- Alter the data type of the 'join_date' column
ALTER TABLE dim_users_table
ALTER COLUMN join_date TYPE DATE;

-- Assign primary key to user_uuid
ALTER TABLE dim_users_table
ADD PRIMARY KEY (user_uuid);


-- Cleaning the dim_store_details and changing the data type.
-- Alter the data type of the 'longitude' column
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT
USING longitude::double precision;

-- Alter the data type of the 'locality' column
ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(225);

-- Alter the data type of the 'store_code' column
ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(20);

-- Alter the data type of the 'staff_numbers' column
ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT;

-- Alter the data type of the 'opening_date' column
ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE
USING opening_date::DATE;

-- Alter the data type of the 'store_type' column
ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(225);

-- Alter the data type of the 'latitude' column
ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT
USING latitude::double precision;

-- Alter the data type of the 'country_code' column
ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(20);

-- Alter the data type of the 'continent' column
ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(225);

-- Assign primary key to store_code
ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);


-- Cleaning the dim_products table and changing the data type.
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);  -- Define the appropriate data type and length

UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid-Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
    ELSE NULL  -- Handle any other cases as needed
END;

-- Rename column removed to still_available
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

-- Alter the data type of the 'product_price' column
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT;

-- Alter the data type of the 'weight' column
ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT;

-- Alter the data type of the 'EAN' column
ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(225);

-- Alter the data type of the 'product_code' column
ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(225);

-- Alter the data type of the 'date_added' column
ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE
USING date_added::DATE;

-- Alter the data type of the 'uuid' column
ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID
USING uuid::UUID;

-- Updating still_available column to take the values of true and false
UPDATE dim_products
SET still_available = True
WHERE still_available = 'Still_avaliable';

UPDATE dim_products
SET still_available = FALSE
WHERE still_available = 'Removed';

-- Alter the data type of the 'still_available' column
ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOL
USING (still_available::boolean);


-- Alter the data type of the 'weight_class' column
ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(50);


-- Assign primary key to store_code
ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

-- Cleaning the dim_card_details table and changing the data type.

-- Alter the data type of the 'date_uuid' column
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(250);

-- Alter the data type of the 'user_uuid' column
ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(20);

-- Alter the data type of the 'date_uuid' column
ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE
USING date_payment_confirmed::DATE;

-- Assign primary key to card_number
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number)

-- Quering data to provide answers to specific business requirments
--Quering data no 1
-- The Operations team would like to know which countries we currently operate in and which country now has the most stores. Perform a query on the database to get the information,
    
SELECT country_code AS country, COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY country
ORDER BY total_no_stores DESC;

--Quering data no 2
-- The business stakeholders would like to know which locations currently have the most stores. They would like to close some stores before opening more in other locations. Find out which locations have the most stores currently.

SELECT locality, COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

--Quering data no 3
-- Query the database to find out which months typically have the most sales

SELECT ROUND(SUM(p.product_price * o.product_quantity)::numeric, 2) AS total_sales, d.month
FROM orders_table AS o
JOIN dim_date_times AS d ON o.date_uuid = d.date_uuid
JOIN dim_products AS p ON o.product_code = p.product_code
GROUP BY d.month
ORDER BY total_sales DESC
LIMIT 6;

--Quering data no 4
-- The company is looking to increase its online sales. They want to know how many sales are happening online vs offline. Calculate how many products were sold and the amount of sales made for online and offline purchases.

SELECT 
    COUNT(p.product_price) AS numbers_of_sales,
    SUM(o.product_quantity) AS product_quantity_count,
    CASE
        WHEN s.store_type = 'Web Portal' THEN 'web portal'
        ELSE 'Offline'
    END AS location
FROM dim_store_details AS s
JOIN orders_table AS o ON s.store_code = o.store_code
JOIN dim_products AS p ON o.product_code = p.product_code
GROUP BY
    CASE
        WHEN s.store_type = 'Web Portal' THEN 'web portal'
        ELSE 'Offline'
    END;

--Quering data no 5
-- The sales team wants to know which of the different store types is generated the most revenue so they know where to focus. Find out the total and percentage of sales coming from each of the different store types.

SELECT
    CASE
        WHEN s.store_type = 'Local' THEN 'Local'
        WHEN s.store_type = 'Web Portal' THEN 'Web Portal'
        WHEN s.store_type = 'Super Store' THEN 'Super Store'
        WHEN s.store_type = 'Mall Kiosk' THEN 'Mall Kiosk'
        ELSE 'Outlet'
    END AS store_type,
    ROUND(SUM(o.product_quantity::numeric * p.product_price::numeric), 2) AS total_sales,
    ROUND((SUM(o.product_quantity::numeric * p.product_price::numeric) / SUM(SUM(o.product_quantity::numeric * p.product_price::numeric)) OVER ()) * 100, 2) AS "percentage_total(%)"
FROM dim_store_details AS s
JOIN orders_table AS o ON s.store_code = o.store_code
JOIN dim_products AS p ON o.product_code = p.product_code
GROUP BY store_type
ORDER BY "percentage_total(%)" DESC;

--Quering data no 6
-- The company stakeholders want assurances that the company has been doing well recently. Find which months in which years have had the most sales historically.

SELECT ROUND(SUM(p.product_price::numeric*o.product_quantity::numeric),2) AS total_sales, d.year AS Year, d.month AS Month
FROM dim_date_times AS d
JOIN orders_table AS o ON d.date_uuid = o.date_uuid
JOIN dim_products AS p ON o.product_code = p.product_code
GROUP BY d.year, d.month
ORDER BY total_sales DESC
LIMIT 10;

--Quering data no 7
--The operations team would like to know the overall staff numbers in each location around the world. Perform a query to determine the staff numbers in each of the countries the company sells in.

SELECT SUM(staff_numbers) AS total_staff_numbers, country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

--Quering data no 8
-- The sales team is looking to expand their territory in Germany. Determine which type of store is generating the most sales in Germany.

SELECT ROUND(SUM(p.product_price::numeric*o.product_quantity::numeric),2) AS total_sales, store_type, country_code
FROM dim_store_details AS s
JOIN orders_table AS o ON s.store_code = o.store_code
JOIN dim_products AS p ON o.product_code = p.product_code
WHERE country_code = 'DE'
GROUP BY store_type, country_code
ORDER BY total_sales;

--Quering data no 9
-- Sales would like the get an accurate metric for how quickly the company is making sales. Determine the average time taken between each sale grouped by year, 

SELECT
    EXTRACT(YEAR FROM datetime) AS year,
    JSON_BUILD_OBJECT(
        'hours', TRUNC(AVG(time_diff) / 3600),
        'minutes', TRUNC(MOD(AVG(time_diff) / 60, 60)),
        'seconds', TRUNC(MOD(AVG(time_diff), 60)),
        'milliseconds', TRUNC(MOD(AVG(time_diff) * 1000, 1000))
    ) AS actual_time_taken
FROM (
    SELECT
        datetime,
        EXTRACT(EPOCH FROM LEAD(datetime) OVER (ORDER BY datetime) - datetime) AS time_diff
    FROM dim_date_times
) subquery
GROUP BY year
ORDER BY AVG(time_diff) DESC
LIMIT 5;
