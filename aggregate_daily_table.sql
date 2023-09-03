CREATE SCHEMA reporting_schema;

DROP TABLE IF EXISTS reporting_schema.aggregate_daily;
CREATE TABLE IF NOT EXISTS reporting_schema.aggregate_daily 
(
	rental_day DATE PRIMARY KEY, 
	total_rentals INT,
	rental_difference NUMERIC, 
	total_revenue NUMERIC, 
	max_paid_amount NUMERIC, 
	min_paid_amount NUMERIC, 
	avg_rental_duration NUMERIC,
	returned_on_time NUMERIC, 
	late_returns NUMERIC, 
	percentage_late_returns NUMERIC, 
	most_rented_category VARCHAR, 
	total_rentals_per_cat INT,
	least_rented_category VARCHAR, 
	total_rentals_per_least_cat INT,
	most_rented_movie VARCHAR, 
	city_with_most_customers VARCHAR, 
	most_rented_rating VARCHAR,
	rentals_per_rating INT,
	total_customers INT, 
	total_active_customers INT, 
	active_customers_perc NUMERIC, 
	total_customers_renting_more_than_one_movie_per_day INT, 
	best_staff_name VARCHAR, 
	best_staff_rental_count INT
);

-- TOTAL RENTALS - TOTAL REVENUE - MAX & MIN PAID AMOUNT:
DROP TABLE IF EXISTS TEMP_TOTAL_RENTAL_REVENUE;
CREATE TEMPORARY TABLE TEMP_TOTAL_RENTAL_REVENUE AS (
	SELECT
		CAST(se_rental.rental_date AS DATE) AS rental_day, 
		COUNT(se_rental.rental_id) AS total_rentals, 
		COALESCE(SUM(se_payment.amount), 0) AS total_revenue,
		MAX(se_payment.amount) AS max_paid_amount,
		MIN(se_payment.amount) AS min_paid_amount,
		LAG(COUNT(se_rental.rental_id), 1, 0) OVER (ORDER BY CAST(se_rental.rental_date AS DATE)) AS previous_day,
        COALESCE(COUNT(se_rental.rental_id) - LAG(COUNT(se_rental.rental_id), 1) OVER (ORDER BY CAST(se_rental.rental_date AS DATE)), 0) AS rental_difference
	FROM public.rental AS se_rental
	LEFT OUTER JOIN public.payment AS se_payment 
		ON se_rental.rental_id = se_payment.rental_id
	GROUP BY 	
		CAST(rental_date AS DATE)
);

-- RENTAL DURATION STATS
DROP TABLE IF EXISTS TEMP_RENTAL_DURATION_STATS;
CREATE TEMPORARY TABLE TEMP_RENTAL_DURATION_STATS AS (
	SELECT
		CAST(se_rental.rental_date AS DATE) AS rental_day, 
		count(se_Rental.rental_id) AS total_rentals,
		ROUND(AVG(EXTRACT(DAY FROM return_date - rental_date)),2) AS avg_rental_duration,
		SUM(
		CASE 
			WHEN (se_film.rental_duration) = EXTRACT(DAY FROM return_date - rental_date)
			THEN 1
			ELSE 0
		END 
		)AS returned_on_time,
		SUM(
		CASE 
			WHEN (se_film.rental_duration) < EXTRACT(DAY FROM return_date - rental_date)
			THEN 1
			ELSE 0
		END 
		) AS late_returns
	FROM public.rental AS se_rental
	INNER JOIN public.inventory AS se_inventory
		ON se_inventory.inventory_id = se_rental.inventory_id
	INNER JOIN public.film AS se_film
		ON se_film.film_id = se_inventory.film_id
	GROUP BY
		CAST(se_rental.rental_date AS DATE)
	);


-- CATEGORY WITH MOST RENTALS: 
DROP TABLE IF EXISTS TEMP_MOST_RENTED_CATEG;
CREATE TEMPORARY TABLE TEMP_MOST_RENTED_CATEG AS (
WITH CTE_MOST_RENTED_CATEG AS ( 
SELECT 
	CAST(se_rental.rental_date as DATE) AS rental_day,
	se_category.name AS most_rented_category, 
	COUNT(se_rental.rental_id) AS total_rentals_per_cat, 
	ROW_NUMBER() OVER (PARTITION BY CAST(se_rental.rental_date AS DATE) 
					   ORDER BY COUNT(se_rental.rental_id) DESC) AS category_rank
FROM public.category AS se_category
INNER JOIN public.film_category AS se_film_category
    ON se_film_category.category_id = se_category.category_id
INNER JOIN public.inventory AS se_inventory
    ON se_inventory.film_id = se_film_category.film_id
INNER JOIN public.rental AS se_rental
    ON se_rental.inventory_id = se_inventory.inventory_id
GROUP BY
	CAST(se_rental.rental_date as DATE),
	se_category.name
)
SELECT rental_day, most_rented_category, total_rentals_per_cat 
FROM CTE_MOST_RENTED_CATEG
WHERE category_rank = 1
);

-- CATEGORY WITH LEAST RENTALS: 
DROP TABLE IF EXISTS TEMP_LEAST_RENTED_CATEG;
CREATE TEMPORARY TABLE TEMP_LEAST_RENTED_CATEG AS (
WITH CTE_MOST_RENTED_CATEG AS ( 
SELECT 
	CAST(se_rental.rental_date AS DATE) AS rental_day,
	se_category.name AS least_rented_category, 
	COUNT(se_rental.rental_id) as total_rentals_per_least_cat, 
	ROW_NUMBER() OVER (PARTITION BY CAST(se_rental.rental_date AS DATE) 
					   ORDER BY COUNT(se_rental.rental_id)ASC) AS category_rank
FROM public.category AS se_category
INNER JOIN public.film_category AS se_film_category
    ON se_film_category.category_id = se_category.category_id
INNER JOIN public.inventory AS se_inventory
    ON se_inventory.film_id = se_film_category.film_id
INNER JOIN public.rental AS se_rental
    ON se_rental.inventory_id = se_inventory.inventory_id
GROUP BY
	CAST(se_rental.rental_date as DATE),
	se_category.name
)
SELECT rental_day, least_rented_category, total_rentals_per_least_cat 
FROM CTE_MOST_RENTED_CATEG
WHERE category_rank = 1
);

-- MOST RENTED MOVIE PER DAY
DROP TABLE IF EXISTS TEMP_MOST_RENTED_MOVIE;
CREATE TEMPORARY TABLE TEMP_MOST_RENTED_MOVIE AS (
WITH CTE_MOST_RENTED_MOVIE AS ( 
SELECT 
	CAST(se_rental.rental_date AS DATE) AS rental_day,
	se_film.title AS most_rented_movie, 
	COUNT(se_rental.rental_id) AS total_rentals_per_movie, 
	ROW_NUMBER() OVER (PARTITION BY CAST(se_rental.rental_date AS DATE) 
					   ORDER BY COUNT(se_rental.rental_id) DESC) AS movies_rank
FROM public.film AS se_film
INNER JOIN public.inventory AS se_inventory
    ON se_inventory.film_id = se_film.film_id
INNER JOIN public.rental AS se_rental
    ON se_rental.inventory_id = se_inventory.inventory_id
GROUP BY
	CAST(se_rental.rental_date AS DATE),
	se_film.title
)
SELECT rental_day, most_rented_movie 
FROM CTE_MOST_RENTED_MOVIE 
WHERE movies_rank = 1
);


-- City with the greatest number of customers doing transactions
DROP TABLE IF EXISTS TEMP_CITY_MOST_CUSTOMERS;
CREATE TEMPORARY TABLE TEMP_CITY_MOST_CUSTOMERS AS (
WITH CTE_CITY_MOST_CUSTOMERS AS (
	SELECT 
		CAST(se_rental.rental_date AS DATE) AS rental_day,
		se_city.city as city_with_most_customers, 
		COUNT(se_rental.customer_id) AS total_customers, 
		ROW_NUMBER() OVER( PARTITION BY CAST(se_rental.rental_date AS DATE) 
						  ORDER BY COUNT(se_rental.customer_id) DESC) AS city_rank
	FROM public.rental AS se_rental
	INNER JOIN public.customer AS se_customer
		ON se_rental.customer_id = se_customer.customer_id
	INNER JOIN public.address AS se_address
		ON se_customer.address_id = se_address.address_id
	INNER JOIN public.city AS se_city
		ON se_address.city_id = se_city.city_id
	GROUP BY 
		CAST(se_rental.rental_date AS DATE),
		se_city.city
)
SELECT	
	CTE_CITY_MOST_CUSTOMERS.rental_day,
	CTE_CITY_MOST_CUSTOMERS.city_with_most_customers, 
	CTE_CITY_MOST_CUSTOMERS.total_customers
FROM CTE_CITY_MOST_CUSTOMERS
WHERE city_rank = 1
);

-- MOST RENTED RATINGS 
DROP TABLE IF EXISTS TEMP_MOST_RENTED_RATING;
CREATE TEMPORARY TABLE TEMP_MOST_RENTED_RATING AS (
WITH CTE_MOST_RENTED_RATING AS ( 
    SELECT
        CAST(se_rental.rental_date AS DATE) AS rental_day,
        se_film.rating AS most_rented_rating,
        COUNT(se_rental.rental_id) AS rentals_per_rating,
        ROW_NUMBER() OVER(PARTITION BY CAST(se_rental.rental_date AS DATE) 
						  ORDER BY COUNT(se_rental.rental_id) DESC) AS rating_rank
    FROM public.film AS se_film
    INNER JOIN public.inventory AS se_inventory
        ON se_inventory.film_id = se_film.film_id
    INNER JOIN public.rental AS se_rental
        ON se_rental.inventory_id = se_inventory.inventory_id
    GROUP BY
        CAST(se_rental.rental_date AS DATE),
        se_film.rating
) 
SELECT rental_day, most_rented_rating, rentals_per_rating
FROM CTE_MOST_RENTED_RATING
WHERE rating_rank = 1
);

-- TOTAL CUSTOMERS, TOTAL ACTIVE CUSTOMERS and ACTIVE CUSTOMERS PERCENTAGE
DROP TABLE IF EXISTS TEMP_CUSTOMER;
CREATE TEMPORARY TABLE TEMP_CUSTOMER AS(
WITH CTE_CUSTOMERS_STATS AS
(
    SELECT
        CAST(se_rental.rental_date AS DATE) as rental_day,
        COUNT(se_rental.customer_id) AS total_customers,
        COUNT(
				CASE 
					WHEN se_customer.active = 1 
					THEN se_customer.customer_id 
				END) AS total_active_customers
    FROM public.customer AS se_customer
    INNER JOIN public.rental AS se_rental
        ON se_customer.customer_id = se_rental.customer_id
    GROUP BY 
		CAST(se_rental.rental_date AS DATE)
)
SELECT
	rental_day,
	total_customers,
	total_active_customers,
	ROUND
	(
		CAST(total_active_customers AS NUMERIC) /
		CAST(NULLIF(total_customers,0) AS NUMERIC)*100
	,2) AS active_customers_percentage
FROM CTE_CUSTOMERS_STATS
);
-- NUMBER OF CUSTOMERS WHO MADE MORE THAN ONE RENTAL EACH DAY
DROP TABLE IF EXISTS TEMP_CUSTOMER_MULTIPLE_RENTAL;
CREATE TEMPORARY TABLE TEMP_CUSTOMER_MULTIPLE_RENTAL AS(
WITH CTE_DAILY_CUSTOMER_MOVIE_COUNTS AS (
    SELECT
        CAST(se_rental.rental_date AS DATE) AS rental_day,
        se_customer.customer_id,
        COUNT(DISTINCT se_inventory.film_id) AS movies_rented
    FROM public.rental AS se_rental
    INNER JOIN public.customer AS se_customer 
		ON se_customer.customer_id = se_rental.customer_id
    INNER JOIN public.inventory AS se_inventory 
		ON se_inventory.inventory_id = se_rental.inventory_id
    INNER JOIN public.film AS se_film 
		ON se_film.film_id = se_inventory.film_id
    GROUP BY 
		CAST(se_rental.rental_date AS DATE), 
		se_customer.customer_id
)
SELECT
    CTE_DAILY_CUSTOMER_MOVIE_COUNTS.rental_day AS rental_day, 
	COUNT(DISTINCT customer_id) AS total_customers_renting_more_than_one_movie_per_day
FROM CTE_DAILY_CUSTOMER_MOVIE_COUNTS
WHERE movies_rented > 1
GROUP BY 
	CTE_DAILY_CUSTOMER_MOVIE_COUNTS.rental_day
ORDER BY 
	CTE_DAILY_CUSTOMER_MOVIE_COUNTS.rental_day
);

-- STAFF MEMBER WITH THE BEST PERFORMANCE
DROP TABLE IF EXISTS TEMP_STAFF_PERFORMANCE;
CREATE TEMPORARY TABLE TEMP_STAFF_PERFORMANCE AS(
WITH CTE_STAFF_PERFORMANCE AS (
    SELECT
        CAST(se_rental.rental_date AS DATE) AS rental_day,
        CONCAT(se_staff.first_name,' ',se_staff.last_name) AS best_staff_name,
        COUNT(se_rental.rental_id) AS best_staff_rental_count,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(se_rental.rental_date AS DATE) 
            ORDER BY COUNT(se_rental.rental_id) DESC
        ) AS row_number_staff
    FROM public.staff AS se_staff
    LEFT OUTER JOIN public.rental AS se_rental
        ON se_staff.staff_id = se_rental.staff_id
    GROUP BY 
        rental_day, 
        best_staff_name
)
SELECT
    CTE_STAFF_PERFORMANCE.rental_day,
    CTE_STAFF_PERFORMANCE.best_staff_name,
    CTE_STAFF_PERFORMANCE.best_staff_rental_count
FROM CTE_STAFF_PERFORMANCE
WHERE CTE_STAFF_PERFORMANCE.row_number_staff = 1
ORDER BY 
    CTE_STAFF_PERFORMANCE.rental_day
);


-- NOW WE INSERT ALL OF THOSE DATA INTO THE AGG DAILY TABLE WE CREATED
INSERT INTO reporting_schema.aggregate_daily (
	rental_day, 
	total_rentals,
	rental_difference, 
	total_revenue, 
	max_paid_amount, 
	min_paid_amount, 
	avg_rental_duration,
	returned_on_time, 
	late_returns, 
	percentage_late_returns, 
	most_rented_category, 
	total_rentals_per_cat,
	least_rented_category, 
	total_rentals_per_least_cat,
	most_rented_movie, 
	city_with_most_customers, 
	most_rented_rating,
	rentals_per_rating,
	total_customers, 
	total_active_customers, 
	active_customers_perc, 
	total_customers_renting_more_than_one_movie_per_day, 
	best_staff_name, 
	best_staff_rental_count
)
SELECT 
	TEMP_TOTAL_RENTAL_REVENUE.rental_day,
	TEMP_TOTAL_RENTAL_REVENUE.total_rentals, 
	TEMP_TOTAL_RENTAL_REVENUE.rental_difference,
	TEMP_TOTAL_RENTAL_REVENUE.total_revenue, 
	COALESCE(TEMP_TOTAL_RENTAL_REVENUE.max_paid_amount, 0) as max_paid_amount, 
	COALESCE(TEMP_TOTAL_RENTAL_REVENUE.min_paid_amount, 0) as min_paid_amount, 
	TEMP_RENTAL_DURATION_STATS.avg_rental_duration, 
	TEMP_RENTAL_DURATION_STATS.returned_on_time, 
	TEMP_RENTAL_DURATION_STATS.late_returns,
	(TEMP_RENTAL_DURATION_STATS.late_returns *100 / TEMP_RENTAL_DURATION_STATS.total_rentals) AS percentage_late_returns,
	TEMP_MOST_RENTED_CATEG.most_rented_category, 
	TEMP_MOST_RENTED_CATEG.total_rentals_per_cat, 
	TEMP_LEAST_RENTED_CATEG.least_rented_category, 
	TEMP_LEAST_RENTED_CATEG.total_rentals_per_least_cat, 
	TEMP_MOST_RENTED_MOVIE.most_rented_movie,
	TEMP_CITY_MOST_CUSTOMERS.city_with_most_customers,
	TEMP_MOST_RENTED_RATING.most_rented_rating, 
	TEMP_MOST_RENTED_RATING.rentals_per_rating,
	TEMP_CUSTOMER.total_customers, 
	TEMP_CUSTOMER.total_active_customers,
	TEMP_CUSTOMER.active_customers_percentage,
	COALESCE(TEMP_CUSTOMER_MULTIPLE_RENTAL.total_customers_renting_more_than_one_movie_per_day,0) AS total_customers_renting_more_than_one_movie_per_day,
	TEMP_STAFF_PERFORMANCE.best_staff_name, 
	TEMP_STAFF_PERFORMANCE.best_staff_rental_count


FROM TEMP_TOTAL_RENTAL_REVENUE 
LEFT JOIN TEMP_RENTAL_DURATION_STATS
	ON TEMP_TOTAL_RENTAL_REVENUE.rental_day = TEMP_RENTAL_DURATION_STATS.rental_day
LEFT JOIN TEMP_MOST_RENTED_CATEG
	ON TEMP_MOST_RENTED_CATEG.rental_day = TEMP_RENTAL_DURATION_STATS.rental_day
LEFT JOIN TEMP_LEAST_RENTED_CATEG
	ON TEMP_LEAST_RENTED_CATEG.rental_day = TEMP_MOST_RENTED_CATEG.rental_day
LEFT JOIN TEMP_MOST_RENTED_MOVIE
	ON TEMP_MOST_RENTED_MOVIE.rental_day = TEMP_LEAST_RENTED_CATEG.rental_day 
LEFT JOIN TEMP_CITY_MOST_CUSTOMERS
	ON TEMP_CITY_MOST_CUSTOMERS.rental_day = TEMP_MOST_RENTED_MOVIE.rental_day
LEFT JOIN TEMP_MOST_RENTED_RATING 
	ON TEMP_MOST_RENTED_RATING.rental_day = TEMP_CITY_MOST_CUSTOMERS.rental_day
LEFT JOIN TEMP_CUSTOMER
	ON TEMP_CUSTOMER.rental_day = TEMP_MOST_RENTED_RATING.rental_day 
LEFT JOIN TEMP_STAFF_PERFORMANCE
	ON TEMP_STAFF_PERFORMANCE.rental_day = TEMP_CUSTOMER.rental_day
LEFT JOIN TEMP_CUSTOMER_MULTIPLE_RENTAL
	ON TEMP_CUSTOMER_MULTIPLE_RENTAL.rental_day = TEMP_STAFF_PERFORMANCE.rental_day
;


SELECT *
FROM reporting_schema.aggregate_daily