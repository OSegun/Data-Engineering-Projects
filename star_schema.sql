CREATE TABLE dimDate
(
	date_key 	integer NOT NULL PRIMARY KEY,
	date     	date NOT NULL,
	year 	 	smallint NOT NULL,
	quater 	 	smallint NOT NULL,
	month 	 	smallint NOT NULL,
	day  	 	smallint NOT NULL,
	week 	 	smallint NOT NULL,
	is_weekend  boolean
	
);


INSERT INTO dimDate
(date_key, date, year, quater, month, day, week, is_weekend)
SELECT 
		DISTINCT(TO_CHAR(payment_date :: DATE, 'yyyMMDD')::integer) AS date_key,
		date(payment_date) AS date,
		EXTRACT(year from payment_date) AS year,
		EXTRACT(quarter from payment_date) AS quarter,
		EXTRACT(month from payment_date) AS month,
		EXTRACT(day from payment_date) AS day,
		EXTRACT(week from payment_date) AS week,
		CASE WHEN EXTRACT(ISODOW FROM payment_date) IN (6, 7) THEN True Else False END AS is_weekend 
FROM payment;


SELECT * FROM information_schema.columns where table_name = 'dimdate'


CREATE TABLE dimCustomer
(

	customer_key SERIAL PRIMARY KEY,
	customer_id  smallint NOT NULL,
	first_name 	 varchar(45) NOT NULL,
	last_name 	 varchar(45) NOT NULL,
	email 		 varchar(50),
	address 	 varchar(50) NOT NULL,
	address2 	 varchar(50),
	district 	 varchar(20) NOT NULL,
	city 		 varchar(50) NOT NULL,
	country 	 varchar(50) NOT NULL,
	postalcode 	 varchar(10),
	phone 		 varchar(20) NOT NULL,
	active 		 smallint NOT NULL,
	create_date  timestamp NOT NULL,
	start_date 	 date NOT NULL,
	end_date 	 date NOT NULL
);

INSERT INTO dimCustomer
(customer_key, customer_id, first_name, last_name, email, address, address2, district,
 city, country, postalcode, phone, active, create_date, start_date, end_date)
SELECT  c.customer_id AS customer_key,
		c.customer_id,
		c.first_name,
		c.last_name,
		c.email,
		a.address,
		a.address2,
		a.district,
		ci.city,
		co.country,
		a.postal_code,
		a.phone,
		c.active,
		c.create_date,
		now() AS start_date,
		now() AS end_date
FROM customer c
JOIN address a ON (c.address_id = a.address_id)
JOIN city ci ON (a.city_id = ci.city_id)
JOIN COUNTRY co ON (ci.country_id = co.country_id);

CREATE TABLE dimMovie
(

	movie_key 	 	 SERIAL PRIMARY KEY,
	film_id   	 	 smallint NOT NULL,
	title 	  	 	 varchar(255) NOT NULL,
	description  	 text,
	release_year 	 year,
	language 	 	 varchar(20) NOT NULL,
	original_langage varchar(20),
	rental_duration  smallint NOT NULL,
	length 			 smallint NOT NULL,
	rating 			 varchar(5) NOT NULL,
	special_features varchar(60) NOT NULL
);

INSERT INTO dimMovie
(movie_key, film_id, title, description, release_year, language, original_langage,
rental_duration, length, rating, special_features)
SELECT f.film_id AS moive_key,
		f.film_id,
		f.title,
		f.description,
		f.release_year,
		f.language_id,
		f.language_id,
		f.rental_duration,
		f.length,
		f.rating,
		f.special_features
FROM film f;

SELECT fulltext from film limit 5;

CREATE TABLE dimStore
(

	store_key 			 SERIAL PRIMARY KEY,
	store_id  		 	 smallint NOT NULL,
	address 	 		 varchar(50) NOT NULL,
	address2 	 		 varchar(50),
	district 	 		 varchar(20) NOT NULL,
	city 		 		 varchar(50) NOT NULL,
	country 	 		 varchar(50) NOT NULL,
	postalcode 	 		 varchar(10),
	manager_first_name 	 varchar(45) NOT NULL,
	manager_last_name 	 varchar(45) NOT NULL,
	start_date 	 		 date NOT NULL,
	end_date 	 		 date NOT NULL
);

INSERT INTO dimStore
(store_key, store_id, address, address2, district, city, country, postalcode, 
 manager_first_name, manager_last_name, start_date, end_date)
SELECT s.store_id AS store_key,
		s.store_id,
		a.address,
		a.address2,
		a.district,
		ci.city,
		co.country,
		a.postal_code,
		st.first_name,
		st.last_name,
		now() AS start_date,
		now() AS end_date
FROM store s
JOIN address a ON (s.address_id = a.address_id)
JOIN staff st ON (s.manager_staff_id = st.staff_id)
JOIN city ci ON (a.city_id = ci.city_id)
JOIN COUNTRY co ON (ci.country_id = co.country_id);

CREATE TABLE factSales
(

	sales_key SERIAL PRIMARY KEY,
	date_key integer REFERENCES dimDate (date_key),
	customer_key integer REFERENCES dimCustomer (customer_key),
	movie_key integer REFERENCES dimMovie (movie_key),
	store_key integer REFERENCES dimStore (store_key),
	sales_amount numeric
);


INSERT INTO factSales
(date_key, customer_key, movie_key, store_key, sales_amount)

SELECT 
		TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer AS date_key,
		p.customer_id as customer_key,
		i.film_id as movie_key,
		i.store_id as store_key,
		p.amount as sales_amount
FROM payment p
JOIN rental r ON (p.rental_id = r.rental_id)
JOIN inventory i ON (r.inventory_id = i.inventory_id);
