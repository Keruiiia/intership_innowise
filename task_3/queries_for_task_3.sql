/* 1. Вывести количество фильмов в каждой категории,
   отсортировать по убыванию. */

SELECT 
	category.name,
	COUNT(*) AS movie_count
FROM 
	category
	JOIN film_category USING (category_id)
	JOIN film USING (film_id)
GROUP BY
	category.name
ORDER BY
	movie_count DESC;



/* 2. Вывести 10 актеров, чьи фильмы большего всего арендовали,
   отсортировать по убыванию. */


SELECT 
    actor.actor_id,
    actor.first_name,
    actor.last_name,
    COUNT(rental.rental_id) AS rental_count
FROM 
    actor
    JOIN film_actor USING(actor_id)
    JOIN film USING(film_id)
    JOIN inventory USING(film_id)
    JOIN rental USING(inventory_id)
GROUP BY 
    actor.actor_id,
    actor.first_name,
    actor.last_name
ORDER BY 
    rental_count DESC
LIMIT 10;




-- 3. Вывести категорию фильмов, на которую потратили больше всего денег.

SELECT
	c.name AS category,
	SUM(f.rental_rate) AS total_spent
FROM 
	rental r
	JOIN inventory i USING(inventory_id)
	JOIN film f USING(film_id)
	JOIN film_category fc USING(film_id)
	JOIN category c USING(category_id)
GROUP BY
	c.name
ORDER BY
	total_spent DESC
LIMIT 1;



/* 4. Вывести названия фильмов, которых нет в inventory. 
   Написать запрос без использования оператора IN. */

SELECT 
	film.title
FROM 
	film
WHERE NOT EXISTS (
		  SELECT *
		  FROM inventory
		  WHERE inventory.film_id = film.film_id
		 );

-- query with left join

SELECT 
	film.title
FROM
	film
	LEFT JOIN inventory USING(film_id)
WHERE
	inventory.film_id IS NULL;



/* 5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. 
      Если у нескольких актеров одинаковое кол-во фильмов, вывести всех. */

WITH top_children_actors AS (
		SELECT
			actor.actor_id,
			actor.first_name,
			actor.last_name,
			COUNT(*) AS count_films,
			DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank
		FROM
			actor
			JOIN film_actor USING (actor_id)
			JOIN film_category USING (film_id)
			JOIN category USING (category_id)
		WHERE
			category.name = 'Children'
		GROUP BY
			1, 2, 3
		)
SELECT
	actor_id,
	first_name,
	last_name,
	count_films
FROM
	top_children_actors
WHERE
	rank <= 3;



/* 6. Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). 
      Отсортировать по количеству неактивных клиентов по убыванию. */

SELECT 
    city.city,
    COUNT(CASE WHEN customer.active = 1 THEN 1 END) AS active_customers,
    COUNT(CASE WHEN customer.active = 0 THEN 1 END) AS inactive_customers
FROM 
    address
    JOIN city USING(city_id)
    JOIN customer USING(address_id)
GROUP BY 
    city.city
ORDER BY 
    inactive_customers DESC;



/* 7. Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды
   в городах (customer.address_id в этом city), и которые начинаются на букву “a”. 
   То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе. */

WITH film_full AS (
		SELECT
			film_id,
			title,
			name
		FROM
		film
			LEFT JOIN film_category USING(film_id)
			LEFT JOIN category USING(category_id)
		),
	max_rental_hours AS (
		SELECT
		c.city AS city_name,
		CASE
			WHEN UPPER(c.city) LIKE 'A%' THEN 'Starts with A'
			WHEN UPPER(c.city) LIKE '%-%' THEN 'Has hyphen'
		END AS city_category,
		f.name AS film_category,
		SUM(DATE_PART('day', (return_date - rental_date)) * 24 + DATE_PART('hour', (return_date - rental_date))) AS total_rental_hours
		FROM
			city c
			JOIN address a USING(city_id)
			JOIN customer cu USING(address_id)
			JOIN rental r USING(customer_id)
			JOIN inventory i USING(inventory_id)
			JOIN film_full f USING(film_id)
		WHERE
			UPPER(c.city) LIKE 'A%' OR UPPER(c.city) LIKE '%-%'
		GROUP BY
		c.city, city_category, f.name
		),
	sum_rental_hours_by_city_category AS (
		SELECT
			city_category,
			film_category,
			SUM(total_rental_hours) AS max_total_rental_hours
		FROM
			max_rental_hours
		GROUP BY
			city_category, film_category
		),
	max_rental_hours_by_city AS (
		SELECT
			city_category,
			film_category,
			max_total_rental_hours,
			ROW_NUMBER() OVER (PARTITION BY city_category ORDER BY max_total_rental_hours DESC)
				AS rn
		FROM
			sum_rental_hours_by_city_category
		ORDER BY
			city_category
	)

SELECT
	city_category,
	film_category,
	max_total_rental_hours
FROM max_rental_hours_by_city
WHERE rn = 1;