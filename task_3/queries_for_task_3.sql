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



/* 5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. 
      Если у нескольких актеров одинаковое кол-во фильмов, вывести всех. */

WITH top_children_actors AS(
			SELECT 
				actor.actor_id,
				actor.first_name,
				actor.last_name,
				COUNT(*) AS count_films
			FROM
				actor
				JOIN film_actor USING(actor_id)
				JOIN film_category USING(film_id)
				JOIN category USING(category_id)
			WHERE 
				category.name = 'Children'
			GROUP BY
				actor.actor_id
			ORDER BY
				count_films DESC
	),
	top_counters AS(
		SELECT 
			DISTINCT count_films AS counter
		FROM 
			top_children_actors
		ORDER BY
			1 DESC
		LIMIT 3
	)
	
SELECT
	*
FROM
	top_children_actors
WHERE 
	top_children_actors.count_films IN (SELECT counter FROM top_counters);



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

WITH film_full AS(
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
            f.name AS film_category,
            SUM(EXTRACT(HOUR FROM (r.return_date - r.rental_date))) AS total_rental_hours
     FROM 
 	  city c
          JOIN address a USING(city_id)
          JOIN customer cu USING(address_id)
          JOIN rental r USING(customer_id)
          JOIN inventory i USING(inventory_id)
          JOIN film_full f USING(film_id)
     WHERE 
	  (c.city LIKE 'a%' OR c.city LIKE '%-%')
     GROUP BY 
	   c.city, f.name
     ORDER BY
	   c.city, total_rental_hours DESC
     )

SELECT 
	city_name,
	film_category,
	total_rental_hours
FROM 
	max_rental_hours m
WHERE 
	(m.city_name, m.total_rental_hours) IN (
     	SELECT 
		city_name, 
		MAX(total_rental_hours)
     	FROM
		max_rental_hours
     	GROUP BY
		city_name
     	)
ORDER BY 
	city_name;