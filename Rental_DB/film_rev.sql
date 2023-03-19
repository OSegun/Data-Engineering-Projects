/* Query for filmm revenue */
SELECT f.title AS FILM_TITLE, SUM(p.amount) AS  FILM_REVENUE FROM payment p
JOIN rental r ON p.rental_id = r.rental_id
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
GROUP BY f.title
ORDER BY SUM(p.amount) DESC