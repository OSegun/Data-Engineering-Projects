/* Query for filmm revenue */
SELECT c.city, SUM(p.amount) AS CITY_REVENUE FROM payment p
JOIN staff s ON p.staff_id = s.staff_id
JOIN address a ON s.address_id = a.address_id
JOIN city c ON a.city_id = c.city_id
GROUP BY c.city
ORDER BY SUM(p.amount) DESC