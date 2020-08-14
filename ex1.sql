SELECT 
 film.film_id AS FID, 
 film.title AS title, 
 film.description AS description, 
 category.name AS category, 
 film.rental_rate AS price, 
 film.length AS length, 
 film.rating AS rating, 
 GROUP_CONCAT(CONCAT(actor.first_name, _utf8' ', actor.last_name) SEPARATOR ', ') AS actors 
FROM 
 category LEFT JOIN 
 film_category ON 
  category.category_id = film_category.category_id 
 LEFT JOIN film ON 
  film_category.film_id = film.film_id 
 JOIN film_actor ON 
  film.film_id = film_actor.film_id 
 JOIN actor ON 
  film_actor.actor_id = actor.actor_id 
GROUP BY film.film_id; 

