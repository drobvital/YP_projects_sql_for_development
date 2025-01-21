--ЭТАП №2
 --Аналитическая часть,составление запросов
 
CREATE VIEW top_3 AS
 WITH s AS (
 	SELECT r.rest_name AS название,r.rest_type AS тип,round(avg(s.avg_check),2) AS средний_чек,
 	ROW_NUMBER() OVER (PARTITION BY r.rest_type ORDER BY avg(s.avg_check) DESC) AS rnk
 	FROM cafe.sales s 
    JOIN cafe.restaurants r ON s.restaurant_uuid = r.restaurant_uuid
    GROUP BY r.rest_name,r.rest_type
)
SELECT s.название,s.тип,s.средний_чек
FROM s
WHERE rnk<4;
 
CREATE MATERIALIZED VIEW изменение_чека AS
WITH s AS
    (SELECT EXTRACT(YEAR FROM date) AS year,r.rest_name AS name,r.rest_type AS type,round(avg(avg_check),2) AS avg_check
     FROM cafe.sales s
 	JOIN cafe.restaurants r ON s.restaurant_uuid = r.restaurant_uuid	
 	GROUP BY EXTRACT(YEAR FROM date),r.rest_name,r.rest_type
 	ORDER BY year)
 SELECT s.year, s.name, s.type, s.avg_check AS чек_этот_год, lag(s.avg_check) OVER (PARTITION BY s.name ORDER BY s.year) AS чек_прошлый_год,
 (round(s.avg_check::numeric/lag(s.avg_check) OVER (PARTITION BY s.name ORDER BY s.year),2)-1)*100 AS "прирост в %"
 FROM s
 WHERE year<2023;

SELECT r.rest_name,count(DISTINCT manager_uuid) AS cnt
FROM cafe.restaurant_manager_work_dates rm
JOIN cafe.restaurants r ON rm.restaurant_uuid=r.restaurant_uuid
GROUP BY r.rest_name
ORDER BY cnt DESC
LIMIT 3;
 
WITH pr AS(
 SELECT name, count(menu) AS cnt,DENSE_RANK() OVER (ORDER BY count(menu) DESC) AS rnk
 FROM (SELECT rest_name AS name,jsonb_each_text(menu->'Пицца') as menu
 FROM cafe.restaurants
 WHERE rest_type='pizzeria') sub
 GROUP BY name)
SELECT pr.name,pr.cnt
FROM pr
WHERE rnk=1;
 
WITH pr AS(
 SELECT rest_name,dish_type,name,price,ROW_NUMBER() OVER (PARTITION BY rest_name ORDER BY price DESC) AS rnk
 FROM (SELECT rest_name,'Пицца' AS dish_type,rest_type,(jsonb_each_text(menu->'Пицца')).key as name,
 (jsonb_each_text(menu->'Пицца')).value::int AS price,RANK() OVER (PARTITION BY rest_name ORDER BY (jsonb_each_text(menu->'Пицца')).value::int DESC) AS rnk
 FROM cafe.restaurants) sb )
SELECT pr.rest_name,pr.dish_type,pr.name,pr.price
FROM pr
WHERE rnk=1	
ORDER BY pr.rest_name;
 
SELECT r_1.rest_name,r_2.rest_name,r_1.rest_type,
round(st_distance(r_1.location::geography,r_2.location::geography)) as distance
FROM cafe.restaurants r_1,cafe.restaurants r_2
WHERE round(st_distance(r_1.location::geography,r_2.location::geography))>0 AND r_1.rest_type=r_2.rest_type
ORDER BY distance
LIMIT 1;	
 
WITH max_rest AS(
	SELECT d.district_name AS name,count(r.rest_name) AS max_cnt
 FROM cafe.districts AS d 
 join cafe.restaurants AS r ON ST_Within(r.location,d.district_geom)
 GROUP BY d.district_name
 ORDER BY max_cnt DESC
 LIMIT 1),
 min_rest AS(
 SELECT d.district_name AS name_2,count(r.rest_name) as min_cnt
 FROM cafe.districts AS d 
 JOIN cafe.restaurants AS r ON ST_Within(r.location,d.district_geom)
 GROUP BY d.district_name
 ORDER BY min_cnt 
 LIMIT 1
 )
 SELECT name AS district_name, max_cnt AS quantity
 FROM max_rest
 UNION ALL
 SELECT name_2, min_cnt
 FROM min_rest;
 


 