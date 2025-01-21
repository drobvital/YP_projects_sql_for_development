--ЭТАП №2
--Запросы на аналитику в порядке их выполнения.

SELECT ((count(*)-count(gasoline_consumption))/count(*)::real)*100 AS nulls_percentage_gasoline_consumption
FROM car_shop.models;

SELECT DISTINCT(b.brand_name),EXTRACT(year FROM s.date) AS year,round(avg(s.price),2) AS price_avg
FROM car_shop.sales AS s
LEFT JOIN car_shop.brands AS b ON s.brand_id=b.brand_id
GROUP BY b.brand_name,EXTRACT(year FROM s.date)
ORDER BY b.brand_name,EXTRACT(year FROM s.date);

SELECT EXTRACT(MONTH FROM date) AS month,EXTRACT(year FROM date) AS year,round(avg(price),2) AS price_avg
FROM car_shop.sales
WHERE EXTRACT(year FROM date)=2022
GROUP BY EXTRACT(MONTH FROM date),EXTRACT(year FROM date) 
ORDER BY EXTRACT(month FROM date);

SELECT p.person_name AS person,string_agg((b.brand_name||' '||m.model_name),', ') AS cars
FROM car_shop.persons AS p
JOIN car_shop.sales AS s ON p.person_id=s.person_id
JOIN car_shop.brands AS b ON s.brand_id=b.brand_id
JOIN car_shop.models AS m ON s.model_id=m.model_id
GROUP BY p.person_name
ORDER BY p.person_name;

SELECT bo.brand_origin_name,max(price/((100-discount::real)/100)) AS price_max,min(price/((100-discount::real)/100)) AS price_min
FROM car_shop.sales AS s
JOIN car_shop.models as m on m.model_id = s.model_id	
JOIN car_shop.brands AS b ON m.brand_id=b.brand_id
JOIN car_shop.brands_origin AS bo ON b.brand_origin_id=bo.origin_id
GROUP BY bo.brand_origin_name;

SELECT count(person_id) AS persons_from_usa_count
FROM car_shop.persons
WHERE split_part(substr(phone,strpos(phone,'+')+1),'-',1)='1';

-- Команда COPY для заполнения таблицы с сырыми данными из csv-файла.
COPY raw_data.sales(id,auto,gasoline_consumption,price,date,person,phone,discount,brand_origin)
FROM 'C:\Temp\cars.csv' 
WITH CSV HEADER NULL 'null';
