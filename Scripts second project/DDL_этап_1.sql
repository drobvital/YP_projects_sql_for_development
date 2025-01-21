--======== ОСНОВНАЯ ЧАСТЬ ==============
 
--ЭТАП №1

--Создание таблиц и заполнение данными
 
CREATE TYPE restaurant_type AS enum('coffee_shop','restaurant','bar','pizzeria');
 
CREATE TABLE cafe.restaurants(
 restaurant_uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 	rest_name varchar(50) UNIQUE NOT NULL,
 	location geometry(POINT,4326),
 	rest_type restaurant_type,
 	menu jsonb
 	);
 

INSERT INTO cafe.restaurants(rest_name,location,rest_type,menu)
 SELECT DISTINCT s.cafe_name,
    ST_point(s.latitude, s.longitude),
 	s.type::restaurant_type,
 	m.menu::jsonb
 FROM raw_data.sales s 
LEFT JOIN raw_data.menu m ON s.cafe_name = m.cafe_name;
 
CREATE TABLE IF NOT EXISTS cafe.managers(
 manager_uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 	manager_name varchar(50) UNIQUE NOT NULL,
	phone varchar(50)
 );
 
INSERT INTO cafe.managers(manager_name,phone)
SELECT DISTINCT manager,manager_phone
FROM raw_data.sales;
 
CREATE TABLE cafe.restaurant_manager_work_dates(
 restaurant_uuid uuid REFERENCES cafe.restaurants(restaurant_uuid),
 manager_uuid uuid REFERENCES cafe.managers(manager_uuid),
 start_date date,
 last_date date,
 PRIMARY KEY(restaurant_uuid,manager_uuid)
 	);
 
INSERT INTO cafe.restaurant_manager_work_dates(restaurant_uuid,manager_uuid,start_date,last_date)
 SELECT r.restaurant_uuid,m.manager_uuid,min(s.report_date),max(s.report_date)
 FROM raw_data.sales s 
 JOIN cafe.restaurants r ON s.cafe_name=r.rest_name
 JOIN cafe.managers m ON s.manager=m.manager_name 
 GROUP BY r.restaurant_uuid,m.manager_uuid;
 
CREATE TABLE cafe.sales(
 	date date,
restaurant_uuid uuid references cafe.restaurants(restaurant_uuid),
 	avg_check numeric(6,2),
 	PRIMARY KEY (date, restaurant_uuid)
 );
 
INSERT INTO cafe.sales (date,restaurant_uuid,avg_check)
SELECT s.report_date,r.restaurant_uuid,s.avg_check
 FROM raw_data.sales s
 JOIN cafe.restaurants r ON s.cafe_name=r.rest_name;