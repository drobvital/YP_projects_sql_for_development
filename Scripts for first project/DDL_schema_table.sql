CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.sales(
	id smallint PRIMARY key,
	auto text,
	gasoline_consumption real,
	price NUMERIC(9,2),
	date date,
	person varchar(100),
	phone varchar(40),
	discount integer,
	brand_origin varchar(20)
    );

CREATE SCHEMA IF NOT EXISTS car_shop;

CREATE TABLE IF NOT EXISTS car_shop.colors(
	color_id serial PRIMARY KEY,
	color_name VARCHAR(20) UNIQUE -- не существует названий цветов длиной более 20 символов, поэтому выбираем varchar(20),UNIQUE-необходимы только уникальные значения,без дубликатов	
    );

CREATE TABLE IF NOT EXISTS car_shop.brands_origin(
	origin_id serial PRIMARY KEY,
	brand_origin_name VARCHAR(20) UNIQUE -- не существует названий стран длиной более 20 символов,необходимы только уникальные значения	
    );

CREATE TABLE IF NOT EXISTS car_shop.brands(
	brand_id serial PRIMARY KEY,
	brand_name VARCHAR(50) UNIQUE, -- не существует названий брендов длиной более 50 символов, в названии бренда могут быть и цифры, и буквы, поэтому выбираем varchar(50) и только уникальные значения	
    brand_origin_id integer REFERENCES car_shop.brands_origin
    );

CREATE TABLE IF NOT EXISTS car_shop.models(
	model_id serial PRIMARY key,
	model_name VARCHAR(50) UNIQUE, -- не существует названий моделей длиной более 50 символов, в названии бренда могут быть и цифры, и буквы, поэтому выбираем varchar(50).Ограничение UNIQUE,чтобы попали только уникальные значения	
    gasoline_consumption real,
	brand_id INTEGER REFERENCES car_shop.brands
	);
   
CREATE TABLE IF NOT EXISTS car_shop.persons(
	person_id serial PRIMARY KEY,
	person_name VARCHAR(50) NOT NULL UNIQUE, -- не существует фио длиной более 50 символов,данные не должны дублироваться и содержать пропуски 
    phone VARCHAR (40) -- контакты удобнее хранить в текстовом файле(никаких математических функций с этими данными делать не нужно,хранится как информация),значения не превышают 40 символов, 
	);

CREATE TABLE IF NOT EXISTS car_shop.sales(
	sales_id serial PRIMARY key,
    model_id INTEGER REFERENCES car_shop.models,
	color_id integer REFERENCES car_shop.colors,	
    date date,
	person_id integer REFERENCES car_shop.persons,
	discount integer,-- используем этот тип,потому что в данных целочисленное значение,также можно использовать данные в агрегирующих функциях
	price numeric(9,2) --данный тип с заданной точностью используют для точных растчетов,в том числе денежных средств 
	);

--Запросы на заполнение таблиц данными.

INSERT INTO car_shop.colors(color_name)
SELECT DISTINCT split_part(auto,',',2)
FROM raw_data.sales;

INSERT INTO car_shop.brands_origin(brand_origin_name)
SELECT DISTINCT brand_origin
FROM raw_data.sales;

INSERT INTO car_shop.brands(brand_name,brand_origin_id)
SELECT DISTINCT split_part(s.auto,' ',1),b.origin_id
FROM raw_data.sales as s
LEFT JOIN car_shop.brands_origin as b on s.brand_origin=b.brand_origin_name;

INSERT INTO car_shop.models(model_name,gasoline_consumption,brand_id)
SELECT DISTINCT (split_part(substr(s.auto,strpos(s.auto,' ')+1),',',1)),s.gasoline_consumption,b.brand_id
FROM raw_data.sales AS s
JOIN car_shop.brands AS b ON split_part(s.auto,' ',1)=b.brand_name;


INSERT INTO car_shop.persons(person_name,phone)
SELECT DISTINCT person,phone
FROM raw_data.sales;

INSERT INTO car_shop.sales(model_id,color_id,date,person_id,discount,price)
SELECT m.model_id,c.color_id,s.date,p.person_id,s.discount,s.price
FROM raw_data.sales AS s
LEFT JOIN car_shop.models m ON split_part(substr(s.auto,strpos(s.auto,' ')+1),',',1)=m.model_name
LEFT JOIN car_shop.brands b ON split_part(s.auto,' ',1)=b.brand_name	
LEFT JOIN car_shop.colors c ON split_part(s.auto,',',2)=c.color_name
LEFT JOIN car_shop.persons p ON s.person=p.person_name
WHERE trim(b.brand_name || ' ' || m.model_name || ',' || c.color_name) = TRIM(s.auto);	
