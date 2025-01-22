
--======== ОСНОВНАЯ ЧАСТЬ ==============

--Создание процедур, функций и триггеров

--Задание 1.
1.	explain ANALYSE
INSERT INTO orders
    (order_id, order_dt, user_id, device_type, city_id, total_cost, discount, 
    final_cost)
SELECT MAX(order_id) + 1, current_timestamp, 
    '329551a1-215d-43e6-baee-322f2467272d', 
    'Mobile', 1, 1000.00, null, 1000.00
FROM orders;
--при изучении статистики анализа запроса выше, стоимость 0.35 и время выполнения 0.238.   В нашем случае запрос на вставку данных, поэтому анализируем что может тормозить вставку.. Проверим какие индексы есть в таблице запросом SELECT * FROM pg_stat_user_indexes. На одну таблицу созданы 10 индексов,почти на каждый столбец. По колонкам idx_scan,idx_tup_read видно что из всех индексов используются только три: orders_user_id_idx(колонка user_id),orders_order_id_idx(order_id),orders_order_dt_idx(order_dt).  Изучая популярные запросы к этой таблице,эти 3 колонки используются чаще всех. Остальные индексы можно удалить: 
DROP INDEX public.orders_device_type_idx;
DROP INDEX public.orders_discount_idx;
DROP INDEX public.orders_final_cost_idx;
DROP INDEX public.orders_total_cost_idx;
DROP INDEX public.orders_total_final_cost_discount_idx;  
По структуре таблицы: необходимо поменять тип данных в столбце order_id(c bigint на int),также на этот столбец установить автоинкремент, чтобы не делать в запросе лишние вычисления, удалить столбец city_id,так как в таблице есть данные по user_id;поменять тип данных в колонке discount numeric(8,2),поскольку максимальные и минимальные значения известны, изменить тип данных столбца device_type  varchar(7),поскольку значения в столбце с фиксированной длиной.
CREATE sequence orders_order_id_seq OWNED by orders.order_id;
ALTER TABLE orders ALTER COLUMN order_id set default nextval('orders_order_id_seq');
SELECT setval('orders_order_id_seq',coalesce(max(order_id),0)+1) from orders; 
alter table orders drop column city_id;
alter table orders alter column discount type numeric(8,2);
alter table orders alter column order_id type integer;
alter table orders alter column device_type type varchar(7);
--Создадим триггер для столбца order_dt,для того чтобы не вычислять  и каждый раз подставлять текущую метку,тем самым экономия времени и ресурсов.
CREATE OR REPLACE FUNCTION set_order_dt_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.order_dt := current_timestamp;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- Создание триггера для таблицы orders
CREATE TRIGGER set_order_dt_timestamp
BEFORE INSERT ON orders
FOR EACH ROW
EXECUTE FUNCTION set_order_dt_timestamp();
--После всех преобразований запрос выглядит так:  insert into orders(user_id,device_type,total_cost,discount,final_cost)
values('329551a1-215d-43e6-baee-322f2467272d', 
    'Mobile', 1000.00, null, 1000.00); --cost=0.01,actual time 0.108

--Задание 2.	
 --явные проблемы в структуре таблицы, не правильно подобраны типы данных, 
--в связи с чем нужно выбрать более подходящие форматы, 
--тем самым избавиться от приведения типов в select  и убрать преобразования в where.
alter table users alter column user_id type uuid using user_id::text::uuid; 
alter table users alter column last_name(first_name) type varchar(50); 
alter table users add column full_name varchar(100);
update users 
set full_name=concat(last_name,' ',first_name);
alter table users
drop column first_name,
drop column last_name;
alter table users alter column city_id type smallint;
alter table users alter column gender type varchar(6);
alter table users alter column birth_date type date using birth_date::date;
alter table users alter column registration_date type timestamptz using registration_date::timestamptz;
--Просмотрев с помощью запроса SELECT * FROM pg_stat_user_indexes; наличие индексов на таблицу users и в виду их отсутствия добавил 2 индекса по полям user_id и city_id.
create UNIQUE index users_user_id_idx on users(user_id);
create index users_city_id_idx on users(city_id);
--Итог: первоначальный анализ запроса показывал cost=2813,actual time 14.370; после всех преобразований и добавлений индексов cost=156.21 actual time = 0.334; 


--Задание 3.	
--При изучении в консоли кода процедуры add_payment выяснилось,что основная работа процедуры заключается в добавлении новых строк в таблицы: order_statuses,payments и sales.
--Для возможного ускорения работы процедуры, после анализа структур таблиц на соответствие типов данных и наличию индексов, изменил несколько типов данных,также добавил индексы в таблицах. При изучении таблицы sales,пришел к выводу,что данная таблица содержит избыточные данные. Поле sale_dt из таблицы sales это поле status_dt в таб. order_statuses c order_id=2.  Полe sale_sum это поле в таблице payments payment_sum. Данную таблицу можно удалить.
alter table order_statuses alter column order_id type integer;
alter table order_statuses alter column status_id type smallint;
create index order_statuses_order_id_idx on order_statuses(order_id);
alter table payments alter column payment_id type integer;
alter table payments alter column order_id type integer;
create index payments_payment_id_idx on payments(payment_id);
create index payments_order_id_idx on payments(order_id);
drop table sales;
--Исправленный код процедуры
create or replace procedure add_payment(p_order_id integer,p_sum_payment numeric)
LANGUAGE sql
as $$
	insert into order_statuses(order_id,status_id,status_dt)
	values(p_order_id,2,statement_timestamp());

	insert into payments(payment_id,order_id,payment_sum)
	values(nextval('payments_payment_id_sq'),p_order_id,p_sum_payment);
  $$;


--Задание 4.
--Данные для анализа используются поквартально,можно партицировать таблицу logs по временной метке(кварталы),
--что позволит ускорить процесс внесения и получения данных. 
--Прежде изменим некоторые типы данных на более соответствующие и добавим индексы.
--Индекс по полю datetime уже есть в базе,поэтому его не трогаем.
alter table user_logs alter column visitor_uuid type uuid using visitor_uuid::text::uuid; 
alter table user_logs alter column log_id type integer;
alter table user_logs drop column log_date;
create table user_logs_part(
	visitor_uuid uuid,
	user_id uuid,
	event character varying(128),
	datetime timestamp without time zone,
	log_id integer not null,
	primary key(log_id,datetime)
) PARTITION by range(datetime);
CREATE TABLE logs_q1 PARTITION OF user_logs_part
    FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-04-01 00:00:00');
CREATE TABLE logs_q2 PARTITION OF user_logs_part
    FOR VALUES FROM ('2021-04-01 00:00:00') TO ('2021-07-01 00:00:00');
CREATE TABLE logs_q3 PARTITION OF user_logs_part
	FOR VALUES FROM ('2021-07-01 00:00:00') TO ('2021-10-01 00:00:00');
CREATE TABLE logs_q4 PARTITION OF user_logs_part
	FOR VALUES FROM ('2021-10-01 00:00:00') TO ('2022-01-01 00:00::00');
--можно будет в партицированных частях дополнительно создать индексы. 
--мое решение взято по примеру из теории. Подскажите какие дальнейшие шаги? 
--В теории ничего не сказано про наполнение данных уже партцированных частей и основной таблицы, 
--создание которой мы дублируем,используя декларативное партицирование.


--Задание 5.
--Оптимизировать запрос в этом задании поможет структура CTE
--CTE вычисляется один раз и сохраняет результат до окончания выполнения запроса, 
--поэтому оно позволяет оптимизировать как вычислительные ресурсы, так и чтение данных. 
WITH t1 AS ( 
 SELECT object_id, 
 spicy, 
 fish, 
 meat 
 FROM dishes),  
t2 AS ( 
 SELECT order_id, 
 SUM(t1.spicy*oi.count) AS spicy,  
 SUM(t1.fish*oi.count) AS fish,  
 SUM(t1.meat*oi.count) AS meat, 
 SUM(oi.count) AS total 
 FROM order_items AS oi 
 INNER JOIN t1 ON oi.item = t1.object_id 
 GROUP BY order_id 
),
t3 AS ( 
 SELECT  
 t2.spicy as sp, 
 t2.fish as f,  
 t2.meat as m ,  
 t2.total as t, 
 o.user_id,  
 o.order_dt::date 
 FROM orders AS o 
 INNER JOIN t2 ON o.order_id = t2.order_id 
)
 
SELECT t3.order_dt AS day, 
 CASE 
 when DATE_PART('year', AGE(t3.order_dt::date, u.birth_date::date)) >= 0 and DATE_PART('year', AGE(t3.order_dt::date, u.birth_date::date)) < 21 
 then '0-20' 
 when DATE_PART('year', AGE(t3.order_dt::date, u.birth_date::date)) >= 20 and DATE_PART('year', AGE(t3.order_dt::date, u.birth_date::date)) < 31 
 then '20-30' 
 when DATE_PART('year', AGE(t3.order_dt::date, u.birth_date::date)) >= 30 and DATE_PART('year', AGE(t3.order_dt::date, u.birth_date::date)) < 41 
 then '30-40' 
 when DATE_PART('year', AGE(t3.order_dt::date, u.birth_date::date)) >= 40 
 then '40-100' 
END age, 
 round(SUM(sp) / SUM(t),2) as spicy, 
 round(SUM(f) / SUM(t),2) as fish, 
 round(SUM(m) / SUM(t),2) as spicy 
FROM users AS u 
inner join t3 ON t3.user_id::text = u.user_id::text 
group by day, age 
order by DAY;
	


