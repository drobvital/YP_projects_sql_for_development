
--======== ОСНОВНАЯ ЧАСТЬ ==============

--4й проект вторая часть. Оптимизация запросов

--Задание 1.
--При изучении запросов из файла выбрал 5 самых дорогих из них:
--Запросы 2,7,8,9,15.

1. Запрос 2

explain ANALYZE
SELECT o.order_id, o.order_dt, o.final_cost, s.status_name
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
    JOIN statuses s ON s.status_id = os.status_id
WHERE o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid
	AND os.status_dt IN (
	SELECT max(status_dt)
	FROM order_statuses
	WHERE order_id = o.order_id
    );
--анализ первичного запроса показал высокую стоимость cost =33173.85 и time(время выполнения) 361.171. 
--последовательное чтение данных seq sqan таблицы order_statuses по полям  status_dt и order_id 
--составляет большую часть стоимости запроса. Последовтельное чтение происходит по несортированным данным, 
--точнее там где отсутствует индекс. 
--Запросом SELECT * FROM pg_stat_user_indexes подтвердилось отсутствие индексов в таблицe order_statuses.
--Также происходит последовательное сканирование таблицы statuses,но в таблице находятся статические 
--повторяющиеся данные. В этом случае лучше применять полное последовательное сканирование таблицы. 
--Сперва перестроим запрос и заменим подзапрос в where на подзапрос в join
SELECT o.order_id, o.order_dt, o.final_cost, s.status_name
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
    JOIN statuses s ON s.status_id = os.status_id
	join (
	SELECT max(status_dt) as dt,order_id
	FROM order_statuses
	group by order_id) max_st on max_st.order_id=o.order_id and max_st.dt=os.status_dt  
WHERE o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid;
--Анализ запроса после показал снижение стоимости cost=5839 time 121.676
-- Дополнительнр создал 2 индекса таблицы order_statuses по полям status_dt и order_id.
create index order_statuses_status_dt_idx on order_statuses(status_dt);
create index order_statuses_order_id_idx on order_statuses(order_id);
--перестроенный запрос + индекс cost=3253 time=149 
--Без перестроения запроса после создания индекса повторный анализ запроса показал значительное улучшение стоимости
--и времени: соst = 205.71,time=0.211. Самый оптимальный вариант.

--Запрос 7.	
explain analyze
SELECT event, datetime
FROM user_logs
WHERE visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
ORDER BY 2;
--Анализ показал высокую стоимость cost = 92040 time = 1119. 
--Параллельное полное сканирование всех партиций по фильтру  
--visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0',что может говорить об отстутсвии индекса
-- по полю visitor_uuid отсутствует. Проверим это запросом SELECT * FROM pg_stat_user_indexes. 
--Так и есть индекс по этому полю в основной таблице и партициях отсутствует
create index user_logs_visitor_uuid_idx on user_logs(visitor_uuid);
create index user_logs_y2021q2_visitor_uuid_idx on user_logs_y2021q2(visitor_uuid);
create index user_logs_y2021q3_visitor_uuid_idx on user_logs_y2021q3(visitor_uuid);
create index user_logs_y2021q4_visitor_uuid_idx on user_logs_y2021q4(visitor_uuid);
--после добавления индексов cost=939 time 0.388.


--Запрос 8.	
Explain analyze
SELECT *
FROM user_logs
WHERE datetime::date>CURRENT_date;
--Анализ запроса показал cost=155910 и time=1993. Происходит полное параллельное сканирование всех партиций 
--seq scan, так как в фильтре происходит вычисления и преобразования не срабатывают индексы 
--по полю datetime. В таблице user_logs есть поле log_date в формате datetime. 
--Его можно использовать в фильтре,чтобы не делать преобразования. 
--Так же изменим запрос со знака > на равенство = .Сперва установим индексы по этому полю.  
create index user_logs_log_date_idx on user_logs(log_date);
create index user_logs_y2021q2_log_date_idx on user_logs_y2021q2(log_date);
create index user_logs_y2021q3_log_date_idx on user_logs_y2021q3(log_date);
create index user_logs_y2021q4_log_date_idx on user_logs_y2021q4(log_date);
explain analyze
SELECT *
FROM user_logs
WHERE log_date=CURRENT_date;  -- cost=188.57 time 0.190 
--но самый оптимальный вариант без добавления индекса, соответственно меньшей нагрузки на память, сделать приведение к другому типу в where ни столбца таблицы  datetime ,а значения функции current_date::timestamp,что позволит снизить нагрузку на базу данных.
Explain analyze
SELECT *	
FROM user_logs
WHERE datetime>CURRENT_date::timestamp; --cost=37.71 time = 0.030



--Запрос 9.
-- анализ запроса показал очень высокую стоимость cost=60531007.47 time=1993.
--Пробую поменять подзапросы на СТЕ,тем самым избавиться от вложенного запроса в where,оптимизируя ресурсы. 
explain analyze
with ord as (
	select *
	from orders
	where city_id=1
),
ost as (
	select *
	from order_statuses
	where status_id=2
)
select count(*)
from ord o 
left join ost on o.order_id=ost.order_id
where ost.order_id is null;  --cost=3291 time=20.790 заметно улучшились покзатели,но все еще довольно высокие. 
--Происходит полное последовательное сканирование по фильтру по полям city_id и  status_id . 
--данные в этих столбцах статичны, создадим частичный индекс по этим полям.
CREATE INDEX orders_city_id_idx ON orders (city_id)
WHERE city_id = 1;
CREATE INDEX order_statuses_status_id_idx ON order_statuses(status_id)
WHERE status_id = 2;
--после создания индексов cost=1781 time 12.547;


--Запрос 15.
Explain analyze
SELECT d.name, SUM(count) AS orders_quantity
FROM order_items oi
JOIN dishes d ON d.object_id = oi.item
WHERE oi.item IN (
SELECT item
FROM (SELECT item, SUM(count) AS total_sales
FROM order_items oi
GROUP BY 1) dishes_sales
WHERE dishes_sales.total_sales > (
SELECT SUM(t.total_sales)/ COUNT(*)
FROM (SELECT item, SUM(count) AS total_sales
FROM order_items oi
GROUP BY
1) t)
)
GROUP BY 1
ORDER BY orders_quantity DESC;
--cost данного запроса = 4810 или time=133. 
--Из запроса видно что несколько раз повторяется один и тот же подзапрос и  обращение к таблице order_items. 
--Повторяющийся подзапрос лучше вынести в СТЕ.
explain analyze
WITH dishes_sales AS (
 SELECT item, SUM(count) AS total_sales
 FROM order_items
 GROUP BY item
),
total_sales_avg AS (
 SELECT AVG(total_sales) AS avg_total_sales
 FROM dishes_sales
),
filtered_dishes_sales AS (
 SELECT item, total_sales
 FROM dishes_sales
 WHERE total_sales > (SELECT avg_total_sales FROM total_sales_avg)
)
SELECT d.name, SUM(oi.count) AS orders_quantity
FROM order_items oi
JOIN dishes d ON d.object_id = oi.item
JOIN filtered_dishes_sales fds ON fds.item = oi.item
GROUP BY d.name
ORDER BY orders_quantity DESC;
--Cost снизилась до 3346 и time = 69.131 



