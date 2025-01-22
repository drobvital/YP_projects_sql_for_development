
--======== ОСНОВНАЯ ЧАСТЬ ==============

--Создание процедур, функций и триггеров

--Задание 1.
create or replace PROCEDURE update_employees_rate(p_string JSON)
language plpgsql
as $$
DECLARE
	_id uuid;
	_rate_change INTEGER;
	_rec JSON;
	_rate_after_change integer;
 begin 
 for _rec in select json_array_elements(p_string) loop
	
	 _id:=(_rec::json->>'employee_id')::uuid;

	_rate_change:=(_rec::json->>'rate_change')::integer;

	select rate * (_rate_change+100)/100
	into _rate_after_change
	from employees
	where employees.id=_id;

	if _rate_after_change<500 then _rate_after_change:=500; end if;

	update employees set rate = _rate_after_change where employees.id=_id;
 end loop;
 end;
$$;

--Задание 2.	
create or replace procedure indexing_salary(p_index integer)
language plpgsql
as $$
DECLARE
	_avg_rate integer;
 BEGIN
 	select round(avg(rate),0)
	into _avg_rate
	from employees;

	update employees
	set rate = case 
		when rate<_avg_rate then 
		round((rate*(p_index+2+100)/100),0)
	    ELSE round(rate * (p_index+100)/100) end;
 end;
$$;

--Задание 3.	
create or replace procedure close_project(p_id uuid)
language plpgsql
as $$
declare
	_is_active BOOLEAN;
	_estimated_time integer;
	_work_hours integer;
	_count_emp integer;
	_bonus_time integer;
 begin
	select is_active
	into _is_active
	from projects
	where id=p_id;
	if not _is_active then
	raise exception 'Project closed';
	end if;
	update projects
	set is_active=false
	where id=p_id;

	select estimated_time
	into _estimated_time
	from projects
	where id=p_id;

	select sum(work_hours)
	into _work_hours
	from logs
	where logs.project_id=p_id;

	select count(distinct employee_id)
	into _count_emp	
	from logs
	where project_id=p_id;

	if _estimated_time is not null and _work_hours is not null and _estimated_time>_work_hours
	then
	_bonus_time:=floor(0.75*(_estimated_time -_work_hours)/_count_emp);
	END IF;
	EXCEPTION
    WHEN division_by_zero THEN 
        RAISE NOTICE 'Ошибка выполнения: %', SQLERRM;
	if _bonus_time>16
	then _bonus_time:=16;
	END IF;

	if _bonus_time>0
	then
	with de as (
		select distinct employee_id as id
		from logs
		where p_id=project_id)
		insert into logs (created_at,employee_id,project_id,work_date,work_hours,required_review,is_paid)
        select NOW(), de.id,p_id,NOW(),_bonus_time,false,false
		from de;
	end if;
 END;
$$;

--Задание 4.
create or replace procedure log_work(p_employee_id uuid,p_project_id uuid,p_work_date date,p_work_hours integer)
language plpgsql
as $$
DECLARE
	_is_active BOOLEAN;
	_required_review BOOLEAN;
	_cur_time date;
begin
	select is_active
	into _is_active
	from projects
	where id=p_project_id;

	if not _is_active then
		raise notice 'Project closed';
		return;
	end if;

	if p_work_hours < 1 then
		raise notice 'Cannot be less than 1 hour';
		return;
	end if;

	if p_work_hours > 24 then
		raise notice 'Cannot be more than 24 hours';
		return;
	end if;

	select current_date - interval '7 day' into _cur_time;

	if p_work_hours > 16 then _required_review = true;
	elseif
	p_work_date > current_date then _required_review = true; 
	elseif
	p_work_date < _cur_time then _required_review = true;
	else
	_required_review = false;	
	end if;

	insert into logs(employee_id,project_id,work_date,work_hours,required_review)
	values(p_employee_id,p_project_id,p_work_date,p_work_hours,_required_review);
end;
$$;

--Задание 5.
	create table if not exists employee_rate_history(
	id serial primary key,
	employee_id uuid REFERENCES employees(id),
	rate integer,
	from_date date 
);	
insert into employee_rate_history(employee_id,rate,from_date)
select id,rate,'2020-12-26'::date from employees;

create or replace function save_employee_rate_history()
returns trigger 
language plpgsql
as $$
 begin
 	if old.rate is distinct from new.rate then 
		insert into employee_rate_history(employee_id,rate,from_date)
		values(NEW.id,NEW.rate,current_date);
	end if;
	return null;
end
$$;

create or replace trigger change_employee_rate
after update or insert on employees
for each row
execute function save_employee_rate_history();

--Задание 6.
create or replace function best_project_workers(p_id uuid)
returns table(name text,work_hours integer)
language plpgsql
as $$
begin
	return query
	
	select e.name,sum(l.work_hours) AS work_hours
	from employees e
	join logs l on e.id=l.employee_id
	where l.project_id=p_id
	group by e.name
	order by work_hours DESC
	limit 3;
	
end;
$$;

--Задание 7.
create or replace function calculate_month_salary(date_start date,date_end date)
returns table (id text,name text,worked_hours integer,salary integer)
language plpgsql
as $$
BEGIN
	return query
	
	with t1 as (
	select l.employee_id as em_id,e.name as name,sum(work_hours) as work_hours
	from logs l 
	join employees e ON e.id = l.employee_id
	where l.required_review is false and is_paid IS false and l.work_date between $1 and $2
	group by l.employee_id,e.name),
	t2 as (
		select employees.id,rate
		from employees
	),
	t3 as (
		select t1.em_id::text as id, t1.name as name,t1.work_hours::integer as worked_hours,round(t1.work_hours*t2.rate)::integer as salary
		from t1 
		join t2 on t1.em_id=t2.id)
	select t3.id,t3.name,t3.worked_hours::integer,
	case
	 when t3.worked_hours>160 then round(t3.salary*1.25)::integer
	 else t3.salary::integer
	end as salary
	from t3;
end;
$$;











