              Описание проекта. Автосалон «Врум-Бум»

«Врум-Бум» — прославившаяся сеть салонов легковых автомобилей — стремительно набирает обороты. Их карманный слоган «Если вы слышите Врум, значит уже Бум!» стал знаком качества, который привлекает тысячи покупателей ежедневно. Сеть предоставляет широкий выбор машин от экономкласса до люксовых спорткаров и обслуживает всю страну.
Однако их быстрый рост привел к непредвиденным трудностям: с каждым новым салоном становится все сложнее управлять огромным объёмом данных о продажах, поставках, запасах и клиентах. Вся эта информация сейчас хранится в сыром, неструктурированном виде, что сильно затрудняет работу.
Кроме того, «Врум-Бум» хотел бы применять более сложные аналитические методы, чтобы лучше понять своих клиентов, улучшить бизнес-процессы и увеличить продажи. Они понимают, что успешное будущее их компании во многом зависит от качественного анализа данных, и поэтому обратились к вам за помощью.
Ваша задача — нормализовать и структурировать существующие сырые данные, а потом написать несколько запросов для получения информации из БД. Для этого перенесите сырые данные в PostgreSQL. Вы можете выполнить работу в любом удобном для вас клиенте. Результатом станет набор SQL-команд, объединённых в единый скрипт. 
Вы будете работать с данными по продажам автомобилей с 2015 по 2023 год.

Этап 1. Создание и заполнение БД
На этом этапе вам нужно создать базу данных, схемы и таблицы и заполнить их данными. 
Названия полей в таблицах должны быть на английском языке — помните о принципах наименования объектов. Правильные названия полей облегчают поддержку вашей БД и повышают читабельность кода.
Вот пошаговая инструкция:
Шаг 1. Создайте БД с именем sprint_1. Всю работу выполните в этой БД, команду создания БД в итоговый скрипт включать не нужно.
Шаг 2. Создайте схему raw_data и таблицу sales для загрузки сырых данных в этой схеме. Начиная с этого пункта (включительно) все SQL-команды должны быть в итоговом скрипте.
Шаг 3. Скачайте и проанализируйте исходный csv-файл.
cars.csv
Шаг 4. Заполните таблицу sales данными, используя команду COPY в менеджере БД (например, pgAdmin) или \copy в psql. Если возникнет ошибка о лишних данных в исходном файле, укажите явно параметр DELIMITER.
Шаг 5. Проанализируйте сырые данные. Подумайте:
какие данные повторяются и их стоит вынести в отдельные таблицы;
какие типы данных должны быть у каждого поля;
на какие поля стоит добавить ограничения (CONSTRAINTS);
какие поля могут содержать NULL, а где нужно добавить ограничение NOT NULL.
Шаг 6. Создайте схему car_shop, а в ней cоздайте нормализованные таблицы (до третьей нормальной формы). Подумайте, какие поля будут выступать первичными ключами, а какие внешними. У всех первичных ключей должен быть автоинкремент.
Шаг 7. Выберите наиболее подходящий формат данных для каждой колонки. Используя комментарий (-- comment), для каждого поля напишите, почему вы выбрали тот или иной тип данных. Например:
brand_name varchar(50) — не существует названий брендов длиной более 50 символов, в названии бренда могут быть и цифры, и буквы, поэтому выбираем varchar(50).
price numeric(9, 2) — цена может содержать только сотые и не может быть больше семизначной суммы. У numeric повышенная точность при работе с дробными числами, поэтому при операциях c этим типом данных, дробные числа не потеряются.
Шаг 8. Заполните все таблицы данными, c помощью команд INSERT INTO … SELECT …. Чтобы разбить столбец с маркой, моделью машины и её цветом, можно использовать разные способы — выбирайте любой, удобный для себя.

Этап 2. Создание выборок
Вы создали таблицы и заполнили их данными — браво! Теперь выполните задания на выборку данных для аналитики. Все запросы выполните к созданным таблицам, а не к сырым данным:
Задание 1
Напишите запрос, который выведет процент моделей машин, у которых нет параметра gasoline_consumption.
Задание 2
Напишите запрос, который покажет название бренда и среднюю цену его автомобилей в разбивке по всем годам с учётом скидки. Итоговый результат отсортируйте по названию бренда и году в восходящем порядке. Среднюю цену округлите до второго знака после запятой.
Задание 3
Посчитайте среднюю цену всех автомобилей с разбивкой по месяцам в 2022 году с учётом скидки. Результат отсортируйте по месяцам в восходящем порядке. Среднюю цену округлите до второго знака после запятой.
Задание 4
Используя функцию STRING_AGG, напишите запрос, который выведет список купленных машин у каждого пользователя через запятую. Пользователь может купить две одинаковые машины — это нормально. Название машины покажите полное, с названием бренда — например: Tesla Model 3. Отсортируйте по имени пользователя в восходящем порядке. Сортировка внутри самой строки с машинами не нужна.
Задание 5
Напишите запрос, который вернёт самую большую и самую маленькую цену продажи автомобиля с разбивкой по стране без учёта скидки. Цена в колонке price дана с учётом скидки.
Задание 6
Напишите запрос, который покажет количество всех пользователей из США. Это пользователи, у которых номер телефона начинается на +1.
