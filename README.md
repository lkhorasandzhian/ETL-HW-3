# ETL-processes HW 3
Домашнее задание 3 по учебной дисциплине «ETL-процессы».  
Тема: «Загрузка данных в целевую систему».  
Выполнил студент: Хорасанджян Левон, МИНДА251.

## Task
1. Используйте ваш результат преобразования данных из задания к вебинару 3 «Основы трансформации данных».
2. Настройте два процесса загрузки для преобразованных данных:
   - первый процесс должен прогружать исторические данные полностью;
   - второй — подхватывать изменения за последние несколько дней.

## Solution
Для решения задачи использован инструмент **Apache Airflow**.  
Реализованы два отдельных DAG:

- `load_cleaned_full` — полная историческая загрузка данных из `cleaned.csv` в целевую БД Postgres;
- `load_cleaned_last_days` — инкрементальная загрузка данных за последние 5 дней
  с идемпотентным обновлением (удаление окна дат и повторная вставка данных из источника).

В качестве целевой системы используется отдельная база данных Postgres (`target-postgres`),
развёрнутая в Docker Compose.

## Repository Description
В репозитории настроено окружение **Apache Airflow** с использованием **Docker Compose**.

Основные компоненты:
- `dags/load_cleaned_full.py` — DAG полной исторической загрузки;
- `dags/load_cleaned_last_days.py` — DAG инкрементальной загрузки за последние 5 дней;
- `data/cleaned.csv` — результат трансформации данных из [HW-2](https://github.com/lkhorasandzhian/ETL-HW-2);
- `docker-compose.yml` — окружение Airflow + целевая БД Postgres.

## Credentials
### Airflow UI
URL: http://localhost:8080  
Username: airflow  
Password: airflow  

### Airflow Connection (`target_pg`)
Connection Type: Postgres  
Host: `target-postgres`  
Port: `5432`  
Database: `target`  
Login: `target`  
Password: `target`  
Description: `Target Postgres for ETL HW-3 (cleaned.csv full + incremental loads)`

## Results
В результате выполнения DAG данные из `data/cleaned.csv` загружаются в целевую таблицу `public.cleaned` в базе данных Postgres.
Скриншоты успешного выполнения DAG в Airflow размещены в папке `screenshots`.
