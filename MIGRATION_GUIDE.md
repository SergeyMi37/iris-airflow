# Руководство по миграции на Apache Airflow 3.1

## Обзор изменений

Проект был успешно обновлен с Apache Airflow 2.8.3 на версию 3.1.0.

## Выполненные изменения

### 1. Код DAG (dags/cbr_elt.py)
- Обновлен импорт `PythonOperator`:
  ```python
  # Было:
  from airflow.operators.python_operator import PythonOperator

  # Стало:
  from airflow.operators.python import PythonOperator
  ```

### 2. Docker Compose (docker-compose.yaml)
- Обновлена версия образа Airflow:
  ```yaml
  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.1.0}
  ```
- Обновлены названия пакетов провайдеров:
  ```yaml
  _PIP_ADDITIONAL_REQUIREMENTS: apache-airflow-providers-rabbitmq apache-airflow-providers-postgres apache-airflow-providers-mongo pyvalhalla geopandas
  ```

### 3. Переменные окружения (env.sample)
- Исправлена опечатка в названии переменной:
  ```bash
  # Было:
  DB_POST=5432

  # Стало:
  DB_PORT=5432
  ```

### 4. Документация (README.md)
- Обновлены бейджи версии Airflow
- Обновлено описание требований

## Тестирование миграции

### Предварительные требования
- Docker и Docker Compose установлены
- Минимум 4GB RAM, 2 CPU ядра
- Стабильное интернет-соединение

### Шаги тестирования

1. **Остановка текущих контейнеров:**
   ```bash
   docker compose down
   ```

2. **Очистка volumes (опционально, для чистого теста):**
   ```bash
   docker volume rm iris-airflow_postgres-db-volume
   ```

3. **Запуск с новой версией:**
   ```bash
   docker compose up -d
   ```

4. **Проверка состояния:**
   ```bash
   docker compose ps
   docker compose logs airflow-webserver
   ```

5. **Доступ к интерфейсу:**
   - Airflow UI: http://localhost:8080
   - Логин: airflow
   - Пароль: airflow

6. **Тестирование DAG:**
   - Проверить загрузку DAG в интерфейсе
   - Запустить тестовый запуск DAG
   - Проверить логи выполнения

## План отката

В случае проблем с версией 3.1.0:

### Быстрый откат:
1. Остановить контейнеры:
   ```bash
   docker compose down
   ```

2. Вернуть старую версию в docker-compose.yaml:
   ```yaml
   image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.8.3}
   ```

3. Восстановить старые названия провайдеров:
   ```yaml
   _PIP_ADDITIONAL_REQUIREMENTS: airflow-provider-rabbitmq apache_airflow_providers_postgres apache_airflow_providers_mongo pyvalhalla geopandas
   ```

4. Перезапустить:
   ```bash
   docker compose up -d
   ```

### Резервное копирование данных:
- База данных PostgreSQL сохраняется в volume `postgres-db-volume`
- Логи сохраняются в папке `logs/`
- Конфигурация в папке `config/`

## Новые возможности Airflow 3.1

- Улучшенная производительность
- Лучшая поддержка Python 3.10+
- Обновленные провайдеры
- Улучшения в UI/UX

## Известные проблемы

- Возможны изменения в API некоторых операторов
- Могут потребоваться обновления кастомных плагинов
- Проверить совместимость с существующими соединениями

## Контакты

При возникновении проблем во время миграции:
1. Проверить логи контейнеров
2. Проверить версию Python (минимум 3.10)
3. Убедиться в наличии достаточных ресурсов системы