# BigDataSparkApp

Проект для проведения 4 экспериментов с Hadoop HDFS и Spark на датасете NYC Taxi Trips.

## Состав проекта

- `docker/spark/Dockerfile` - обогащенный необходимыми для сбора метрик python зависимости контейнер для spark
- `docker-compose.hadoop-1dn.yml` - стенд с `1 NameNode`, `1 DataNode`, `Spark Master`, `Spark Worker`
- `docker-compose.hadoop-3dn.yml` - стенд с `1 NameNode`, `3 DataNode`, `Spark Master`, `3 Spark Worker`
- `main.py` - базовое Spark-приложение
- `main_opt.py` - оптимизированное Spark-приложение
- `prepare_parquet.py` - подготовка Parquet-версии датасета в HDFS
- `scripts/run_1dn_baseline.sh` - запуск эксперимента `1 DataNode + baseline`
- `scripts/run_1dn_optimized.sh` - запуск эксперимента `1 DataNode + optimized`
- `scripts/run_3dn_baseline.sh` - запуск эксперимента `3 DataNode + baseline`
- `scripts/run_3dn_optimized.sh` - запуск эксперимента `3 DataNode + optimized`

## Что делает main.py

Файл `main.py` реализует базовое Spark-приложение для эксперимента без оптимизаций.

Основные шаги выполнения:

1. Создается `SparkSession`
2. Из HDFS читается подготовленный Parquet-датасет
3. Выполняется очистка данных:
   - преобразование времени в `timestamp`
   - удаление строк с `null`
   - удаление строк с нулевыми и отрицательными значениями в ключевых числовых полях
4. Выполняются агрегирующие операции:
   - подсчет числа строк после очистки
   - расчет средней, минимальной и максимальной длины поездки
   - расчет среднего `fare_amount`
   - группировка по `passenger_count`
   - группировка по часу начала поездки
   - поиск самых популярных точек начала поездки
5. Замеряется время выполнения ключевых шагов
6. Замеряется использование RAM драйвера
7. Результат сохраняется в JSON-файл в директорию `artifacts`

Это baseline-версия приложения, которая используется как базовая точка сравнения.

## Основные отличия main_opt.py

Файл `main_opt.py` содержит оптимизированную версию того же Spark-приложения.

Ключевые отличия от `main.py`:

- включен `Adaptive Query Execution`
- увеличено число `shuffle partitions`
- добавлен `repartition(...)` перед тяжелыми агрегациями
- добавлен `persist(StorageLevel.MEMORY_AND_DISK)`
- кэш материализуется заранее через `count()`
- колонка `pickup_hour` вычисляется заранее, а не во время каждой агрегации
- перед кэшированием оставляются только нужные для агрегаций колонки
- в результирующий JSON дополнительно записываются параметры оптимизации

`main_opt.py` нужен для сравнения с базовой версией и оценки эффекта оптимизации Spark-приложения.

## Подготовка данных

Для ускорения чтения эксперименты выполняются не по исходному CSV, а по Parquet-версии датасета.

Сценарий такой:

1. Исходный CSV загружается в HDFS
2. Скрипт `prepare_parquet.py` читает CSV по явной схеме
3. Dataset сохраняется в HDFS в формате Parquet
4. `main.py` и `main_opt.py` запускаются уже на Parquet

Это уменьшает накладные расходы на повторное чтение большого CSV и исключает дорогое `inferSchema`.

## Требования

Для работы проекта необходим датасет `yellow_tripdata_2016-01.csv`, который можно скачать здесь:
https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data?select=yellow_tripdata_2016-01.csv


## Общая схема экспериментов

Нужно выполнить 4 запуска:

1. `1 DataNode + Spark baseline`
2. `1 DataNode + Spark optimized`
3. `3 DataNode + Spark baseline`
4. `3 DataNode + Spark optimized`

Для каждого сценария порядок один и тот же:

1. Поднять нужный Hadoop/Spark стенд через `docker compose up -d`
2. Дождаться запуска контейнеров
3. Выполнить соответствующий `sh`-скрипт
4. Дождаться завершения Spark job
5. Сохранить результаты из папки `artifacts`
6. Остановить стенд через `docker compose down`

## Алгоритм запуска экспериментов

### Эксперименты для стенда с 1 DataNode

Поднять кластер:

```bash
docker compose -f docker-compose.hadoop-1dn.yml up -d
```

Запустить базовый эксперимент:

```bash
./scripts/run_1dn_baseline.sh
```

Запустить оптимизированный эксперимент:

```bash
./scripts/run_1dn_optimized.sh
```

После завершения обоих запусков остановить кластер:

```bash
docker compose -f docker-compose.hadoop-1dn.yml down
```

### Эксперименты для стенда с 3 DataNode

Поднять кластер:

```bash
docker compose -f docker-compose.hadoop-3dn.yml up -d
```

Запустить базовый эксперимент:

```bash
./scripts/run_3dn_baseline.sh
```

Запустить оптимизированный эксперимент:

```bash
./scripts/run_3dn_optimized.sh
```

После завершения обоих запусков остановить кластер:

```bash
docker compose -f docker-compose.hadoop-3dn.yml down
```

## Что делают sh-скрипты

Каждый скрипт автоматически:

1. Ждет готовности HDFS
2. Создает директорию `/data/nyc_taxi` в HDFS
3. Загружает `yellow_tripdata_2016-01.csv` в HDFS
4. Проверяет наличие файла в HDFS
5. При отсутствии Parquet подготавливает его через `prepare_parquet.py`
6. Ждет готовности `spark-master`
7. Запускает `spark-submit` для нужного приложения

## Где смотреть результаты

После выполнения экспериментов результаты сохраняются в локальную папку:

- `artifacts/`

Форматы файлов:

- `experiment_*.json` - результаты базового приложения
- `experiment_opt_*.json` - результаты оптимизированного приложения

В логах содержатся:

- общее время выполнения
- время по шагам
- оценка использования памяти драйвера
- вычисленные агрегаты по датасету

## Веб-интерфейсы

После старта контейнеров доступны:

- Hadoop NameNode UI: [http://localhost:9870](http://localhost:9870)
- Spark Master UI: [http://localhost:8080](http://localhost:8080)
- Spark Worker UI: [http://localhost:8081](http://localhost:8081)

## Важное замечание

Одновременно нужно запускать только один compose-файл:

- либо `docker-compose.hadoop-1dn.yml`
- либо `docker-compose.hadoop-3dn.yml`

Иначе возникнет конфликт по именам контейнеров и портам.
