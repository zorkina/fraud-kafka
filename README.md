# Скоринг транзакций

Сервис получает транзакции из Kafka, считает скор CatBoost и отправляет результат в топик `scores`. Отдельный consumer записывает результаты в PostgreSQL. Для отправки данных и просмотра результатов используется Streamlit.

```text
CSV -> Streamlit -> transactions -> scorer -> scores -> db-writer -> PostgreSQL
```

За основу взят [код семинара](https://github.com/NikitaMalykhin/mts25_mlops_hw2_real_time_fraud_detection). Модель `my_catboost.cbm` взята оттуда без изменения. Данные из [соревнования teta-ml-1-2025](https://www.kaggle.com/competitions/teta-ml-1-2025).

## Запуск

Нужны Docker Desktop с Docker Compose и архив `teta-ml-1-2025.zip` из Kaggle. Положи архив в `~/Downloads`, сами CSV в репозитории не хранятся.

```bash
git clone https://github.com/zorkina/fraud-kafka.git
cd fraud-kafka
bash start.sh
```

Скрипт извлечет `test.csv`, соберет контейнеры и проверит обработку первых 1000 строк. Другой путь к архиву можно передать аргументом:

```bash
bash start.sh /путь/к/teta-ml-1-2025.zip
```

Интерфейс доступен по адресу http://localhost:8501. Во вкладке `Результаты` нажми `Посмотреть результаты`. После первой проверки в пустой базе будет 1000 записей, из них 5 с флагом фрода. Повторный запуск проверки добавляет новую пачку с новыми идентификаторами.

Во вкладке `Отправка транзакций` можно прочитать строки из `data/test.csv` или загрузить CSV через браузер. CSV читает интерфейс, сам скоринговый сервис работает только с Kafka.

Запуск контейнеров без отправки проверочной пачки:

```bash
docker compose up -d --build
```

## Что запущено

`kafka` хранит сообщения, `kafka-init` создает топики `transactions` и `scores`. После создания топиков `kafka-init` завершается с кодом 0.

`scorer` получает транзакции и применяет модель. `db-writer` читает результаты и сохраняет их в `postgres`. `interface` запускает Streamlit. Все сервисы работают в одной Docker-сети. На хосте открыт только `127.0.0.1:8501`, Kafka и PostgreSQL наружу не опубликованы. Пароль `fraud_local` в compose используется для локального запуска.

## Модель и сообщения

Модель работает на CPU. Обучения при запуске нет, порог сохранен из семинара:

```python
fraud_flag = int(score > 0.98)
```

В `transactions` поступает JSON с полями `transaction_id` и `data`. В `data` находятся столбцы одной строки `test.csv`. Также поддерживается плоский JSON, где идентификатор и столбцы транзакции находятся на одном уровне.

В `scores` уходит один JSON-объект с тремя полями:

```json
{
  "transaction_id": "example-1",
  "score": 0.995,
  "fraud_flag": 1
}
```

Препроцессинг вынесен в `app/preprocessing.py`, вызов модели в `app/scorer.py`, работа с входным топиком в `app/consumer.py`. Категориальные отображения и средние заранее рассчитаны по `train.csv` и сохранены в `models/preprocessing.json`. Для inference файл `train.csv` не нужен.

Повторный расчет параметров на том же обучающем наборе:

```bash
unzip -p "$HOME/Downloads/teta-ml-1-2025.zip" train.csv > data/train.csv
docker compose run --rm prepare
docker compose up -d --build
```

Эта команда пересчитывает параметры препроцессинга, веса CatBoost не меняются.

## Результаты

В PostgreSQL создается таблица `transaction_scores`. В ней хранятся идентификатор транзакции, скор, флаг, время и порядковый номер записи. Повторное сообщение с тем же `transaction_id` не добавляет вторую строку.

По кнопке `Посмотреть результаты` интерфейс выводит последние 10 записей с `fraud_flag = 1` и гистограмму скоров последних 100 транзакций независимо от флага. Если записей меньше, используются все доступные. Порядок определяется по `id`, то есть по вставке в базу.

Скоринговый consumer подтверждает offset после записи результата в Kafka. Consumer базы подтверждает offset после коммита в PostgreSQL. Некорректные сообщения пропускаются с записью ошибки в лог.

## Проверки

Проверка прохождения 1000 транзакций через Kafka и PostgreSQL со сравнением скоров с прямым вызовом модели:

```bash
docker compose run --rm check
```

Модульные тесты:

```bash
docker compose run --rm --no-deps check python -m pytest -q
```

Для тестов с данными нужен `data/test.csv`. Он появляется после запуска `start.sh`.

## Логи и остановка

```bash
docker compose ps -a
docker compose logs --tail=100 scorer db-writer interface
docker compose down
```

При обычном `docker compose down` результаты в базе и сообщения Kafka сохраняются в volumes.
