import argparse
import json
import time
import uuid

import numpy as np
import pandas as pd
from confluent_kafka import Consumer, TopicPartition

from app.db import connect, read_results, validate_score
from app.kafka_io import BOOTSTRAP, SCORES, TRANSACTIONS, make_producer
from app.preprocessing import ROOT, Preprocessor
from app.scorer import Scorer, THRESHOLD


def check(count, timeout):
    path = ROOT / 'data/test.csv'
    if not path.is_file():
        raise RuntimeError('Нет data/test.csv. Положи туда test.csv из Kaggle.')
    frame = pd.read_csv(path, nrows=count)
    if frame.empty:
        raise RuntimeError('test.csv пустой')
    expected = Scorer().predict(Preprocessor().transform(frame))
    run_id = uuid.uuid4().hex
    ids = [f'check-{run_id}-{index}' for index in range(len(frame))]
    expected_by_id = dict(zip(ids, expected))
    watcher = Consumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': 'check-' + run_id,
        'enable.auto.commit': False,
        'auto.offset.reset': 'latest',
    })
    producer = make_producer()
    try:
        metadata = watcher.list_topics(SCORES, timeout=15).topics[SCORES]
        if metadata.error:
            raise RuntimeError(str(metadata.error))
        partitions = []
        for partition in metadata.partitions:
            _, end = watcher.get_watermark_offsets(TopicPartition(SCORES, partition), timeout=15)
            partitions.append(TopicPartition(SCORES, partition, end))
        watcher.assign(partitions)
        failures = []

        def delivered(error, message):
            if error is not None:
                failures.append(str(error))

        records = json.loads(frame.to_json(orient='records', double_precision=15))
        for transaction_id, record in zip(ids, records):
            payload = {'transaction_id': transaction_id, 'data': record}
            producer.produce(TRANSACTIONS, key=transaction_id.encode(), value=json.dumps(payload, allow_nan=False).encode(), on_delivery=delivered)
            producer.poll(0)
        if producer.flush(35) or failures:
            raise RuntimeError('Kafka не подтвердила отправку тестовой пачки')
        print(f'Отправлено строк из test.csv: {len(frame)}', flush=True)
        received = {}
        deadline = time.monotonic() + timeout
        while len(received) < len(ids) and time.monotonic() < deadline:
            message = watcher.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            payload = json.loads(message.value())
            if not isinstance(payload, dict) or payload.get('transaction_id') not in expected_by_id:
                continue
            validate_score(payload)
            transaction_id = payload['transaction_id']
            if not np.isclose(payload['score'], expected_by_id[transaction_id], rtol=0, atol=1e-12):
                raise RuntimeError(f'Скор не совпал с результатом модели: {transaction_id}')
            if payload['fraud_flag'] != int(payload['score'] > THRESHOLD):
                raise RuntimeError('Неверный fraud_flag')
            received[transaction_id] = payload
        if len(received) != len(ids):
            raise RuntimeError(f'За {timeout} секунд в scores получено {len(received)} из {len(ids)} сообщений')
        with connect() as connection:
            stored = []
            while time.monotonic() < deadline:
                stored = connection.execute(
                    'SELECT transaction_id, score, fraud_flag FROM transaction_scores WHERE transaction_id = ANY(%s)', (ids,)
                ).fetchall()
                connection.commit()
                if len(stored) == len(ids):
                    break
                time.sleep(1)
        if len(stored) != len(ids):
            raise RuntimeError(f'В PostgreSQL записано {len(stored)} из {len(ids)} результатов')
        for row in stored:
            if row != received[row['transaction_id']]:
                raise RuntimeError('Результат в PostgreSQL отличается от сообщения Kafka')
        _, fraud, recent = read_results()
        if len(fraud) > 10 or any(row['fraud_flag'] != 1 for row in fraud):
            raise RuntimeError('Неверная выборка подозрительных транзакций')
        if len(recent) != min(100, len(ids)) and len(ids) >= 100:
            raise RuntimeError('Неверная выборка для гистограммы')
        print(f'Проверка пройдена. Kafka: {len(received)}, PostgreSQL: {len(stored)}, с флагом фрода: {int((expected > THRESHOLD).sum())}.')
        print('Все скоры совпали с прямым вызовом модели. Выходные сообщения содержат ровно 3 поля.')
    finally:
        watcher.close()
        producer.flush(5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=1000)
    parser.add_argument('--timeout', type=int, default=180)
    args = parser.parse_args()
    if args.count < 1 or args.timeout < 1:
        parser.error('count и timeout должны быть положительными')
    check(args.count, args.timeout)
