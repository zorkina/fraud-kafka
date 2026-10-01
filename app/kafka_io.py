import json
import os

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TRANSACTIONS = 'transactions'
SCORES = 'scores'


def make_consumer(group, topic):
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.auto.offset.store': False,
    })
    consumer.subscribe([topic])
    return consumer


def make_producer():
    return Producer({
        'bootstrap.servers': BOOTSTRAP,
        'enable.idempotence': True,
        'acks': 'all',
        'delivery.timeout.ms': 30000,
        'linger.ms': 5,
    })


def send_json(producer, topic, payload):
    errors = []

    def delivered(error, message):
        if error is not None:
            errors.append(str(error))

    producer.produce(
        topic,
        key=str(payload['transaction_id']).encode('utf-8'),
        value=json.dumps(payload, ensure_ascii=False, allow_nan=False).encode('utf-8'),
        on_delivery=delivered,
    )
    pending = producer.flush(35)
    if pending or errors:
        raise RuntimeError('Kafka не подтвердила запись: ' + '; '.join(errors))


def decode_transaction(raw):
    if raw is None:
        raise ValueError('Пустое сообщение')
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError('Сообщение должно быть JSON-объектом')
    transaction_id = payload.get('transaction_id')
    if not isinstance(transaction_id, str) or not transaction_id.strip():
        raise ValueError('transaction_id должен быть непустой строкой')
    if len(transaction_id) > 200:
        raise ValueError('Слишком длинный transaction_id')
    data = payload.get('data')
    if data is None:
        data = {key: value for key, value in payload.items() if key != 'transaction_id'}
    if not isinstance(data, dict):
        raise ValueError('Поле data должно быть объектом')
    return transaction_id, data
