import logging
import signal

import pandas as pd

from app.kafka_io import SCORES, TRANSACTIONS, decode_transaction, make_consumer, make_producer, send_json
from app.preprocessing import InvalidTransaction, Preprocessor
from app.scorer import Scorer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    preprocessor = Preprocessor()
    scorer = Scorer()
    consumer = make_consumer('fraud-scorer', TRANSACTIONS)
    producer = make_producer()
    running = True
    processed = 0

    def stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logger.info('Скоринг запущен')
    try:
        while running:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            try:
                transaction_id, data = decode_transaction(message.value())
                features = preprocessor.transform(pd.DataFrame([data]))
            except (ValueError, UnicodeError, InvalidTransaction) as error:
                logger.error('Пропущено некорректное сообщение partition=%s offset=%s: %s',
                             message.partition(), message.offset(), error)
                consumer.commit(message=message, asynchronous=False)
                continue
            result = scorer.score(features, transaction_id)
            send_json(producer, SCORES, result)
            consumer.commit(message=message, asynchronous=False)
            processed += 1
            if processed % 100 == 0:
                logger.info('Обработано транзакций: %s', processed)
    finally:
        consumer.close()
        producer.flush(5)


if __name__ == '__main__':
    main()
