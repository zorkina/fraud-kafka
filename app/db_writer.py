import json
import logging
import signal

from app.db import connect, save_score, validate_score
from app.kafka_io import SCORES, make_consumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    consumer = make_consumer('fraud-db-writer', SCORES)
    connection = connect()
    running = True
    processed = 0

    def stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logger.info('Запись результатов запущена')
    try:
        while running:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            try:
                payload = validate_score(json.loads(message.value()))
            except (ValueError, TypeError, UnicodeError) as error:
                logger.error('Пропущен некорректный скор partition=%s offset=%s: %s',
                             message.partition(), message.offset(), error)
                consumer.commit(message=message, asynchronous=False)
                continue
            save_score(connection, payload)
            consumer.commit(message=message, asynchronous=False)
            processed += 1
            if processed % 100 == 0:
                logger.info('Сохранено сообщений: %s', processed)
    finally:
        connection.close()
        consumer.close()


if __name__ == '__main__':
    main()
