import math
import os

import psycopg
from psycopg.rows import dict_row


def connect():
    return psycopg.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        dbname=os.getenv('POSTGRES_DB', 'fraud'),
        user=os.getenv('POSTGRES_USER', 'fraud'),
        password=os.getenv('POSTGRES_PASSWORD', 'fraud_local'),
        connect_timeout=10,
        row_factory=dict_row,
    )


def validate_score(payload):
    if not isinstance(payload, dict) or set(payload) != {'transaction_id', 'score', 'fraud_flag'}:
        raise ValueError('Ожидаются transaction_id, score, fraud_flag')
    transaction_id = payload['transaction_id']
    score = payload['score']
    flag = payload['fraud_flag']
    if not isinstance(transaction_id, str) or not transaction_id.strip() or len(transaction_id) > 200:
        raise ValueError('Некорректный transaction_id')
    if isinstance(score, bool) or not isinstance(score, (int, float)):
        raise ValueError('Скор должен быть числом')
    if not math.isfinite(score) or not 0 <= score <= 1:
        raise ValueError('Скор вне диапазона [0, 1]')
    if type(flag) is not int or flag not in (0, 1):
        raise ValueError('fraud_flag должен быть 0 или 1')
    return payload


def save_score(connection, payload):
    connection.execute(
        'INSERT INTO transaction_scores (transaction_id, score, fraud_flag) '
        'VALUES (%s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING',
        (payload['transaction_id'], payload['score'], payload['fraud_flag']),
    )
    connection.commit()


def read_results():
    with connect() as connection:
        counts = connection.execute(
            'SELECT count(*) AS total, count(*) FILTER (WHERE fraud_flag = 1) AS fraud '
            'FROM transaction_scores'
        ).fetchone()
        fraud = connection.execute(
            'SELECT transaction_id, score, fraud_flag, created_at '
            'FROM transaction_scores WHERE fraud_flag = 1 ORDER BY id DESC LIMIT 10'
        ).fetchall()
        recent = connection.execute(
            'SELECT transaction_id, score FROM transaction_scores ORDER BY id DESC LIMIT 100'
        ).fetchall()
    return counts, fraud, recent
