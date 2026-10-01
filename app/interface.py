import json
import uuid

import numpy as np
import pandas as pd
import streamlit as st

from app.db import read_results
from app.kafka_io import TRANSACTIONS, make_producer
from app.preprocessing import ROOT, INPUT_COLUMNS


def send_rows(frame):
    producer = make_producer()
    errors = []
    progress = st.progress(0.0)
    records = json.loads(frame.to_json(orient='records'))

    def delivered(error, message):
        if error is not None:
            errors.append(str(error))

    for index, record in enumerate(records):
        transaction_id = record.pop('transaction_id', None) or str(uuid.uuid4())
        transaction_id = str(transaction_id)
        payload = {'transaction_id': transaction_id, 'data': record}
        encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode('utf-8')
        while True:
            try:
                producer.produce(TRANSACTIONS, key=transaction_id.encode(), value=encoded, on_delivery=delivered)
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
        if (index + 1) % 100 == 0:
            progress.progress((index + 1) / len(records))
    pending = producer.flush(35)
    if pending or errors:
        raise RuntimeError('Не все сообщения записаны в Kafka: ' + '; '.join(errors[:3]))
    progress.progress(1.0)
    return len(records)


st.set_page_config(page_title='Скоринг транзакций', layout='wide')
st.title('Скоринг транзакций')
upload_tab, results_tab = st.tabs(['Отправка транзакций', 'Результаты'])

with upload_tab:
    source = st.radio('Источник данных', ['Файл из папки data', 'Загрузить CSV'], horizontal=True)
    frame = None
    try:
        if source == 'Файл из папки data':
            path = ROOT / 'data/test.csv'
            if path.exists():
                limit = st.number_input('Количество первых строк test.csv', min_value=1, max_value=262144, value=1000)
                frame = pd.read_csv(path, nrows=int(limit))
            else:
                st.info('Положи test.csv в папку data или загрузи файл ниже через выбор источника.')
        else:
            uploaded = st.file_uploader('CSV с транзакциями', type=['csv'])
            if uploaded is not None:
                frame = pd.read_csv(uploaded)
                if not frame.empty:
                    limit = st.number_input('Количество строк для отправки', min_value=1, max_value=len(frame), value=min(1000, len(frame)))
                    frame = frame.head(int(limit))
        if frame is not None:
            missing = set(INPUT_COLUMNS) - set(frame.columns)
            if missing:
                st.error('Нет столбцов: ' + ', '.join(sorted(missing)))
            elif frame.empty:
                st.info('В файле нет транзакций')
            else:
                st.dataframe(frame.head(10), hide_index=True, use_container_width=True)
                if st.button('Отправить в Kafka', type='primary'):
                    count = send_rows(frame)
                    st.success(f'Отправлено: {count}. Результаты появятся после обработки.')
    except Exception as error:
        st.error(str(error))

with results_tab:
    if st.button('Посмотреть результаты', type='primary'):
        try:
            st.session_state['results'] = read_results()
        except Exception as error:
            st.error('Не удалось прочитать базу: ' + str(error))
    if 'results' in st.session_state:
        counts, fraud, recent = st.session_state['results']
        left, right = st.columns(2)
        left.metric('Всего транзакций', counts['total'])
        right.metric('С флагом фрода', counts['fraud'])
        st.subheader('Последние 10 транзакций с fraud_flag = 1')
        if fraud:
            st.dataframe(pd.DataFrame(fraud), hide_index=True, use_container_width=True)
        else:
            st.info('Транзакций с флагом фрода пока нет')
        st.subheader('Распределение скоров последних 100 транзакций')
        if recent:
            values = [row['score'] for row in recent]
            counts, edges = np.histogram(values, bins=np.linspace(0, 1, 21))
            histogram = pd.DataFrame({
                'Интервал скора': [f'{edges[i]:.2f}-{edges[i + 1]:.2f}' for i in range(20)],
                'Количество': counts,
            })
            st.bar_chart(histogram, x='Интервал скора', y='Количество')
            st.caption(f'Транзакций в гистограмме: {len(values)}')
        else:
            st.info('В базе пока нет результатов')
