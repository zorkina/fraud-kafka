import json
from pathlib import Path

import numpy as np
import pandas as pd
from geopy.distance import great_circle

ROOT = Path(__file__).resolve().parents[1]
CATEGORIES = ['gender', 'merch', 'cat_id', 'one_city', 'us_state', 'jobs']
TIME_COLUMNS = ['hour', 'year', 'month', 'day_of_month', 'day_of_week']
INPUT_COLUMNS = [
    'transaction_time', 'merch', 'cat_id', 'amount', 'name_1', 'name_2',
    'gender', 'street', 'one_city', 'us_state', 'post_code', 'lat', 'lon',
    'population_city', 'jobs', 'merchant_lat', 'merchant_lon',
]


class InvalidTransaction(ValueError):
    pass


class Preprocessor:
    def __init__(self, path=ROOT / 'models/preprocessing.json'):
        with open(path, encoding='utf-8') as source:
            self.parameters = json.load(source)

    def transform(self, frame):
        missing = set(INPUT_COLUMNS) - set(frame.columns)
        if missing:
            raise InvalidTransaction('Нет столбцов: ' + ', '.join(sorted(missing)))
        if frame.empty:
            raise InvalidTransaction('Пустая таблица')
        frame = frame.reset_index(drop=True).copy()
        try:
            dates = pd.to_datetime(frame['transaction_time'], errors='raise')
            if dates.isna().any():
                raise InvalidTransaction('Пустое время транзакции')
            output = pd.DataFrame({
                'hour': dates.dt.hour,
                'year': dates.dt.year,
                'month': dates.dt.month,
                'day_of_month': dates.dt.day,
                'day_of_week': dates.dt.dayofweek,
            })
            for column in CATEGORIES:
                output[column + '_cat'] = (
                    frame[column].astype('string')
                    .map(self.parameters['category_maps'][column])
                    .fillna('cat_NAN')
                )
            for column in [x + '_cat' for x in CATEGORIES] + TIME_COLUMNS:
                output[column + '_mean_enc'] = (
                    output[column].astype(str).map(self.parameters['means'][column])
                )
            coordinate_names = ['lat', 'lon', 'merchant_lat', 'merchant_lon']
            coordinates = frame[coordinate_names].apply(pd.to_numeric, errors='raise')
            if not np.isfinite(coordinates.to_numpy()).all():
                raise InvalidTransaction('Координаты должны быть конечными числами')
            if (coordinates[['lat', 'merchant_lat']].abs() > 90).any().any():
                raise InvalidTransaction('Широта вне диапазона')
            if (coordinates[['lon', 'merchant_lon']].abs() > 180).any().any():
                raise InvalidTransaction('Долгота вне диапазона')
            distances = [
                great_circle((lat, lon), (merchant_lat, merchant_lon)).km
                for lat, lon, merchant_lat, merchant_lon in coordinates.itertuples(index=False, name=None)
            ]
            numeric = pd.DataFrame({
                'amount': pd.to_numeric(frame['amount'], errors='raise'),
                'population_city': pd.to_numeric(frame['population_city'], errors='raise'),
                'distance': distances,
            })
            numeric = numeric.fillna(self.parameters['numeric_means'])
            if not np.isfinite(numeric.to_numpy()).all() or (numeric < 0).any().any():
                raise InvalidTransaction('Сумма, население и расстояние должны быть неотрицательными числами')
            for column in numeric:
                output[column + '_log'] = np.log(numeric[column] + 1)
            for column in TIME_COLUMNS + [x + '_cat' for x in CATEGORIES]:
                output[column] = output[column].astype(str)
            return output
        except InvalidTransaction:
            raise
        except (ValueError, TypeError, AttributeError, OverflowError) as error:
            raise InvalidTransaction(str(error)) from error
