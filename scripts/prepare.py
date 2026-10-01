import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from geopy.distance import great_circle
from sklearn.impute import SimpleImputer

from app.preprocessing import CATEGORIES, ROOT, TIME_COLUMNS


def prepare(path):
    train = pd.read_csv(path).drop(columns=['name_1', 'name_2', 'street', 'post_code'])
    if 'target' not in train or set(train['target'].dropna().unique()) - {0, 1}:
        raise ValueError('В train.csv нужен столбец target со значениями 0 и 1')
    dates = pd.to_datetime(train.pop('transaction_time'))
    for name, values in zip(TIME_COLUMNS, [dates.dt.hour, dates.dt.year, dates.dt.month, dates.dt.day, dates.dt.dayofweek]):
        train[name] = values
    for column in CATEGORIES:
        table = (
            train.groupby(column, dropna=False)[['target']].count()
            .sort_values('target', ascending=False).reset_index()
            .set_axis([column, 'count'], axis=1).reset_index()
        )
        table['index'] = table.apply(lambda row: np.nan if pd.isna(row[column]) else row['index'], axis=1)
        table[column + '_cat'] = [
            'cat_NAN' if pd.isna(rank) else 'cat_' + str(rank) if rank < 50 else 'cat_50+'
            for rank in table['index']
        ]
        train = train.merge(table[[column, column + '_cat']], how='left', on=column)
    train['distance'] = [
        great_circle((lat, lon), (merchant_lat, merchant_lon)).km
        for lat, lon, merchant_lat, merchant_lon in train[['lat', 'lon', 'merchant_lat', 'merchant_lon']].itertuples(index=False, name=None)
    ]
    parameters = {'category_maps': {}, 'means': {}, 'numeric_means': {}}
    for column in CATEGORIES:
        pairs = train[[column, column + '_cat']].drop_duplicates()
        parameters['category_maps'][column] = {
            str(key): str(value) for key, value in pairs.itertuples(index=False, name=None)
        }
    for column in [x + '_cat' for x in CATEGORIES] + TIME_COLUMNS:
        parameters['means'][column] = {
            str(key): float(value) for key, value in train.groupby(column)['target'].mean().items()
        }
    columns = ['amount', 'population_city', 'distance']
    imputer = SimpleImputer(strategy='mean').fit(train[columns])
    parameters['numeric_means'] = dict(zip(columns, imputer.statistics_.tolist()))
    parameters['train_rows'] = len(train)
    parameters['train_sha256'] = hashlib.sha256(Path(path).read_bytes()).hexdigest()
    return parameters


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', type=Path, default=ROOT / 'data/train.csv')
    parser.add_argument('--output', type=Path, default=ROOT / 'models/preprocessing.json')
    args = parser.parse_args()
    if not args.train.is_file():
        raise SystemExit(f'Нет файла {args.train}')
    parameters = prepare(args.train)
    args.output.write_text(json.dumps(parameters, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    print(f'Параметры сохранены в {args.output}. Строк train.csv: {parameters["train_rows"]}')


if __name__ == '__main__':
    main()
