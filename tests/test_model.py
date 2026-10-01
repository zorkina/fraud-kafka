import numpy as np
import pandas as pd
import pytest

from app.preprocessing import INPUT_COLUMNS, InvalidTransaction, Preprocessor, ROOT
from app.scorer import Scorer, THRESHOLD


@pytest.fixture(scope='module')
def preprocessor():
    return Preprocessor()


@pytest.fixture(scope='module')
def scorer():
    return Scorer()


@pytest.fixture(scope='module')
def rows():
    path = ROOT / 'data/test.csv'
    if not path.exists():
        pytest.skip('Нужен data/test.csv из Kaggle')
    return pd.read_csv(path, nrows=10)


def test_artifacts_present():
    assert (ROOT / 'models/my_catboost.cbm').is_file()
    assert (ROOT / 'models/preprocessing.json').is_file()


def test_train_metadata(preprocessor):
    assert preprocessor.parameters['train_rows'] == 786431
    assert len(preprocessor.parameters['train_sha256']) == 64


def test_model_features(scorer):
    assert len(scorer.model.feature_names_) == 25
    assert len(scorer.model.get_cat_feature_indices()) == 11
    assert scorer.model.tree_count_ == 48


def test_columns(rows, preprocessor, scorer):
    features = preprocessor.transform(rows)
    assert list(features) == scorer.model.feature_names_
    assert len(features) == len(rows)
    assert set(INPUT_COLUMNS).issubset(rows.columns)


def test_single_and_batch_equal(rows, preprocessor, scorer):
    batch = scorer.predict(preprocessor.transform(rows))
    single = [scorer.predict(preprocessor.transform(rows.iloc[[index]]))[0] for index in range(len(rows))]
    np.testing.assert_array_equal(batch, single)


def test_result_format(rows, preprocessor, scorer):
    result = scorer.score(preprocessor.transform(rows.iloc[[0]]), 'test-id')
    assert set(result) == {'transaction_id', 'score', 'fraud_flag'}
    assert result['transaction_id'] == 'test-id'
    assert 0 <= result['score'] <= 1
    assert result['fraud_flag'] == int(result['score'] > THRESHOLD)


def test_input_is_not_modified(rows, preprocessor):
    source = rows.copy(deep=True)
    preprocessor.transform(rows)
    pd.testing.assert_frame_equal(rows, source)


def test_missing_column(rows, preprocessor):
    with pytest.raises(InvalidTransaction):
        preprocessor.transform(rows.drop(columns='amount'))


def test_empty_frame(rows, preprocessor):
    with pytest.raises(InvalidTransaction):
        preprocessor.transform(rows.iloc[:0])


@pytest.mark.parametrize('column,value', [
    ('amount', -1), ('amount', float('inf')), ('population_city', -1),
    ('lat', 91), ('lon', 181), ('merchant_lat', -91),
    ('merchant_lon', -181), ('lat', float('nan')),
])
def test_invalid_numeric(rows, preprocessor, column, value):
    frame = rows.iloc[[0]].copy()
    frame[column] = value
    with pytest.raises(InvalidTransaction):
        preprocessor.transform(frame)


@pytest.mark.parametrize('value', ['not-a-date', None])
def test_invalid_time(rows, preprocessor, value):
    frame = rows.iloc[[0]].copy()
    frame['transaction_time'] = value
    with pytest.raises(InvalidTransaction):
        preprocessor.transform(frame)


def test_unknown_category(rows, preprocessor, scorer):
    frame = rows.iloc[[0]].copy()
    frame['merch'] = '__unknown__'
    features = preprocessor.transform(frame)
    assert features['merch_cat'].iloc[0] == 'cat_NAN'
    assert np.isfinite(scorer.predict(features)).all()


def test_numeric_imputation(rows, preprocessor, scorer):
    frame = rows.iloc[[0]].copy()
    frame['amount'] = np.nan
    frame['population_city'] = np.nan
    features = preprocessor.transform(frame)
    assert np.isfinite(scorer.predict(features)).all()


def test_extra_columns_do_not_change_score(rows, preprocessor, scorer):
    original = scorer.predict(preprocessor.transform(rows))
    extra = rows.assign(target=1, index=100)
    np.testing.assert_array_equal(original, scorer.predict(preprocessor.transform(extra)))
