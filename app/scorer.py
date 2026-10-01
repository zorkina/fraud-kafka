import numpy as np
from catboost import CatBoostClassifier

from app.preprocessing import ROOT

THRESHOLD = 0.98


class Scorer:
    def __init__(self, path=ROOT / 'models/my_catboost.cbm'):
        self.model = CatBoostClassifier()
        self.model.load_model(str(path))

    def predict(self, features):
        scores = self.model.predict_proba(
            features[self.model.feature_names_], task_type='CPU', thread_count=1
        )[:, 1]
        if not np.isfinite(scores).all() or ((scores < 0) | (scores > 1)).any():
            raise RuntimeError('Модель вернула некорректный скор')
        return scores

    def score(self, features, transaction_id):
        score = float(self.predict(features)[0])
        return {
            'transaction_id': transaction_id,
            'score': score,
            'fraud_flag': int(score > THRESHOLD),
        }
