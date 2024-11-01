import pandas as pd
import xgboost as xgb
from funcoin.huobi.model import BaseModel


class XgboostModel(BaseModel):
    def __init__(self, windows=-15, *args, **kwargs):
        super(XgboostModel, self).__init__(*args, **kwargs)
        self.model = None
        self.windows = windows

    def solve(self, df, train=True):
        df2 = df.copy()
        df2 = df2.sort_values(['id'])
        df2 = df2.reset_index(drop=True)

        def ma_n(n):
            df2['MA' + str(n)] = df2['open'].rolling(window=n).mean()

        def shift_n(name, n):
            df2[name + '_' + str(n)] = df2[name].shift(n)

        [ma_n(i) for i in (2, 3, 4, 5, 10, 15, 20, 25, 30)]
        cols = [col for col in df2.columns]
        [[shift_n(name, n) for name in cols if (name in ('MA5', 'open') or 'MA' in name)] for n in
         (1, 2, 3, 4, 5, 10, 15, 30)]

        df2['label'] = df2['open'].shift(self.windows)
        del df2['symbol'], df2['id']

        if train:
            df2.dropna(axis=0, how='any', inplace=True)
            x = df2[df2.columns[1:-1]].values
            y = df2['label'].values
            return x, y
        else:
            del df2['label']
            df2.dropna(axis=0, how='any', inplace=True)
            x = df2[df2.columns[1:]].values
            return x

    def train(self, df, *args, **kwargs):
        x, y = self.solve(df)
        self.model = xgb.XGBRegressor(n_jobs=1).fit(x, y)

    def predict(self, df, *args, **kwargs):
        x = self.solve(df, train=False)
        return self.model.predict(x)


class MultiXgboostModel(BaseModel):
    def __init__(self, windows=None, *args, **kwargs):
        super(MultiXgboostModel, self).__init__(*args, **kwargs)

        if windows is None:
            windows = [-5, -10, -15]
        self.model = None
        self.windows = windows

    def solve(self, df, train=True):
        df2 = df.copy()
        df2 = df2.sort_values(['id'])
        df2 = df2.reset_index(drop=True)

        def ma_n(n):
            df2['MA' + str(n)] = df2['open'].rolling(window=n).mean()

        def shift_n(name, n):
            df2[name + '_' + str(n)] = df2[name].shift(n)

        [ma_n(i) for i in (2, 3, 4, 5, 10, 15, 20, 25, 30)]
        cols = [col for col in df2.columns]
        [[shift_n(name, n) for name in cols if (name in ('MA5', 'open') or 'MA' in name)] for n in
         (1, 2, 3, 4, 5, 10, 15, 30)]

        #del df2['symbol'], df2['id']

        if train:
            cols = []
            df2['open2'] = df2['open'] - df2['open'].shift()
            df2['open2'] = df2['open2'].apply(lambda x: 1 if x > 0 else 0)
            for window in self.windows:
                col = f'label{window}'
                df2[col] = df2['open2'].shift(window)
                cols.append(col)

            df2.dropna(axis=0, how='any', inplace=True)
            x = df2[df2.columns[1:-1 - len(cols)]].values
            y = df2[cols].values
            return df2, x, y
        else:
            df2.dropna(axis=0, how='any', inplace=True)
            x = df2[df2.columns[1:]].values
            return df2, x

    def train(self, df, *args, **kwargs):
        df2, x, y = self.solve(df)

        from sklearn.multiclass import OneVsRestClassifier
        from xgboost import XGBClassifier

        self.model = OneVsRestClassifier(
            XGBClassifier(eval_metric=['logloss', 'auc', 'error'], use_label_encoder=False))
        self.model.fit(x, y)

    def predict(self, df, *args, **kwargs):
        df2, x = self.solve(df, train=False)
        return df2, self.model.predict(x)
