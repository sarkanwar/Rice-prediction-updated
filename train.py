
from __future__ import annotations
import os, pandas as pd, numpy as np, joblib
from dataclasses import dataclass
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor

@dataclass
class TrainResult:
    sarimax_model_path: str
    xgb_model_path: str | None
    metrics: dict

def fit_sarimax(series: pd.Series, order=(1,1,1), seasonal_order=(0,1,1,7)):
    model = SARIMAX(series, order=order, seasonal_order=seasonal_order, enforce_stationarity=False, enforce_invertibility=False)
    return model.fit(disp=False)

def _metrics(y_true, y_pred):
    mae = mean_absolute_error(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred, squared=False)
    mape = (abs((y_true - y_pred) / y_true).replace([float("inf")], float("nan"))).dropna().mean() * 100
    return {"MAE": float(mae), "RMSE": float(rmse), "MAPE_pct": float(mape)}

def train_models(series: pd.Series, features: pd.DataFrame, artifacts_dir: str, sarimax_cfg: dict,
                 xgb_cfg: dict | None, test_size_days: int = 60) -> TrainResult:
    os.makedirs(artifacts_dir, exist_ok=True)
    df = features.copy(); df['price'] = series; df = df.dropna()
    y = df['price']; X = df.drop(columns=['price'])
    cutoff = y.index.max() - pd.Timedelta(days=test_size_days)
    y_train, y_test = y[y.index <= cutoff], y[y.index > cutoff]
    X_train, X_test = X.loc[y_train.index], X.loc[y_test.index]

    sar_train = fit_sarimax(y_train, order=tuple(sarimax_cfg.get("order",(1,1,1))),
                            seasonal_order=tuple(sarimax_cfg.get("seasonal_order",(0,1,1,7))))
    base_pred_test = sar_train.get_forecast(steps=len(y_test)).predicted_mean
    base_pred_test.index = y_test.index
    metrics_base = _metrics(y_test, base_pred_test)

    xgb_path = None
    if xgb_cfg and xgb_cfg.get("enabled", True):
        xgb = XGBRegressor(
            n_estimators=xgb_cfg.get("n_estimators", 400),
            max_depth=xgb_cfg.get("max_depth", 4),
            learning_rate=xgb_cfg.get("learning_rate", 0.05),
            subsample=0.9, colsample_bytree=0.9, objective="reg:squarederror", random_state=42
        )
        resid_train = (y_train - sar_train.fittedvalues.reindex(y_train.index).ffill()).dropna()
        xgb.fit(X_train.loc[resid_train.index], resid_train.values)
        resid_pred_test = pd.Series(xgb.predict(X_test), index=X_test.index)
        metrics_hybrid = _metrics(y_test, (base_pred_test + resid_pred_test).reindex(y_test.index))
    else:
        xgb = None; metrics_hybrid = metrics_base

    sar_full = fit_sarimax(y, order=tuple(sarimax_cfg.get("order",(1,1,1))),
                           seasonal_order=tuple(sarimax_cfg.get("seasonal_order",(0,1,1,7))))
    sar_path = os.path.join(artifacts_dir, "sarimax.pkl"); joblib.dump(sar_full, sar_path)

    if xgb is not None:
        base_fit = sar_full.fittedvalues.reindex(y.index).ffill()
        resid_full = (y - base_fit).dropna()
        xgb.fit(X.loc[resid_full.index], resid_full.values)
        xgb_path = os.path.join(artifacts_dir, "xgb.pkl"); joblib.dump(xgb, xgb_path)

    return TrainResult(sarimax_model_path=sar_path, xgb_model_path=xgb_path, metrics={"baseline":metrics_base,"hybrid":metrics_hybrid})
