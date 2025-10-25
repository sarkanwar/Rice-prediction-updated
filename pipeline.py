
from __future__ import annotations
import os, pandas as pd
from utils import load_config, ensure_dir, today_str
from data_sources.csv_source import load_price_csv
from data_sources.yfinance_source import fetch_yf
from features.tech_indicators import rolling_features
from features.weather import aggregate_regions
from model.train import train_models
from model.infer import forecast

def build_features(price_s: pd.Series, cfg: dict) -> pd.DataFrame:
    df = rolling_features(price_s)
    ind_cfg = cfg.get("indicators", {})
    for key, meta in ind_cfg.items():
        if not meta or not meta.get("enabled", False): continue
        s = fetch_yf(meta.get("ticker"), meta.get("lookback_days", 365))
        if s.empty: continue
        s = s.reindex(df.index).ffill()
        df[f"ind_{key}"] = s
        for l in [1,3,7,14,30]:
            df[f"ind_{key}_lag{l}"] = s.shift(l)
    w_cfg = cfg.get("weather", {})
    if w_cfg.get("enabled", False) and w_cfg.get("regions"):
        wdf = aggregate_regions(w_cfg["regions"], past_days=max(365, len(df)))
        wdf = wdf.reindex(df.index).ffill()
        df = df.join(wdf, how="left")
        for col in [c for c in wdf.columns if c.endswith("_avg")]:
            for l in [1,3,7,14]:
                df[f"{col}_lag{l}"] = wdf[col].shift(l)
    return df

def make_future_features_builder(cfg: dict):
    def _builder(history_series: pd.Series, future_index: pd.DatetimeIndex) -> pd.DataFrame:
        combined = history_series.copy()
        if len(future_index):
            ext = pd.Series([combined.iloc[-1]] * len(future_index), index=future_index, name=combined.name)
            combined = pd.concat([combined, ext])
        feats = build_features(combined, cfg).loc[future_index]
        return feats.drop(columns=['price'], errors='ignore').fillna(method='ffill').fillna(method='bfill')
    return _builder

def run_pipeline(config_path: str = "config.yaml", horizons=None):
    cfg = load_config(config_path)
    price_s = load_price_csv(cfg["price_csv"])
    feats = build_features(price_s, cfg)
    out_root = os.path.join("artifacts", today_str()); ensure_dir(out_root)
    models_dir = os.path.join("artifacts", "models"); ensure_dir(models_dir)
    tr = train_models(price_s, feats, models_dir, cfg.get("model",{}).get("sarimax",{}),
                      cfg.get("model",{}).get("xgboost",{}), cfg.get("model",{}).get("test_size_days",60))
    hz = horizons or cfg.get("horizons", [7,30,180])
    fut_builder = make_future_features_builder(cfg)
    forecast(tr.sarimax_model_path, tr.xgb_model_path, price_s, fut_builder, hz, out_root, "forecast")
    print("Training metrics:", tr.metrics); print(f"Done. Artifacts at: {out_root}")
