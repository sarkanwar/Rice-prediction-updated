
# --- pipeline.py (v3) with heavy-duty path bootstrapping + dual import modes ---
from __future__ import annotations
import sys, pathlib, os
ROOT = pathlib.Path(__file__).resolve().parent

# 1) Put repo root and subfolders on sys.path
paths = [str(ROOT), str(ROOT / "data_sources"), str(ROOT / "features"), str(ROOT / "model")]
for p in paths:
    if p not in sys.path:
        sys.path.insert(0, p)

# 2) Ensure __init__.py exists in module folders (so package-style works)
for sub in ("data_sources", "features", "model"):
    d = ROOT / sub
    if d.is_dir():
        initf = d / "__init__.py"
        if not initf.exists():
            try:
                initf.write_text("", encoding="utf-8")
            except Exception:
                pass

import pandas as pd
from utils import load_config, ensure_dir, today_str

# 3) Try package imports first, then flat
try:
    from data_sources.csv_source import load_price_csv
    from data_sources.yfinance_source import fetch_yf
except Exception:
    from csv_source import load_price_csv
    from yfinance_source import fetch_yf

try:
    from features.tech_indicators import rolling_features
    from features.weather import aggregate_regions
except Exception:
    from tech_indicators import rolling_features
    from weather import aggregate_regions

try:
    from model.train import train_models
    from model.infer import forecast
except Exception:
    from train import train_models
    from infer import forecast

def build_features(price_s: pd.Series, cfg: dict) -> pd.DataFrame:
    df = rolling_features(price_s)
    ind_cfg = cfg.get("indicators", {})
    for key, meta in ind_cfg.items():
        if not meta or not meta.get("enabled", False): 
            continue
        s = fetch_yf(meta.get("ticker"), meta.get("lookback_days", 365))
        if s is None or s.empty:
            continue
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
        feats = feats.drop(columns=['price'], errors='ignore').fillna(method='ffill').fillna(method='bfill')
        return feats
    return _builder

def run_pipeline(config_path: str = "config.yaml", horizons=None):
    cfg = load_config(config_path)
    price_s = load_price_csv(cfg["price_csv"])
    feats = build_features(price_s, cfg)
    out_root = os.path.join("artifacts", today_str()); ensure_dir(out_root)
    models_dir = os.path.join("artifacts", "models"); ensure_dir(models_dir)
    tr = train_models(
        series=price_s, features=feats, artifacts_dir=models_dir,
        sarimax_cfg=cfg.get("model", {}).get("sarimax", {}),
        xgb_cfg=cfg.get("model", {}).get("xgboost", {}),
        test_size_days=cfg.get("model", {}).get("test_size_days", 60),
    )
    hz = horizons or cfg.get("horizons", [7, 30, 180])
    fut_builder = make_future_features_builder(cfg)
    forecast(tr.sarimax_model_path, tr.xgb_model_path, price_s, fut_builder, hz, out_root, "forecast")
    print("Training metrics:", tr.metrics)
    print(f"Done. Artifacts at: {out_root}")
