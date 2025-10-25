
from __future__ import annotations
import os, pandas as pd, numpy as np, joblib, matplotlib.pyplot as plt
from typing import Optional
from statsmodels.tsa.statespace.sarimax import SARIMAXResults

def load_sarimax(path: str) -> SARIMAXResults: return joblib.load(path)
def load_xgb(path: Optional[str]):
    if path and os.path.exists(path): return joblib.load(path)
    return None

def forecast(sarimax_path: str, xgb_path: str | None, history_series: pd.Series, feature_maker,
             horizons: list[int], out_dir: str, title_prefix: str = "forecast"):
    os.makedirs(out_dir, exist_ok=True)
    sar = load_sarimax(sarimax_path); xgb = load_xgb(xgb_path)
    max_h = max(horizons)
    sar_fore = sar.get_forecast(steps=max_h)
    base_mean = sar_fore.predicted_mean
    conf = sar_fore.conf_int(alpha=0.05)
    lower, upper = conf.iloc[:,0], conf.iloc[:,1]
    fut_idx = pd.date_range(history_series.index.max() + pd.Timedelta(days=1), periods=max_h, freq="D")
    fut_feats = feature_maker(history_series, fut_idx)
    if xgb is not None and fut_feats is not None and not fut_feats.empty:
        resid = pd.Series(xgb.predict(fut_feats), index=fut_idx)
        adj = base_mean.copy(); adj.loc[fut_idx] = base_mean.values + resid.values
    else:
        adj = base_mean
    outs = {}
    for h in horizons:
        idx = fut_idx[:h]
        df = pd.DataFrame({"date": idx, "forecast": adj.loc[idx].values, "lower_95": lower.loc[idx].values, "upper_95": upper.loc[idx].values})
        df.to_csv(os.path.join(out_dir, f"{title_prefix}_{h}d.csv"), index=False); outs[h]=df
        plt.figure(); hist_tail = history_series.last("120D")
        plt.plot(hist_tail.index, hist_tail.values, label="History")
        plt.plot(df["date"], df["forecast"], label=f"Forecast {h}d")
        plt.fill_between(df["date"], df["lower_95"], df["upper_95"], alpha=0.2, label="95% PI")
        plt.title(f"Rice Price {h}-Day Forecast"); plt.xlabel("Date"); plt.ylabel("Price"); plt.legend(); plt.tight_layout()
        plt.savefig(os.path.join(out_dir, f"{title_prefix}_plot_{h}d.png"), dpi=120); plt.close()
    return outs
