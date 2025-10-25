
from __future__ import annotations
import pandas as pd, requests
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

def fetch_weather_daily(lat: float, lon: float, past_days: int = 365) -> pd.DataFrame:
    params = {"latitude": lat, "longitude": lon, "past_days": min(past_days,92),
              "daily": "temperature_2m_mean,precipitation_sum", "timezone": "auto"}
    r = requests.get(OPEN_METEO_URL, params=params, timeout=30); r.raise_for_status()
    js = r.json()
    df = pd.DataFrame({"date": pd.to_datetime(js["daily"]["time"]),
                       "temp_mean": js["daily"]["temperature_2m_mean"],
                       "precip": js["daily"]["precipitation_sum"]}).set_index("date").asfreq("D").ffill()
    return df

def aggregate_regions(regions, past_days: int = 365):
    frames = []
    for reg in regions:
        df = fetch_weather_daily(reg["lat"], reg["lon"], past_days=past_days).add_prefix(f'{reg["name"]}_')
        frames.append(df)
    out = pd.concat(frames, axis=1).ffill()
    out["temp_mean_avg"] = out.filter(like="_temp_mean").mean(axis=1)
    out["precip_sum_avg"] = out.filter(like="_precip").sum(axis=1)
    return out
