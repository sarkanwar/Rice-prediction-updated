
from __future__ import annotations
import pandas as pd, requests
from typing import Optional
BASE = "https://api.data.gov.in/resource"

def fetch_datagov_prices_csv(api_key: str, resource_id: str, out_csv: str, commodity_filter: str = "Rice",
                             state: str | None = None, centre: str | None = None, date_from: str | None = None,
                             date_to: str | None = None) -> str:
    session = requests.Session(); url = f"{BASE}/{resource_id}"; limit, offset, rows = 1000, 0, []
    while True:
        params = {"api-key": api_key, "format": "json", "limit": limit, "offset": offset}
        if date_from: params["from"]=date_from
        if date_to: params["to"]=date_to
        r = session.get(url, params=params, timeout=45); r.raise_for_status()
        chunk = r.json().get("records", [])
        if not chunk: break
        rows.extend(chunk)
        if len(chunk) < limit: break
        offset += limit
    df = pd.DataFrame(rows)
    if df.empty: pd.DataFrame(columns=["Date","Price"]).to_csv(out_csv, index=False); return out_csv
    df.columns = [c.lower() for c in df.columns]
    if "commodity" in df.columns and commodity_filter: df = df[df["commodity"].str.contains(commodity_filter, case=False, na=False)]
    if state and "state" in df.columns: df = df[df["state"].str.contains(state, case=False, na=False)]
    if centre and "centre" in df.columns: df = df[df["centre"].str.contains(centre, case=False, na=False)]
    date_col = next((c for c in ["date","reported_date","price_date"] if c in df.columns), None)
    if not date_col: raise ValueError("No date column found")
    price_col = next((c for c in ["retail","wholesale","modal_price","price"] if c in df.columns), None)
    if not price_col:
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(pd.to_numeric(df[c], errors="coerce"))]
        if not num_cols: raise ValueError("No numeric price column detected")
        price_col = num_cols[0]
    df[date_col] = pd.to_datetime(df[date_col]).dt.date; df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df.dropna(subset=[price_col])
    daily = df.groupby(date_col, as_index=False)[price_col].mean().rename(columns={date_col:"Date", price_col:"Price"})
    daily.sort_values("Date").to_csv(out_csv, index=False); return out_csv
