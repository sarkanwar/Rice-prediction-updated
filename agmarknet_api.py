
from __future__ import annotations
import pandas as pd, requests
from typing import List, Optional
BASE = "https://api.ceda.ashoka.edu.in"

class AgmarknetClient:
    def __init__(self, base_url: str = BASE, timeout: int = 30):
        self.base = base_url.rstrip("/"); self.timeout = timeout
    def _get(self, path: str, params: dict | None = None):
        r = requests.get(f"{self.base}{path}", params=params, timeout=self.timeout); r.raise_for_status(); return r.json()
    def prices(self, commodity: Optional[str]=None, variety: Optional[str]=None, state: Optional[str]=None, market: Optional[str]=None,
               date_from: Optional[str]=None, date_to: Optional[str]=None, limit: int = 100000) -> pd.DataFrame:
        p = {"limit": limit}
        if commodity: p["commodity"]=commodity
        if variety: p["variety"]=variety
        if state: p["state"]=state
        if market: p["market"]=market
        if date_from: p["from"]=date_from
        if date_to: p["to"]=date_to
        data = self._get("/agmarknet/prices", params=p)
        df = pd.DataFrame(data)
        if df.empty: return df
        ren = {"date":"Date","modal_price":"ModalPrice","min_price":"MinPrice","max_price":"MaxPrice",
               "market":"Market","state":"State","variety":"Variety","commodity":"Commodity"}
        df = df.rename(columns={k:v for k,v in ren.items() if k in df.columns})
        if "Date" in df.columns: df["Date"] = pd.to_datetime(df["Date"]).dt.date
        return df

def fetch_basmati_prices_csv(out_csv: str, state: str | None = None, market: str | None = None,
                             variety_keywords: List[str] | None = None, date_from: str | None = None,
                             date_to: str | None = None, commodity_name: str = "Paddy") -> str:
    df = AgmarknetClient().prices(commodity=commodity_name, state=state, market=market, date_from=date_from, date_to=date_to)
    if df.empty:
        pd.DataFrame(columns=["Date","Price"]).to_csv(out_csv, index=False); return out_csv
    if variety_keywords and "Variety" in df.columns:
        pat = "|".join([str(x) for x in variety_keywords]); df = df[df["Variety"].str.contains(pat, case=False, na=False)].copy()
    if "ModalPrice" in df.columns:
        daily = df.groupby("Date", as_index=False)["ModalPrice"].mean().rename(columns={"ModalPrice":"Price"})
    elif {"MinPrice","MaxPrice"}.issubset(df.columns):
        tmp = df.groupby("Date", as_index=False)[["MinPrice","MaxPrice"]].mean(); tmp["Price"]=(tmp["MinPrice"]+tmp["MaxPrice"])/2.0
        daily = tmp[["Date","Price"]]
    else:
        daily = df.groupby("Date", as_index=False).size(); daily["Price"]=float("nan"); daily=daily[["Date","Price"]]
    daily.sort_values("Date").to_csv(out_csv, index=False); return out_csv
