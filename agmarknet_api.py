
from __future__ import annotations
import pandas as pd, requests
from typing import List, Optional

DEFAULT_BASE = "https://api.ceda.ashoka.edu.in"

class AgmarknetClient:
    def __init__(self, base_url: str = DEFAULT_BASE, endpoint_path: str = "/agmarknet/prices",
                 api_key: str | None = None, timeout: int = 30):
        self.base = base_url.rstrip("/")
        self.endpoint_path = endpoint_path if endpoint_path.startswith("/") else "/" + endpoint_path
        self.timeout = timeout
        self.api_key = api_key

    def _get(self, path: str, params: dict | None = None):
        headers = {}
        if self.api_key:
            headers["x-api-key"] = self.api_key
            if params is not None and "api-key" not in params:
                params = {**params, "api-key": self.api_key}
        url = f"{self.base}{path}"
        r = requests.get(url, params=params, headers=headers, timeout=self.timeout)
        if r.status_code >= 400:
            raise requests.HTTPError(
                f"HTTP {r.status_code} for {url} params={params}. "
                f"Response: {r.text[:300]}",
                response=r,
            )
        return r.json()

    def prices(self, commodity: Optional[str]=None, variety: Optional[str]=None, state: Optional[str]=None, market: Optional[str]=None,
               date_from: Optional[str]=None, date_to: Optional[str]=None, limit: int = 100000) -> pd.DataFrame:
        p = {"limit": limit}
        if commodity: p["commodity"]=commodity
        if variety: p["variety"]=variety
        if state: p["state"]=state
        if market: p["market"]=market
        if date_from: p["from"]=date_from
        if date_to: p["to"]=date_to

        data = self._get(self.endpoint_path, params=p)
        df = pd.DataFrame(data)
        if df.empty: return df
        ren = {"date":"Date","modal_price":"ModalPrice","min_price":"MinPrice","max_price":"MaxPrice",
               "market":"Market","state":"State","variety":"Variety","commodity":"Commodity"}
        df = df.rename(columns={k:v for k,v in ren.items() if k in df.columns})
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"]).dt.date
        return df

def fetch_basmati_prices_csv(out_csv: str, state: str | None = None, market: str | None = None,
                             variety_keywords: List[str] | None = None, date_from: str | None = None,
                             date_to: str | None = None, commodity_name: str = "Paddy",
                             base_url: str = DEFAULT_BASE, endpoint_path: str = "/agmarknet/prices",
                             api_key: str | None = None) -> str:
    df = AgmarknetClient(base_url=base_url, endpoint_path=endpoint_path, api_key=api_key).prices(
        commodity=commodity_name, state=state, market=market, date_from=date_from, date_to=date_to
    )
    if df.empty:
        pd.DataFrame(columns=["Date","Price"]).to_csv(out_csv, index=False); return out_csv
    if variety_keywords and "Variety" in df.columns:
        import re
        pat = "|".join([re.escape(str(x)) for x in variety_keywords])
        df = df[df["Variety"].str.contains(pat, case=False, na=False)].copy()
    if "ModalPrice" in df.columns:
        daily = df.groupby("Date", as_index=False)["ModalPrice"].mean().rename(columns={"ModalPrice":"Price"})
    elif {"MinPrice","MaxPrice"}.issubset(df.columns):
        tmp = df.groupby("Date", as_index=False)[["MinPrice","MaxPrice"]].mean(); tmp["Price"]=(tmp["MinPrice"]+tmp["MaxPrice"])/2.0
        daily = tmp[["Date","Price"]]
    else:
        daily = df.groupby("Date", as_index=False).size(); daily["Price"]=float("nan"); daily=daily[["Date","Price"]]
    daily.sort_values("Date").to_csv(out_csv, index=False); return out_csv
