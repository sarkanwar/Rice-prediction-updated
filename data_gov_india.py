
from __future__ import annotations
import pandas as pd, requests, re

BASE = "https://api.data.gov.in/resource"

def _extract_resource_id(s: str) -> str:
    """
    Accepts either a raw UUID-like resource id or a full URL.
    Returns the sanitized resource id (no leading 'resource/').
    """
    s = (s or "").strip()
    # If it's a URL, extract the last path segment
    m = re.search(r"/resource/([A-Za-z0-9\-]+)$", s)
    if m:
        return m.group(1)
    # If it starts with 'resource/', strip it
    if s.startswith("resource/"):
        return s.split("/", 1)[1]
    # Otherwise assume it's already the id
    return s

def fetch_datagov_prices_csv(api_key: str, resource_id: str, out_csv: str, commodity_filter: str = "Rice",
                             state: str | None = None, centre: str | None = None, date_from: str | None = None,
                             date_to: str | None = None) -> str:
    session = requests.Session()
    rid = _extract_resource_id(resource_id)
    if not rid:
        raise ValueError("Invalid Resource ID. Provide the UUID-like id (e.g., 9ef84268-d588-465a-a308-a864a43d0070).")
    url = f"{BASE}/{rid}"
    limit, offset, rows = 1000, 0, []

    while True:
        params = {"api-key": api_key, "format": "json", "limit": limit, "offset": offset}
        # Not all datasets support 'from'/'to'. We'll include them only if provided.
        if date_from: params["from"] = date_from
        if date_to: params["to"] = date_to
        try:
            r = session.get(url, params=params, timeout=45)
            # Special-case helpful guidance for common mistakes
            if r.status_code in (404, 405):
                raise requests.HTTPError(
                    f"{r.status_code} for {url}. "
                    f"Tip: Make sure your Resource ID is just the UUID (not 'resource/<id>' or a full URL). "
                    f"Currently using rid='{rid}'. Full request: {r.url}",
                    response=r
                )
            r.raise_for_status()
        except requests.HTTPError as e:
            # Re-raise with clearer context
            raise requests.HTTPError(str(e), response=getattr(e, "response", None)) from e

        chunk = r.json().get("records", [])
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < limit:
            break
        offset += limit

    df = pd.DataFrame(rows)
    if df.empty:
        # Create an empty CSV with expected schema
        pd.DataFrame(columns=["Date","Price"]).to_csv(out_csv, index=False)
        return out_csv

    # Normalize typical schemas
    df.columns = [c.lower() for c in df.columns]

    # Optional filtering
    if "commodity" in df.columns and commodity_filter:
        df = df[df["commodity"].str.contains(commodity_filter, case=False, na=False)]
    if state and "state" in df.columns:
        df = df[df["state"].str.contains(state, case=False, na=False)]
    if centre and "centre" in df.columns:
        df = df[df["centre"].str.contains(centre, case=False, na=False)]

    # Identify date & price-like columns
    date_candidates = [c for c in ["date","reported_date","price_date","created_date","month","day"] if c in df.columns]
    if not date_candidates:
        # Fallback: find any column that looks like a date
        for c in df.columns:
            try:
                pd.to_datetime(df[c])
                date_candidates.append(c); break
            except Exception:
                pass
    if not date_candidates:
        raise ValueError("No date-like column found in dataset; please choose a different resource.")

    price_candidates = [c for c in ["retail","wholesale","modal_price","price","wholesale_price","retail_price"] if c in df.columns]
    if not price_candidates:
        # Fallback: pick first numeric column
        num_cols = [c for c in df.columns if pd.to_numeric(df[c], errors="coerce").notna().any()]
        if not num_cols:
            raise ValueError("No numeric price column detected in this resource.")
        price_candidates = [num_cols[0]]

    date_col = date_candidates[0]
    price_col = price_candidates[0]

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df.dropna(subset=[date_col, price_col])

    # Aggregate to daily average price
    daily = df.groupby(df[date_col].dt.date, as_index=False)[price_col].mean().rename(columns={date_col:"Date", price_col:"Price"})
    daily = daily.sort_values("Date")
    daily.to_csv(out_csv, index=False)
    return out_csv
