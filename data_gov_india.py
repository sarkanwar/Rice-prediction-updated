
from __future__ import annotations
import pandas as pd, requests, io, re

BASE = "https://api.data.gov.in/resource"

def _extract_resource_id(s: str) -> str:
    """
    Accepts either a raw UUID-like resource id or a full URL.
    Returns the sanitized resource id (no leading 'resource/').
    """
    s = (s or "").strip()
    m = re.search(r"/resource/([A-Za-z0-9\-]+)$", s)
    if m:
        return m.group(1)
    if s.startswith("resource/"):
        return s.split("/", 1)[1]
    return s

def _ensure_json_response(r: requests.Response, url: str, rid: str):
    """Raise a helpful error if the server did not return JSON."""
    ct = (r.headers.get("content-type") or "").lower()
    text_head = (r.text or "")[:300]
    if "json" not in ct:
        # Common cases: HTML error page, CSV returned despite format=json, or 200 OK with HTML from a proxy
        hint = (
            f"Non-JSON response from {url}\n"
            f"Content-Type: {ct}\n"
            f"First 300 chars: {text_head}\n\n"
            "Check that:\n"
            "  1) Your API key is correct and active\n"
            "  2) Resource ID is just the UUID (e.g., 9ef8...0070)\n"
            "  3) You didn't exceed your quota\n"
            "  4) The dataset actually supports 'format=json'\n"
        )
        raise ValueError(hint)

def fetch_datagov_prices_csv(api_key: str, resource_id: str, out_csv: str, commodity_filter: str = "Rice",
                             state: str | None = None, centre: str | None = None, date_from: str | None = None,
                             date_to: str | None = None, prefer_csv: bool = False) -> str:
    """
    Fetch a dataset from data.gov.in and save a clean Date,Price CSV.

    - `resource_id`: UUID like '9ef84268-d588-465a-a308-a864a43d0070' (you may also paste the full URL)
    - If `prefer_csv=True`, will request CSV and parse it when available.
    """
    session = requests.Session()
    rid = _extract_resource_id(resource_id)
    if not rid:
        raise ValueError("Invalid Resource ID. Provide the UUID-like id (e.g., 9ef84268-d588-465a-a308-a864a43d0070).")
    url = f"{BASE}/{rid}"
    limit, offset, rows = 1000, 0, []

    while True:
        params = {"api-key": api_key, "limit": limit, "offset": offset}
        if prefer_csv:
            params["format"] = "csv"
        else:
            params["format"] = "json"
        if date_from: params["from"] = date_from
        if date_to: params["to"] = date_to

        r = session.get(url, params=params, timeout=45)
        # Helpful guidance for 404/405
        if r.status_code in (404, 405):
            raise requests.HTTPError(
                f"{r.status_code} for {url}. "
                f"Tip: Resource ID must be just the UUID (not a full URL). rid='{rid}'. Full request: {r.url}",
                response=r
            )
        r.raise_for_status()

        # Parse as CSV if requested and returned as CSV
        ct = (r.headers.get("content-type") or "").lower()
        if prefer_csv or "csv" in ct:
            try:
                df_chunk = pd.read_csv(io.StringIO(r.text))
            except Exception as e:
                raise ValueError(f"Failed to parse CSV from {r.url}: {e}") from e
            if df_chunk.empty:
                break
            rows.append(df_chunk)
            # CSV export usually returns all rows without pagination; break
            break
        else:
            # Expect JSON
            _ensure_json_response(r, url, rid)
            try:
                payload = r.json()
            except ValueError as e:
                # JSONDecodeError -> show first 300 chars for debugging
                head = (r.text or "")[:300]
                raise ValueError(f"Could not decode JSON from {r.url}. First 300 chars: {head}") from e

            chunk = payload.get("records", [])
            if not chunk:
                break
            rows.extend(chunk)
            if len(chunk) < limit:
                break
            offset += limit

    # Build DataFrame from rows
    if not rows:
        pd.DataFrame(columns=["Date","Price"]).to_csv(out_csv, index=False)
        return out_csv

    if isinstance(rows[0], pd.DataFrame):
        df = pd.concat(rows, ignore_index=True)
    else:
        df = pd.DataFrame(rows)

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
