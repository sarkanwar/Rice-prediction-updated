
# --- Streamlit entry (v3) with unique widget keys ---
import sys, pathlib

ROOT = pathlib.Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
for sub in ("data_sources", "features", "model"):
    p = str(ROOT / sub)
    if p not in sys.path:
        sys.path.insert(0, p)
    initf = ROOT / sub / "__init__.py"
    if (ROOT / sub).is_dir() and not initf.exists():
        try: initf.write_text("", encoding="utf-8")
        except Exception: pass

import streamlit as st

from pipeline import run_pipeline
from data_sources.agmarknet_api import fetch_basmati_prices_csv
from data_sources.data_gov_india import fetch_datagov_prices_csv

st.set_page_config(page_title="Rice Predictions", page_icon="🌾", layout="wide")
st.title("🌾 Rice Price Forecast (fixed keys)")

# --- Agmarknet block ---
with st.expander("Fetch data from Agmarknet (CEDA API)", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        state = st.text_input("State", "Haryana", key="ag_state")
        market = st.text_input("Market", "Karnal", key="ag_market")
        variety = st.text_input("Variety keywords (comma-separated)", "Basmati,1121,1509,1718,PB-1", key="ag_variety")
    with col2:
        date_from = st.text_input("From (YYYY-MM-DD)", "", key="ag_from")
        date_to   = st.text_input("To (YYYY-MM-DD)", "", key="ag_to")
        out_csv   = st.text_input("Save to CSV", "data/basmati_prices.csv", key="ag_outcsv")
    if st.button("Fetch from Agmarknet", key="ag_btn"):
        keys = [k.strip() for k in variety.split(",") if k.strip()]
        try:
            path = fetch_basmati_prices_csv(out_csv, state or None, market or None, keys, date_from or None, date_to or None)
            st.success(f"Saved: {path}")
        except Exception as e:
            st.exception(e)

# --- data.gov.in block ---
with st.expander("Fetch data from data.gov.in (Retail/Wholesale)", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        api_key     = st.text_input("API key", type="password", key="dg_api")
        resource_id = st.text_input("Resource ID", "", key="dg_resid")
        commodity   = st.text_input("Commodity filter", "Rice", key="dg_comm")
    with col2:
        state2   = st.text_input("State (optional)", "", key="dg_state")
        centre   = st.text_input("Centre/City (optional)", "", key="dg_centre")
        dfrom2   = st.text_input("From (YYYY-MM-DD)", "", key="dg_from")
        dto2     = st.text_input("To (YYYY-MM-DD)", "", key="dg_to")
        out_csv2 = st.text_input("Save to CSV", "data/basmati_prices.csv", key="dg_outcsv")
    if st.button("Fetch from data.gov.in", key="dg_btn"):
        if not api_key or not resource_id:
            st.error("Please enter API key and resource_id")
        else:
            try:
                path = fetch_datagov_prices_csv(api_key, resource_id, out_csv2, commodity, state2 or None, centre or None, dfrom2 or None, dto2 or None)
                st.success(f"Saved: {path}")
            except Exception as e:
                st.exception(e)

st.divider()
st.subheader("Run Forecast")
c1, c2, c3 = st.columns(3)
h1 = c1.number_input("Horizon 1 (days)", min_value=1, value=7, key="hz1")
h2 = c2.number_input("Horizon 2 (days)", min_value=1, value=30, key="hz2")
h3 = c3.number_input("Horizon 3 (days)", min_value=1, value=180, key="hz3")
if st.button("Train & Forecast", key="train_btn"):
    with st.spinner("Training & forecasting..."):
        try:
            run_pipeline("config.yaml", horizons=[h1, h2, h3])
            st.success("Done! Check artifacts/ for CSVs & charts.")
        except Exception as e:
            st.exception(e)

st.caption("Tip: Replace data/basmati_prices.csv with your real series (Date,Price).")
