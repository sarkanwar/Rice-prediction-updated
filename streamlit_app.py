
# Robust Streamlit entry: ensure root is importable
import sys, pathlib
ROOT = pathlib.Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from pipeline import run_pipeline
from data_sources.agmarknet_api import fetch_basmati_prices_csv
from data_sources.data_gov_india import fetch_datagov_prices_csv

st.set_page_config(page_title="Rice Predictions", page_icon="🌾", layout="wide")
st.title("🌾 Rice Price Forecast")

with st.expander("Fetch data from Agmarknet (CEDA API)", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        state = st.text_input("State", "Haryana")
        market = st.text_input("Market", "Karnal")
        variety = st.text_input("Variety keywords (comma-separated)", "Basmati,1121,1509,1718,PB-1")
    with col2:
        date_from = st.text_input("From (YYYY-MM-DD)", "")
        date_to   = st.text_input("To (YYYY-MM-DD)", "")
        out_csv   = st.text_input("Save to CSV", "data/basmati_prices.csv")
    if st.button("Fetch from Agmarknet"):
        keys = [k.strip() for k in variety.split(",") if k.strip()]
        try:
            path = fetch_basmati_prices_csv(out_csv, state or None, market or None, keys, date_from or None, date_to or None)
            st.success(f"Saved: {path}")
        except Exception as e:
            st.exception(e)

with st.expander("Fetch data from data.gov.in (Retail/Wholesale)", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        api_key     = st.text_input("API key", type="password")
        resource_id = st.text_input("Resource ID", "")
        commodity   = st.text_input("Commodity filter", "Rice")
    with col2:
        state2   = st.text_input("State (optional)", "")
        centre   = st.text_input("Centre/City (optional)", "")
        dfrom2   = st.text_input("From (YYYY-MM-DD)", "")
        dto2     = st.text_input("To (YYYY-MM-DD)", "")
        out_csv2 = st.text_input("Save to CSV", "data/basmati_prices.csv")
    if st.button("Fetch from data.gov.in"):
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
h1 = c1.number_input("Horizon 1 (days)", min_value=1, value=7)
h2 = c2.number_input("Horizon 2 (days)", min_value=1, value=30)
h3 = c3.number_input("Horizon 3 (days)", min_value=1, value=180)
if st.button("Train & Forecast"):
    with st.spinner("Training & forecasting..."):
        try:
            run_pipeline("config.yaml", horizons=[h1, h2, h3])
            st.success("Done! Check artifacts/ for CSVs & charts.")
        except Exception as e:
            st.exception(e)

st.caption("Tip: Replace data/basmati_prices.csv with your real series (Date,Price).")
