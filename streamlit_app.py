
import os, datetime
import streamlit as st
import pandas as pd

from data_gov_india import fetch_datagov_prices_csv

st.set_page_config(page_title="Rice Fetcher — Minimal", page_icon="🌾", layout="wide")
st.title("🌾 Rice Data Fetcher (Minimal, Self-contained)")

with st.expander("Fetch from data.gov.in", expanded=True):
    col1, col2 = st.columns(2)
    with col1:
        api_key     = st.text_input("API key", type="password", key="api_key")
        resource_id = st.text_input("Resource ID (UUID or full URL)", "", key="res_id")
        commodity   = st.text_input("Commodity filter", "Rice", key="comm")
    with col2:
        state   = st.text_input("State (optional)", "", key="state")
        centre  = st.text_input("Centre/City (optional)", "", key="centre")
        dfrom   = st.text_input("From (YYYY-MM-DD)", "", key="from")
        dto     = st.text_input("To (YYYY-MM-DD)", "", key="to")

    st.markdown("**Save options**")
    c3, c4 = st.columns([2,1])
    with c3:
        base_name = st.text_input("Output file name (no folders)", "basmati_prices.csv", key="outname")
    with c4:
        add_ts = st.checkbox("Add date suffix", value=True, key="addts")

    if st.button("Fetch data", key="fetch"):
        if not api_key or not resource_id:
            st.error("Please enter API key and resource_id")
        else:
            # Force root save only; optionally add timestamp
            name_only = os.path.basename(base_name.strip() or "basmati_prices.csv")
            if add_ts:
                stem, ext = os.path.splitext(name_only)
                name_only = f"{stem}_{datetime.date.today().isoformat()}{ext or '.csv'}"
            try:
                path = fetch_datagov_prices_csv(
                    api_key=api_key,
                    resource_id=resource_id,
                    out_csv=name_only,            # <-- always root
                    commodity_filter=commodity,
                    state=state or None,
                    centre=centre or None,
                    date_from=dfrom or None,
                    date_to=dto or None,
                    prefer_csv=False
                )
                st.success(f"Saved: {path}")
                try:
                    df = pd.read_csv(path)
                    st.dataframe(df.tail(20))
                    st.download_button("Download CSV", data=df.to_csv(index=False).encode("utf-8"),
                                       file_name=os.path.basename(path), mime="text/csv", key="dl")
                except Exception as e:
                    st.info(f"File saved to {path}. (Preview failed: {e})")
            except Exception as e:
                st.exception(e)

st.caption("Tip: Resource ID must be the dataset UUID (or you can paste the full URL; the app will extract it).")
