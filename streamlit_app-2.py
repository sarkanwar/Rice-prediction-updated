
import sys, pathlib, os, datetime
ROOT = pathlib.Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st

# pipeline import is optional here; we only need fetcher. Keep it if you also run forecasts.
try:
    from pipeline import run_pipeline
except Exception:
    run_pipeline = None

# Import the data.gov.in fetcher (support both layouts)
try:
    from data_sources.data_gov_india import fetch_datagov_prices_csv
except Exception:
    from data_gov_india import fetch_datagov_prices_csv

st.set_page_config(page_title="Rice Fetcher — Root Save", page_icon="🧺", layout="wide")
st.title("🧺 Rice Data Fetcher (root save mode)")

with st.expander("Fetch from data.gov.in", expanded=True):
    col1, col2 = st.columns(2)
    with col1:
        api_key     = st.text_input("API key", type="password", key="dg_api")
        resource_id = st.text_input("Resource ID (UUID or full URL)", "", key="dg_resid")
        commodity   = st.text_input("Commodity filter", "Rice", key="dg_comm")
    with col2:
        state   = st.text_input("State (optional)", "", key="dg_state")
        centre  = st.text_input("Centre/City (optional)", "", key="dg_centre")
        dfrom   = st.text_input("From (YYYY-MM-DD)", "", key="dg_from")
        dto     = st.text_input("To (YYYY-MM-DD)", "", key="dg_to")

    st.markdown("**Save options**")
    c3, c4 = st.columns([2,1])
    with c3:
        base_name = st.text_input("Output file name (no folders)", "basmati_prices.csv", key="dg_basename")
    with c4:
        add_ts = st.checkbox("Add date suffix", value=True, key="dg_addts")

    if st.button("Fetch data", key="dg_btn"):
        if not api_key or not resource_id:
            st.error("Please enter API key and resource_id")
        else:
            # Force root save: strip any directories; optionally add timestamp
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
                    prefer_csv=False              # set True if JSON misbehaves
                )
                st.success(f"Saved: {path}")
                try:
                    import pandas as pd
                    df = pd.read_csv(path)
                    st.dataframe(df.tail(20))
                    st.download_button("Download CSV", data=df.to_csv(index=False).encode("utf-8"),
                                       file_name=os.path.basename(path), mime="text/csv")
                except Exception as e:
                    st.info(f"File saved to {path}. (Preview failed: {e})")
            except Exception as e:
                st.exception(e)
                st.info("Tip: Ensure Resource ID is the UUID (or full URL is okay), and the API key is valid.")

st.divider()
st.caption("This version always saves the file in the app's working directory. No 'data/' folder needed.")
