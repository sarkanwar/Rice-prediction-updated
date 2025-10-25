
# Minimal Rice Fetcher (Streamlit)

Self-contained app with **no package imports from your repo structure** (so no `data_sources` errors).  
**Main file path:** `streamlit_app.py`

## Deploy steps (Streamlit Cloud)
1) Create a new repo and upload these four files.
2) On share.streamlit.io → New app:
   - Repository: your repo
   - Branch: main
   - Main file path: **streamlit_app.py**
   - Python: 3.10 or 3.11
3) Run the app, fill **API key** + **Resource ID**, click **Fetch data**.

The CSV is saved in the app root (e.g., `basmati_prices_YYYY-MM-DD.csv`). No `data/` folder required.
