
# 🌾 rice-predictions

Streamlit app + CLI to fetch rice/basmati prices (Agmarknet via CEDA, data.gov.in),
engineer indicators (technicals, currency, weather), train SARIMAX + XGBoost, and
produce 1‑week / 1‑month / 6‑month forecasts.

## Run locally
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Deploy on Streamlit Cloud
- Main file path: `streamlit_app.py`
- Python: 3.10 or 3.11

## CLI examples
```bash
python cli.py fetch-agmarknet --state "Haryana" --market "Karnal"   --variety_keywords "Basmati,1121,1509,1718,PB-1"   --date_from 2023-01-01 --date_to 2025-10-25 --out_csv data/basmati_prices.csv

python cli.py run-all --horizons 7 30 180
```
