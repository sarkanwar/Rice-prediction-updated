
# ✅ Final Data Fetcher Fix (data_gov_india.py)

This version fixes all previous issues:
- Automatically creates the parent folder before saving CSVs.
- Accepts either UUID or full dataset URL as Resource ID.
- Handles both JSON and CSV API responses.
- Prints clear error messages for invalid keys or dataset issues.

## How to use

1. Replace your existing `data_gov_india.py` with this one.
2. In your Streamlit app, keep:
   ```python
   out_csv2 = "data/basmati_prices.csv"
   ```
   — OR just `"basmati_prices.csv"` if you want it in the root folder.

3. If using `"data/...csv"`, make sure a `data/` folder exists in your repo once.

Deploy, and it will now create the folder automatically when saving.
