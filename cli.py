
from __future__ import annotations
import typer
from pipeline import run_pipeline
from data_sources.agmarknet_api import fetch_basmati_prices_csv
from data_sources.data_gov_india import fetch_datagov_prices_csv

app = typer.Typer(help="Rice Predictions CLI")

@app.command("run-all")
def run_all(config: str = typer.Option("config.yaml", help="Config file"),
            horizons: list[int] = typer.Option(None, help="Horizons in days, e.g., --horizons 7 30 180")):
    run_pipeline(config_path=config, horizons=horizons)

@app.command("fetch-agmarknet")
def fetch_agmarknet(out_csv: str = typer.Option("data/basmati_prices.csv"),
                    state: str = typer.Option(None), market: str = typer.Option(None),
                    variety_keywords: str = typer.Option("Basmati,1121,1509,1718,PB-1"),
                    date_from: str = typer.Option(None), date_to: str = typer.Option(None),
                    commodity_name: str = typer.Option("Paddy")):
    keys = [k.strip() for k in variety_keywords.split(",") if k.strip()]
    path = fetch_basmati_prices_csv(out_csv, state, market, keys, date_from, date_to, commodity_name)
    typer.echo(f"Saved: {path}")

@app.command("fetch-datagov")
def fetch_datagov(api_key: str = typer.Option(...),
                  resource_id: str = typer.Option(...),
                  out_csv: str = typer.Option("data/basmati_prices.csv"),
                  commodity: str = typer.Option("Rice"),
                  state: str = typer.Option(None), centre: str = typer.Option(None),
                  date_from: str = typer.Option(None), date_to: str = typer.Option(None)):
    path = fetch_datagov_prices_csv(api_key, resource_id, out_csv, commodity, state, centre, date_from, date_to)
    typer.echo(f"Saved: {path}")

if __name__ == "__main__":
    app()
