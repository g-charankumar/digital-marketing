from fastapi import FastAPI, HTTPException, Query
import pandas as pd
import os
from functools import lru_cache

app = FastAPI(title="Multi-Niche Dataset API")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')  # Folder where CSVs are stored

@lru_cache(maxsize=10)
def load_data_for_niche(niche: str) -> pd.DataFrame:
    niche = niche.strip().lower()
    filename = f"{niche}.csv"
    filepath = os.path.join(DATA_DIR, filename)

    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"No data available for niche '{niche}'")

    df = pd.read_csv(filepath)
    df.columns = [col.lower() for col in df.columns]

    # Add 'id' column if missing
    if 'id' not in df.columns:
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'id'}, inplace=True)
        df['id'] = df['id'] + 1

    return df

@app.get("/")
def root():
    return {
        "message": "Welcome to Multi-Niche Dataset API.",
        "usage": "Use /records?niche=your_niche to fetch data, or /records/{niche}/{end_id} to fetch records from 1 to end_id."
    }

@app.get("/records/{niche}")
def get_all_records(niche: str):
    try:
        df = load_data_for_niche(niche)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if df.empty:
        raise HTTPException(status_code=500, detail="Dataset not loaded or empty")

    # Return all records (or you can limit here if desired)
    return df.to_dict(orient="records")

@app.get("/records/{niche}/{end_id}")
def get_records_upto_id(niche: str, end_id: int):
    try:
        df = load_data_for_niche(niche)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if df.empty:
        raise HTTPException(status_code=500, detail="Dataset not loaded or empty")

    # Filter rows with id between 1 and end_id (inclusive)
    filtered_df = df[(df["id"] >= 1) & (df["id"] <= end_id)]

    if filtered_df.empty:
        raise HTTPException(status_code=404, detail="No records found in the given range")

    return filtered_df.to_dict(orient="records")
