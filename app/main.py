from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
from functools import lru_cache
from typing import List, Dict, Any

app = FastAPI(title="Multi-Niche Dataset API")

# Enable CORS (adjust origins as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change to your domain(s) for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')  # Folder where CSV files are stored

@lru_cache(maxsize=10)
def load_data_for_niche(niche: str) -> pd.DataFrame:
    niche = niche.strip().lower()
    filename = f"{niche}.csv"
    filepath = os.path.join(DATA_DIR, filename)

    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"No data available for niche '{niche}'")

    df = pd.read_csv(filepath)
    df.columns = [col.lower() for col in df.columns]

    if 'id' not in df.columns:
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'id'}, inplace=True)
        df['id'] = df['id'] + 1

    return df

@app.get("/")
def root():
    return {
        "message": "Welcome to Multi-Niche Dataset API.",
        "usage": "Use /records/{niche}?limit=your_limit to fetch data."
    }

@app.get("/records/{niche}", response_model=List[Dict[str, Any]])
def get_records(
    niche: str,
    limit: int = Query(
        None,
        ge=1,
        le=1000,
        description="Return records with id from 1 to limit"
    ),
):
    try:
        df = load_data_for_niche(niche)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if df.empty:
        raise HTTPException(status_code=500, detail="Dataset not loaded or empty")

    if limit is not None:
        filtered_df = df[(df["id"] >= 1) & (df["id"] <= limit)]
    else:
        filtered_df = df

    if filtered_df.empty:
        raise HTTPException(status_code=404, detail="No records found in the given range")

    return filtered_df.to_dict(orient="records")
