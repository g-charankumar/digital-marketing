from fastapi import FastAPI, HTTPException
import pandas as pd
import os

app = FastAPI(title="Dataset API")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(BASE_DIR, 'restro.csv')

try:
    df = pd.read_csv(csv_path)
    df.columns = [col.lower() for col in df.columns]  # lowercase columns

    # Add 'id' column as row number starting from 1
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'id'}, inplace=True)
    df['id'] = df['id'] + 1
except Exception as e:
    print(f"Error loading CSV: {e}")
    df = pd.DataFrame()

@app.get("/")
def root():
    return {"message": "Welcome to Dataset API. Use /records to fetch data."}

@app.get("/records")
def list_records(limit: int = 100):
    if df.empty:
        raise HTTPException(status_code=500, detail="Dataset not loaded")
    return df.head(limit).to_dict(orient="records")

@app.get("/records/{record_id}")
def get_record(record_id: int):
    if df.empty:
        raise HTTPException(status_code=500, detail="Dataset not loaded")
    rec = df[df["id"] == record_id]
    if rec.empty:
        raise HTTPException(status_code=404, detail="Record not found")
    return rec.iloc[0].to_dict()
