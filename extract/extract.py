import requests
import pandas as pd
from sqlalchemy import create_engine

def fetch_unesco_sites():
    url = "https://whc.unesco.org/en/list/json/"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()["features"]
    rows = []
    for f in data:
        props = f["properties"]
        rows.append({
            "id_number": props.get("id_number"),
            "name_en": props.get("name_en"),
            "category": props.get("category"),
            "region_en": props.get("region_en"),
            "date_inscribed": props.get("date_inscribed"),
            "short_description_en": props.get("short_description_en"),
            "longitude": props.get("longitude"),
            "latitude": props.get("latitude"),
        })
    return pd.DataFrame(rows)

def load_to_postgres(df):
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/unesco")
    df.to_sql("unesco_raw", engine, if_exists="replace", index=False)
    print("Loaded to Postgres table: unesco_raw")

if __name__ == "__main__":
    df = fetch_unesco_sites()
    load_to_postgres(df)
