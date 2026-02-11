import requests
import pandas as pd
import sqlite3
from datetime import datetime

# Şehir koordinatları
cities = {
    "Istanbul": (41.01, 28.97),
    "Ankara": (39.93, 32.85),
    "Izmir": (38.42, 27.14),
    "Berlin": (52.52, 13.41),
    "London": (51.50, -0.12)
}

# API Fonksiyonu
def fetch_weather(city, lat, lon):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=auto"
    
    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame({
        "city": city,
        "date": data["daily"]["time"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"]
    })

    return df

# Veri Temizleme Kodu
def clean_data(df):
    df = df.dropna(subset=["city", "date"])
    df = df.dropna(subset=["temp_max", "temp_min"])

    df.loc[df["temp_min"] > df["temp_max"], ["temp_min", "temp_max"]] = \
        df.loc[df["temp_min"] > df["temp_max"], ["temp_max", "temp_min"]].values

    df.loc[df["precipitation"] < 0, "precipitation"] = 0

    df = df.drop_duplicates(subset=["city", "date"])

    return df

# Veri Transform Kodu
def transform_data(df):
    df["avg_temp"] = (df["temp_max"] + df["temp_min"]) / 2
    df["temp_diff"] = df["temp_max"] - df["temp_min"]

    def label(temp):
        if temp >= 30:
            return "hot"
        elif temp >= 15:
            return "warm"
        elif temp >= 5:
            return "cool"
        else:
            return "cold"

    df["weather_label"] = df["temp_max"].apply(label)

    df["has_rain"] = (df["precipitation"] > 0).astype(int)

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day_of_week"] = df["date"].dt.day_name()
    df["is_weekend"] = df["day_of_week"].isin(["Saturday", "Sunday"]).astype(int)

    df["created_at"] = datetime.now()

    return df

# Veritabanına Yazma , bu dosya weather.db olarak oluşacak.

def load_to_db(df):
    conn = sqlite3.connect("weather.db")
    df.to_sql("weather_daily", conn, if_exists="append", index=False)
    conn.close()

# Rapor Fonksiyonu
def report():
    conn = sqlite3.connect("weather.db")

    print(pd.read_sql("""
        SELECT city, date, MAX(temp_max) as max_temp
        FROM weather_daily
    """, conn))

    print(pd.read_sql("""
        SELECT city, date, MIN(temp_min) as min_temp
        FROM weather_daily
    """, conn))

    print(pd.read_sql("""
        SELECT city, AVG(temp_max) as avg_max_temp
        FROM weather_daily
        GROUP BY city
    """, conn))

    conn.close()

# Programı Çalıştıran Kısım
all_data = []

for city, (lat, lon) in cities.items():
    df = fetch_weather(city, lat, lon)
    df = clean_data(df)
    df = transform_data(df)
    all_data.append(df)

final_df = pd.concat(all_data)

load_to_db(final_df)
report()
