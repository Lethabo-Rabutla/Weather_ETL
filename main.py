# weather_etl_pipeline.py
import os
import requests
import pandas as pd
from datetime import datetime, timezone
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("API_KEY")
CITIES = ["Johannesburg", "Pretoria", "Cape Town"]

DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_NAME = os.getenv("DB_NAME")


def kelvin_to_celsius(kelvin: float) -> float:
    """Convert temperature from Kelvin to Celsius."""
    return kelvin - 273.15


def get_db_engine():
    """Create and return a SQLAlchemy database engine."""
    return create_engine(
        f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

# Extract, Transform, Load (ETL) functions
def fetch_weather_data(city: str) -> pd.DataFrame:
    """Fetch 10-day weather forecast data for a given city."""
    url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&cnt=240&appid={API_KEY}"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed to retrieve data for {city}. Status code: {response.status_code}")

    data = response.json()
    records = []
    # Process the data into a list of dictionaries
    # Each dictionary represents a weather record for a specific hour
    for hour in data['list']:
        records.append({
            'city': data['city']['name'],
            'country': data['city']['country'],
            'date_time': datetime.fromtimestamp(hour['dt'], tz=timezone.utc),
            'temperature': round(kelvin_to_celsius(hour['main']['temp']), 2),
            'feels_like': round(kelvin_to_celsius(hour['main']['feels_like']), 2),
            'humidity': hour['main']['humidity'],
            'wind_speed': round(hour['wind']['speed'], 2),
            'pressure': hour['main']['pressure'],
            'weather_description': hour['weather'][0]['description'],
            'weather_main': hour['weather'][0]['main'],
            'wind_direction': hour['wind']['deg'],
            'cloudiness': hour['clouds']['all'],
            'rain_volume': hour.get('rain', {}).get('3h', 0),
            'snow_volume': hour.get('snow', {}).get('3h', 0)
        })
    # Create a DataFrame from the records
    df = pd.DataFrame(records)
    df['Day'] = df['date_time'].dt.date
    df['Time'] = df['date_time'].dt.time
    return df

# Plotting functions
def plot_temperature_trends(df: pd.DataFrame, city: str):
    """Generate line plot and heatmap for temperature trends."""
    avg_temp = df['temperature'].mean()
    plots_dir = "plots"

    # Line plot
    plt.figure(figsize=(12, 3))
    plt.plot(df['date_time'], df['temperature'], color='blue', marker='o', markersize=3, linewidth=2)
    plt.axhline(avg_temp, color='red', linestyle='--', label=f'Average = {avg_temp:.2f}°C')
    plt.title(f'Temperature Trend in {city} over 10 Days')
    plt.xlabel('Date & Time')
    plt.ylabel('Temperature (°C)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, f"{city}_temperature_trend.png"))
    plt.close()

    # Heatmap
    df_pivot = df.pivot(index='Day', columns='Time', values='temperature')
    plt.figure(figsize=(12, 5))
    sns.heatmap(df_pivot, cmap='coolwarm', annot=True, fmt=".1f", linewidths=.5)
    plt.title(f'Temperature Heatmap for {city}')
    plt.xlabel('Hour of Day')
    plt.ylabel('Day')
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, f"{city}_temperature_heatmap.png"))
    plt.close()

# Load data into PostgreSQL database
def load_to_db(df: pd.DataFrame, engine, replace_table=False):
    """
    Insert DataFrame into PostgreSQL database.
    If replace_table=True, drop and create the table automatically.
    """
    if replace_table:
        df.to_sql('weather_data', engine, if_exists='replace', index=False)
    else:
        df.to_sql('weather_data', engine, if_exists='append', index=False)


def main():
    """Main ETL process."""
    engine = get_db_engine()
    first_load = True
    try:
        for city in CITIES:
            print(f"Fetching weather data for {city}...")
            df = fetch_weather_data(city)
            print(f"Generating plots for {city}...")
            plot_temperature_trends(df, city)
            print(f"Inserting data for {city} into database...")
            load_to_db(df, engine, replace_table=first_load)
            first_load = False
            print(f"Data for {city} processed successfully.\n")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
