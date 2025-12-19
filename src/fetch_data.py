import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta
import pytz

# Setup client with cache/retry
cache = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def get_last_day_weather(lat, lon, location_name=None):
    """Get weather data with enhanced metadata - SIMPLE VERSION"""
    
    # Calculate yesterday's date (respecting 5-day delay)
    yesterday = datetime.now() - timedelta(days=5)
    date_str = yesterday.strftime("%Y-%m-%d")
    
    # Record when we're ingesting this data
    ingested_at = datetime.now(pytz.UTC)
    
    # API request
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date_str,
        "end_date": date_str,
        "hourly": ["temperature_2m", "wind_speed_10m", "precipitation"],
    }
    
    # Fetch data
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    
    # Extract metadata
    elevation = response.Elevation()
    timezone = response.Timezone()
    
    # Process hourly data
    hourly = response.Hourly()
    dates = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )
    
    # Create DataFrame with all requested metadata
    data = {
        "location": location_name or f"{lat:.4f}, {lon:.4f}",
        "latitude": lat,
        "longitude": lon,
        "observed_time": dates,
        "temperature_c": hourly.Variables(0).ValuesAsNumpy(),
        "wind_speed_kmh": hourly.Variables(1).ValuesAsNumpy(),
        "precipitation_mm": hourly.Variables(2).ValuesAsNumpy(),
        "source": "Open-Meteo Archive API",
        "ingested_at": ingested_at,
    }
    
    df = pd.DataFrame(data)
    return df

# Example usage
if __name__ == "__main__":
    # Simple list of locations
    locations = [
        ("Cape_Town", -33.9258, 18.4232)
        #("Munich", 48.1351, 11.5820),
        #("Stellenbosch", -33.9322, 18.8602)
    ]
    
    for name, lat, lon in locations:
        print(f"\nFetching data for {name}...")
        df = get_last_day_weather(lat, lon, name)
        print(f"  Got {len(df)} records")
        print(f"  Saved to: weather_{name.lower().replace(' ', '_')}.csv")
        df.to_csv(f"weather_{name.lower().replace(' ', '_')}.csv", index=False)