import os
import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
from google.cloud import pubsub_v1

# Inisialisasi Firestore
cred = credentials.Certificate("service-account.json")  # Ganti dengan path yang benar
firebase_admin.initialize_app(cred)
db = firestore.client()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
locations = {
    "bojongsoang": (-6.983441, 107.633559),
    "baleendah": (-7.008112, 107.635957),
}

def fetch_weather_pollution(request):
    """Fungsi ini akan dipanggil oleh Cloud Scheduler melalui Pub/Sub"""
    for location_name, (lat, lon) in locations.items():
        weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
        pollution_url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"

        weather_response = requests.get(weather_url)
        pollution_response = requests.get(pollution_url)

        if weather_response.status_code == 200 and pollution_response.status_code == 200:
            weather_data = weather_response.json()
            pollution_data = pollution_response.json()

            # Ekstraksi data
            temperature_c = round(weather_data["main"]["temp"] - 273.15, 2)
            humidity = weather_data["main"]["humidity"]
            wind_speed = weather_data["wind"]["speed"]
            wind_direction = weather_data["wind"]["deg"]
            pollution_info = pollution_data["list"][0]
            timestamp_str = datetime.utcfromtimestamp(pollution_info["dt"]).strftime('%Y-%m-%d %H:%M:%S')

            combined_data = {
                "coord": {"lon": lon, "lat": lat},
                "list": [{
                    "main": {"temp": temperature_c, "humidity": humidity},
                    "wind": {"speed": wind_speed, "deg": wind_direction},
                    "polution": {
                        "co": pollution_info["components"]["co"],
                        "no2": pollution_info["components"]["no2"],
                        "o3": pollution_info["components"]["o3"],
                        "pm2_5": pollution_info["components"]["pm2_5"],
                        "pm10": pollution_info["components"]["pm10"]
                    },
                    "dt": timestamp_str
                }]
            }

            db.collection(f"weather_pollution_{location_name}").add(combined_data)
            print(f"✅ Data berhasil disimpan untuk {location_name}")

    return "Data berhasil diperbarui", 200