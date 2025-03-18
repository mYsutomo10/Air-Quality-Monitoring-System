from dotenv import load_dotenv
import os
import requests
import time
import firebase_admin
from firebase_admin import credentials, firestore

# Inisialisasi Firestore dengan service account JSON
cred = credentials.Certificate("path/to/serviceAccountKey.json")  # Ganti dengan path yang benar
firebase_admin.initialize_app(cred)
db = firestore.client()

# Load variabel dari file .env
load_dotenv()

# API Key OpenWeather
API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Daftar koordinat lokasi (latitude, longitude)
locations = [
    (-6.983441, 107.633559),  # Bojongsoang
    (-7.008112, 107.635957),  # Baleendah
]

# Loop tak terbatas untuk menjalankan API call setiap 90 detik
while True:
    for lat, lon in locations:
        # API Cuaca
        weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
        weather_response = requests.get(weather_url)

        # API Polusi Udara
        pollution_url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
        pollution_response = requests.get(pollution_url)

        if weather_response.status_code == 200 and pollution_response.status_code == 200:
            weather_data = weather_response.json()
            pollution_data = pollution_response.json()

            # Ekstrak data cuaca
            temperature_k = weather_data["main"]["temp"]
            temperature_c = round(temperature_k - 273.15, 2)  # Konversi ke Celsius
            humidity = weather_data["main"]["humidity"]
            wind_speed = weather_data["wind"]["speed"]
            wind_direction = weather_data["wind"]["deg"]

            # Ekstrak data polusi udara
            pollution_info = pollution_data["list"][0]
            co = pollution_info["components"]["co"]
            no2 = pollution_info["components"]["no2"]
            o3 = pollution_info["components"]["o3"]
            pm2_5 = pollution_info["components"]["pm2_5"]
            pm10 = pollution_info["components"]["pm10"]
            timestamp = pollution_info["dt"]  # Timestamp dalam UNIX UTC

            # Konversi UNIX timestamp ke format YYYY-MM-DD HH:MM:SS
            timestamp_str = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

            # Format JSON yang akan dikirim ke Firestore
            combined_data = {
                "coord": {
                    "lon": lon,
                    "lat": lat
                },
                "list": [
                    {
                        "main": {
                            "temp": temperature_c,
                            "humidity": humidity
                        },
                        "wind": {
                            "speed": wind_speed,
                            "deg": wind_direction
                        },
                        "polution": {
                            "co": co,
                            "no2": no2,
                            "o3": o3,
                            "pm2_5": pm2_5,
                            "pm10": pm10
                        },
                        "dt": timestamp_str
                    }
                ]
            }

            # Simpan ke Firestore dalam koleksi "weather_pollution_data"
            db.collection("weather_pollution_data").add(combined_data)
            print(f"✅ Data berhasil disimpan untuk lokasi ({lat}, {lon})")
        else:
            print(f"❌ Gagal mengambil data untuk lokasi ({lat}, {lon}). Status code: {weather_response.status_code}, {pollution_response.status_code}")

    print("\n⏳ Menunggu 90 detik sebelum mengambil data lagi...\n")
    time.sleep(90)  # Menunggu 90 detik sebelum melakukan pemanggilan API lagi