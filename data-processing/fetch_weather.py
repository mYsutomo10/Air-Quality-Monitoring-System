import requests
import json
import firebase_admin
from firebase_admin import credentials, firestore

# Inisialisasi Firestore dengan service account JSON
cred = credentials.Certificate("path/to/serviceAccountKey.json")  # Ganti dengan path yang benar
firebase_admin.initialize_app(cred)
db = firestore.client()

# API Key OpenWeather
API_KEY = "YOUR_OPENWEATHER_API_KEY"

# Daftar koordinat lokasi (latitude, longitude)
locations = [
    (-6.983441, 107.633559),  # Bojongsoang
    (-7.008112, 107.635957),  # Baleendah
]

# Loop untuk mengambil data dari setiap koordinat
for lat, lon in locations:
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
    response = requests.get(url)

    if response.status_code == 200:
        json_response = response.json()

        # Ekstraksi data
        temperature_k = json_response["main"]["temp"]
        temperature_c = round(temperature_k - 273.15, 2)  # Konversi ke Celsius
        humidity = json_response["main"]["humidity"]
        wind_speed = json_response["wind"]["speed"]
        wind_direction = json_response["wind"]["deg"]
        timestamp = json_response["dt"]  # Timestamp dalam UNIX UTC

        # Format JSON yang akan dikirim ke Firestore
        weather_data = {
            "latitude": lat,
            "longitude": lon,
            "timestamp": timestamp,
            "temp": temperature_c,
            "humidity": humidity,
            "wind.speed": wind_speed,
            "wind.deg": wind_direction
        }

        # Simpan ke Firestore dalam koleksi "weather_data"
        db.collection("weather_data").add(weather_data)
        print(f"✅ Data berhasil disimpan untuk lokasi ({lat}, {lon})")
    else:
        print(f"❌ Gagal mengambil data untuk lokasi ({lat}, {lon}). Status code: {response.status_code}")

print("\n🚀 Data cuaca telah berhasil dikirim ke Firestore!")