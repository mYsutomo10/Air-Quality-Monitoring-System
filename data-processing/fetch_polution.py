import requests
import time
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
    (45.133, 7.367),  # Turin
    (-6.2088, 106.8456),  # Jakarta
    (35.6895, 139.6917),  # Tokyo
    (40.7128, -74.0060)  # New York
]

# Loop tak terbatas untuk menjalankan API call setiap 90 detik
while True:
    for lat, lon in locations:
        url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
        response = requests.get(url)

        if response.status_code == 200:
            json_response = response.json()

            # Ekstraksi data polusi udara
            pollution_data = json_response["list"][0]  # Data utama
            pm2_5 = pollution_data["components"]["pm2_5"]
            pm10 = pollution_data["components"]["pm10"]
            o3 = pollution_data["components"]["o3"]
            co = pollution_data["components"]["co"]
            no2 = pollution_data["components"]["no2"]
            timestamp = pollution_data["dt"]  # Timestamp dalam UNIX UTC

            # Format JSON yang akan dikirim ke Firestore
            air_quality_data = {
                "latitude": lat,
                "longitude": lon,
                "timestamp": timestamp,
                "pm2_5": pm2_5,
                "pm10": pm10,
                "o3": o3,
                "co": co,
                "no2": no2
            }

            # Simpan ke Firestore dalam koleksi "air_quality_data"
            db.collection("air_quality_data").add(air_quality_data)
            print(f"✅ Data polusi berhasil disimpan untuk lokasi ({lat}, {lon})")
        else:
            print(f"❌ Gagal mengambil data polusi untuk lokasi ({lat}, {lon}). Status code: {response.status_code}")

    print("\n⏳ Menunggu 90 detik sebelum mengambil data lagi...\n")
    time.sleep(90)  # Menunggu 90 detik sebelum melakukan pemanggilan API lagi