import firebase_admin
from firebase_admin import credentials, firestore
import math

# Inisialisasi Firestore
cred = credentials.Certificate("path/to/serviceAccountKey.json")  # Ganti dengan path JSON yang benar
firebase_admin.initialize_app(cred)
db = firestore.client()

# Koleksi yang akan dipantau (bisa diperluas untuk banyak lokasi)
locations = ["bojongsoang", "baleendah"]
collections = [f"weather_pollution_{loc}" for loc in locations]

# Fungsi untuk menghitung AQI berdasarkan polutan
def calculate_aqi(pollutants):
    # AQI Breakpoints (EPA Standard)
    aqi_breakpoints = {
        "pm2_5": [(0, 12, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150), (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300), (250.5, 350.4, 301, 400), (350.5, 500.4, 401, 500)],
        "pm10": [(0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150), (255, 354, 151, 200), (355, 424, 201, 300), (425, 504, 301, 400), (505, 604, 401, 500)],
        "o3": [(0, 54, 0, 50), (55, 70, 51, 100), (71, 85, 101, 150), (86, 105, 151, 200), (106, 200, 201, 300)],
        "no2": [(0, 53, 0, 50), (54, 100, 51, 100), (101, 360, 101, 150), (361, 649, 151, 200), (650, 1249, 201, 300), (1250, 1649, 301, 400), (1650, 2049, 401, 500)],
        "co": [(0, 4.4, 0, 50), (4.5, 9.4, 51, 100), (9.5, 12.4, 101, 150), (12.5, 15.4, 151, 200), (15.5, 30.4, 201, 300), (30.5, 40.4, 301, 400), (40.5, 50.4, 401, 500)]
    }

    def get_aqi(value, breakpoints):
        for bp in breakpoints:
            if bp[0] <= value <= bp[1]:
                return ((bp[3] - bp[2]) / (bp[1] - bp[0])) * (value - bp[0]) + bp[2]
        return None

    aqi_values = []
    for key in aqi_breakpoints.keys():
        if key in pollutants:
            aqi_value = get_aqi(pollutants[key], aqi_breakpoints[key])
            if aqi_value:
                aqi_values.append(aqi_value)

    return max(aqi_values) if aqi_values else None

# Fungsi listener Firestore untuk mendeteksi data baru
def firestore_listener(doc_snapshot, changes, read_time):
    for doc in doc_snapshot:
        data = doc.to_dict()
        
        if "list" in data and len(data["list"]) > 0:
            pollution_data = data["list"][0]["polution"]

            # Ambil data polutan
            pollutants = {
                "pm2_5": pollution_data.get("pm2_5", 0),
                "pm10": pollution_data.get("pm10", 0),
                "o3": pollution_data.get("o3", 0),
                "no2": pollution_data.get("no2", 0),
                "co": pollution_data.get("co", 0),
            }

            # Hitung AQI
            aqi = calculate_aqi(pollutants)

            if aqi is not None:
                # Update dokumen dengan AQI baru
                doc.reference.update({"aqi": round(aqi)})
                print(f"✅ AQI {round(aqi)} berhasil diperbarui untuk dokumen {doc.id}")

# Menjalankan listener Firestore untuk setiap koleksi lokasi
listeners = []
for collection in collections:
    query = db.collection(collection).on_snapshot(firestore_listener)
    listeners.append(query)

print("🚀 Firestore Trigger aktif, menunggu data baru masuk...")