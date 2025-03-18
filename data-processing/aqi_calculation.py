import firebase_admin
from firebase_admin import credentials, firestore

# Inisialisasi Firestore
cred = credentials.Certificate("path/to/serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# AQI Breakpoints
AQI_BREAKPOINTS = {
    "pm2_5": [(0.0, 12.0, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
              (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300), (250.5, 500.4, 301, 500)],
    "pm10": [(0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150),
             (255, 354, 151, 200), (355, 424, 201, 300), (425, 604, 301, 500)],
    "co": [(0.0, 4.4, 0, 50), (4.5, 9.4, 51, 100), (9.5, 12.4, 101, 150),
           (12.5, 15.4, 151, 200), (15.5, 30.4, 201, 300), (30.5, 50.4, 301, 500)],
    "no2": [(0, 53, 0, 50), (54, 100, 51, 100), (101, 360, 101, 150),
            (361, 649, 151, 200), (650, 1249, 201, 300), (1250, 2049, 301, 500)],
    "o3": [(0.0, 0.054, 0, 50), (0.055, 0.070, 51, 100), (0.071, 0.085, 101, 150),
           (0.086, 0.105, 151, 200), (0.106, 0.200, 201, 300), (0.201, 0.404, 301, 500)]
}

# Fungsi untuk menghitung AQI per polutan
def calculate_aqi(concentration, breakpoints):
    for (C_low, C_high, I_low, I_high) in breakpoints:
        if C_low <= concentration <= C_high:
            return round(((I_high - I_low) / (C_high - C_low)) * (concentration - C_low) + I_low)
    return None

# Fungsi untuk mendapatkan AQI dari beberapa polutan
def get_air_quality_index(data):
    aqi_values = {
        "pm2_5": calculate_aqi(data.get("pm2_5", 0), AQI_BREAKPOINTS["pm2_5"]),
        "pm10": calculate_aqi(data.get("pm10", 0), AQI_BREAKPOINTS["pm10"]),
        "co": calculate_aqi(data.get("co", 0) / 1000, AQI_BREAKPOINTS["co"]),  # Convert ppb to ppm
        "no2": calculate_aqi(data.get("no2", 0), AQI_BREAKPOINTS["no2"]),
        "o3": calculate_aqi(data.get("o3", 0) / 1000, AQI_BREAKPOINTS["o3"])  # Convert ppb to ppm
    }
    overall_aqi = max(filter(lambda x: x is not None, aqi_values.values()))
    return {"AQI_per_pollutant": aqi_values, "Overall_AQI": overall_aqi}

# Fungsi untuk mendengarkan perubahan data pada collection tertentu
def on_snapshot(collection_name, result_collection, col_snapshot, changes, read_time):
    for change in changes:
        if change.type.name == "ADDED":  # Jika ada data baru
            doc_id = change.document.id
            data = change.document.to_dict()

            print(f"📥 Data baru masuk dari {collection_name}: {data}")

            aqi_result = get_air_quality_index(data)

            # Simpan hasil AQI ke Firestore di collection yang sesuai
            db.collection(result_collection).document(doc_id).set(aqi_result)
            print(f"✅ AQI for {doc_id} from {collection_name} calculated and saved to {result_collection}!")

# Memulai listener pada dua collection yang berbeda
sensor_ref_loc1 = db.collection("sensor_data_loc1")
sensor_ref_loc2 = db.collection("sensor_data_loc2")

# Listener untuk masing-masing lokasi
sensor_ref_loc1.on_snapshot(lambda col_snapshot, changes, read_time: on_snapshot("sensor_data_loc1", "aqi_results_loc1", col_snapshot, changes, read_time))
sensor_ref_loc2.on_snapshot(lambda col_snapshot, changes, read_time: on_snapshot("sensor_data_loc2", "aqi_results_loc2", col_snapshot, changes, read_time))

# Agar script tetap berjalan
print("🔄 Listening for new data from both locations...")
import time
while True:
    time.sleep(1)