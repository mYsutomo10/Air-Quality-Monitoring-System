import firebase_admin
from firebase_admin import credentials, firestore

# Inisialisasi Firestore
cred = credentials.Certificate("service-account.json")  # Ganti dengan path yang benar
firebase_admin.initialize_app(cred)
db = firestore.client()

# AQI Breakpoints (EPA Standard)
aqi_breakpoints = {
    "pm2_5": [(0, 12, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150), (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300)],
    "pm10": [(0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150), (255, 354, 151, 200), (355, 424, 201, 300)],
    "o3": [(0, 54, 0, 50), (55, 70, 51, 100), (71, 85, 101, 150), (86, 105, 151, 200)],
    "no2": [(0, 53, 0, 50), (54, 100, 51, 100), (101, 360, 101, 150), (361, 649, 151, 200)],
    "co": [(0, 4.4, 0, 50), (4.5, 9.4, 51, 100), (9.5, 12.4, 101, 150), (12.5, 15.4, 151, 200)]
}

def get_aqi(value, breakpoints):
    for bp in breakpoints:
        if bp[0] <= value <= bp[1]:
            return ((bp[3] - bp[2]) / (bp[1] - bp[0])) * (value - bp[0]) + bp[2]
    return None

def calculate_aqi(pollutants):
    aqi_values = [get_aqi(pollutants[key], aqi_breakpoints[key]) for key in pollutants if key in aqi_breakpoints]
    return max(aqi_values) if aqi_values else None

def firestore_listener(event, context):
    path_parts = context.resource.split("/")
    location = path_parts[-3]  # Nama lokasi (misalnya "bojongsoang")
    reading_id = path_parts[-1]  # ID dokumen dalam subcollection `readings`

    doc_ref = db.collection("weather_pollution").document(location).collection("readings").document(reading_id)
    doc = doc_ref.get()

    if doc.exists:
        data = doc.to_dict()
        pollutants = data["polution"]

        aqi = calculate_aqi(pollutants)
        if aqi is not None:
            doc_ref.update({"aqi": round(aqi)})
            print(f"✅ AQI {round(aqi)} diperbarui untuk {location}, dokumen {reading_id}")