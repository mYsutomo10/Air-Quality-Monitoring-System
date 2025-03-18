import firebase_admin
from firebase_admin import credentials, firestore

# Inisialisasi Firestore
cred = credentials.Certificate("service-account.json")  # Ganti dengan path yang benar
firebase_admin.initialize_app(cred)
db = firestore.client()

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

def calculate_aqi(data):
    pollutants = data["list"][0]["polution"]
    aqi_values = [get_aqi(pollutants[key], aqi_breakpoints[key]) for key in pollutants if key in aqi_breakpoints]
    return max(aqi_values) if aqi_values else None

def firestore_listener(event, context):
    doc = event["value"]["fields"]
    doc_id = context.resource.split("/")[-1]
    
    pollutants = {
        "pm2_5": float(doc["list"]["arrayValue"]["values"][0]["mapValue"]["fields"]["polution"]["mapValue"]["fields"]["pm2_5"]["doubleValue"]),
        "pm10": float(doc["list"]["arrayValue"]["values"][0]["mapValue"]["fields"]["polution"]["mapValue"]["fields"]["pm10"]["doubleValue"])
    }

    aqi = calculate_aqi({"list": [{"polution": pollutants}]})

    if aqi is not None:
        db.collection(context.resource.split("/documents/")[-1]).document(doc_id).update({"aqi": round(aqi)})
        print(f"✅ AQI diperbarui: {round(aqi)}")