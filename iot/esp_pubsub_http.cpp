#include <WiFi.h>
#include <HTTPClient.h>
#include <ArduinoJson.h>
#include <TinyGPS++.h>
#include <SoftwareSerial.h>

// Konfigurasi WiFi
const char* ssid = "NAMA_WIFI";
const char* password = "PASSWORD_WIFI";

// Konfigurasi Google Cloud
const char* project_id = "your-project-id";   // Ganti dengan ID proyek
const char* topic_name = "sensor-data";       // Ganti dengan nama topik Pub/Sub
const char* jwt_token = "YOUR_GENERATED_JWT"; // JWT Token dari Google Cloud

// Modul GPS
static const int RXPin = 16, TXPin = 17;
static const uint32_t GPSBaud = 9600;
TinyGPSPlus gps;
SoftwareSerial gpsSerial(RXPin, TXPin);

// Pin Sensor Udara
#define PM25_PIN 4
#define PM10_PIN 5
#define O3_PIN 34
#define CO_PIN 35
#define NO2_PIN 32

void setup() {
    Serial.begin(115200);
    WiFi.begin(ssid, password);
    gpsSerial.begin(GPSBaud);
    
    while (WiFi.status() != WL_CONNECTED) {
        delay(1000);
        Serial.println("Menghubungkan ke WiFi...");
    }
    Serial.println("Terhubung ke WiFi");
}

void loop() {
    if (WiFi.status() == WL_CONNECTED) {
        HTTPClient http;
        
        String url = "https://pubsub.googleapis.com/v1/projects/";
        url += project_id;
        url += "/topics/";
        url += topic_name;
        url += ":publish";

        http.begin(url.c_str());
        http.addHeader("Content-Type", "application/json");
        http.addHeader("Authorization", "Bearer " + String(jwt_token));
        
        // Baca data sensor
        float pm25 = analogRead(PM25_PIN);
        float pm10 = analogRead(PM10_PIN);
        float o3 = analogRead(O3_PIN);
        float co = analogRead(CO_PIN);
        float no2 = analogRead(NO2_PIN);
        
        // Baca data GPS
        float latitude = 0.0, longitude = 0.0;
        String date = "", time = "";
        
        while (gpsSerial.available() > 0) {
            gps.encode(gpsSerial.read());
        }
        
        if (gps.location.isValid()) {
            latitude = gps.location.lat();
            longitude = gps.location.lng();
        }
        
        if (gps.date.isValid() && gps.time.isValid()) {
            date = String(gps.date.year()) + "-" + String(gps.date.month()) + "-" + String(gps.date.day());
            time = String(gps.time.hour()) + ":" + String(gps.time.minute()) + ":" + String(gps.time.second());
        }

        // Buat JSON payload
        StaticJsonDocument<256> doc;
        doc["messages"][0]["data"] = ""; // Data dikosongkan dulu
        JsonObject sensorData = doc.createNestedObject("messages")[0]["attributes"];
        sensorData["pm25"] = pm25;
        sensorData["pm10"] = pm10;
        sensorData["o3"] = o3;
        sensorData["co"] = co;
        sensorData["no2"] = no2;
        sensorData["latitude"] = latitude;
        sensorData["longitude"] = longitude;
        sensorData["date"] = date;
        sensorData["time"] = time;
        
        String payload;
        serializeJson(doc, payload);
        
        int httpResponseCode = http.POST(payload);
        Serial.print("Response Code: ");
        Serial.println(httpResponseCode);
        
        if (httpResponseCode > 0) {
            String response = http.getString();
            Serial.println(response);
        }
        
        http.end();
    } else {
        Serial.println("Koneksi WiFi terputus!");
    }
    
    delay(5000); // Kirim data setiap 5 detik
}