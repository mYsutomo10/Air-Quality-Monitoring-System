import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
from datetime import datetime

class ParsePubSubMessage(beam.DoFn):
    def process(self, message):
        data = json.loads(message.decode('utf-8'))
        attributes = data.get("message", {}).get("attributes", {})
        
        sensor_data = {
            "device_id": attributes.get("device_id", ""),
            "timestamp": attributes.get("timestamp", ""),
            "latitude": float(attributes.get("latitude", 0)),
            "longitude": float(attributes.get("longitude", 0)),
            "pm2_5": float(attributes.get("pm2_5", 0)),
            "pm10": float(attributes.get("pm10", 0)),
            "o3": float(attributes.get("o3", 0)),
            "co": float(attributes.get("co", 0)),
            "no2": float(attributes.get("no2", 0))
        }
        yield sensor_data

class CleanSensorData(beam.DoFn):
    def process(self, element):
        # Format timestamp
        try:
            element["timestamp"] = datetime.strptime(element["timestamp"], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return  # Drop invalid data

        # Handle missing & outliers (contoh: pm2.5 harus dalam range 0-500)
        if not (0 <= element["pm2_5"] <= 500):
            element["pm2_5"] = 0
        if not (0 <= element["pm10"] <= 600):
            element["pm10"] = 0

        yield element

from google.cloud import firestore

class JoinWeatherData(beam.DoFn):
    def setup(self):
        self.db = firestore.Client()

    def process(self, element):
        lat, lon = element["latitude"], element["longitude"]
        query = self.db.collection(f"weather_pollution_{lat}_{lon}").order_by("dt", direction=firestore.Query.DESCENDING).limit(1)
        docs = query.stream()

        for doc in docs:
            weather_data = doc.to_dict()
            element.update({
                "temperature": weather_data["list"][0]["main"]["temp"],
                "humidity": weather_data["list"][0]["main"]["humidity"],
                "wind_speed": weather_data["list"][0]["wind"]["speed"]
            })
        yield element

class SaveToFirestore(beam.DoFn):
    def setup(self):
        self.db = firestore.Client()

    def process(self, element):
        doc_ref = self.db.collection("processed_data").document()
        doc_ref.set(element)
        yield element

class SaveToBigQuery(beam.DoFn):
    def process(self, element):
        from apache_beam.io.gcp.bigquery import WriteToBigQuery
        yield beam.Row(
            device_id=element["device_id"],
            timestamp=element["timestamp"].isoformat(),
            latitude=element["latitude"],
            longitude=element["longitude"],
            pm2_5=element["pm2_5"],
            pm10=element["pm10"],
            o3=element["o3"],
            co=element["co"],
            no2=element["no2"],
            temperature=element["temperature"],
            humidity=element["humidity"],
            wind_speed=element["wind_speed"]
        )

def run():
    pipeline_options = PipelineOptions(
        streaming=True,
        project="your-gcp-project",
        region="us-central1",
        job_name="aqms-dataflow-pipeline",
        temp_location="gs://your-bucket-name/temp",
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read PubSub Messages" >> beam.io.ReadFromPubSub(subscription="projects/your-gcp-project/subscriptions/sensor-sub")
            | "Parse Messages" >> beam.ParDo(ParsePubSubMessage())
            | "Clean Data" >> beam.ParDo(CleanSensorData())
            | "Join Weather Data" >> beam.ParDo(JoinWeatherData())
            | "Save to Firestore" >> beam.ParDo(SaveToFirestore())
            | "Save to BigQuery" >> beam.ParDo(SaveToBigQuery())
        )

if __name__ == "__main__":
    run()