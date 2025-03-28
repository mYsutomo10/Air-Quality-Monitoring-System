import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json
import os
from datetime import datetime
from google.cloud import firestore
from google.cloud.firestore import Query

class ParsePubSubMessage(beam.DoFn):
    def process(self, message):
        data = json.loads(message.decode('utf-8'))
        attributes = data.get("message", {}).get("attributes", {})

        sensor_data = {
            "location": attributes.get("location", ""),
            "dt": attributes.get("dt", ""),
            "lat": float(attributes.get("lat", 0)),
            "lon": float(attributes.get("lon", 0)),
            "pm2_5": float(attributes.get("pm2_5", 0)),
            "pm10": float(attributes.get("pm10", 0)),
            "o3": float(attributes.get("o3", 0)),
            "co": float(attributes.get("co", 0)),
            "no2": float(attributes.get("no2", 0))
        }
        yield sensor_data

class CleanSensorData(beam.DoFn):
    def process(self, element):
        try:
            element["dt"] = datetime.strptime(element["dt"], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return  

        if not (0 <= element["pm2_5"] <= 500):
            element["pm2_5"] = 0
        if not (0 <= element["pm10"] <= 600):
            element["pm10"] = 0

        yield element

class JoinWeatherData(beam.DoFn):
    def setup(self):
        self.db = firestore.Client()

    def process(self, element):
        location_name = element["location"]
        query = (
            self.db.collection("weather_pollution")
            .document(location_name)
            .collection("readings")
            .order_by("dt", direction=Query.DESCENDING)
            .limit(1)
        )
        docs = query.stream()

        for doc in docs:
            weather_data = doc.to_dict()
            element.update({
                "temperature": weather_data["main"]["temp"],
                "humidity": weather_data["main"]["humidity"],
                "wind_speed": weather_data["wind"]["speed"],
                "wind_deg": weather_data["wind"]["deg"]
            })
        yield element

class SaveToFirestore(beam.DoFn):
    def setup(self):
        self.db = firestore.Client()

    def process(self, element):
        element["dt"] = element["dt"].isoformat()  # Pastikan timestamp dalam format ISO untuk Firestore
        _, write_time = (
            self.db.collection("processed_data")
            .document(element["location"])
            .collection("sensor_info")
            .add(element)
        )
        yield element

def run():
    project_id = os.getenv("GCP_PROJECT", "your-gcp-project")  # Kiri ambil dari environment variable, kanan nilai default
    pipeline_options = PipelineOptions(
        streaming=True,
        project=project_id,
        region="us-central1",
        job_name="aqms-dataflow-pipeline",
        temp_location=f"gs://{os.getenv('BUCKET_NAME', 'your-bucket-name')}/temp",
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read PubSub Messages" >> beam.io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/sensor-sub")
            | "Parse Messages" >> beam.ParDo(ParsePubSubMessage())
            | "Clean Data" >> beam.ParDo(CleanSensorData())
            | "Join Weather Data" >> beam.ParDo(JoinWeatherData())
            | "Save to Firestore" >> beam.ParDo(SaveToFirestore())
            | "Save to BigQuery" >> WriteToBigQuery(
                table=f"{project_id}:aqms_data.sensor_readings",
                schema="""
                    location:STRING,
                    timestamp:TIMESTAMP,
                    latitude:FLOAT,
                    longitude:FLOAT,
                    pm2_5:FLOAT,
                    pm10:FLOAT,
                    o3:FLOAT,
                    co:FLOAT,
                    no2:FLOAT,
                    temperature:FLOAT,
                    humidity:FLOAT,
                    wind_speed:FLOAT,
                    wind_deg:FLOAT
                """,
                create_disposition=WriteToBigQuery.CreateDisposition.CREATE_IF_NEEDED,
                write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    run()