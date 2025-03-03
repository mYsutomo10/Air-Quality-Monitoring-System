#!/usr/bin/env python3
import argparse
import json
import os
import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions 
from apache_beam.io import ReadFromPubSub, WriteToBigQuery, WriteToText 
from apache_beam.transforms.window import FixedWindows 
import datetime
import tensorflow as tf 
from google.cloud import storage, firestore

class ParseJsonDoFn(beam.DoFn):
    """Parse JSON messages from PubSub."""
    def process(self, element):
        try:
            row = json.loads(element.decode('utf-8'))
            return [row]
        except Exception as e:
            print(f"Error parsing JSON: {e}")
            return []

class CalibrateReadingsDoFn(beam.DoFn):
    """Apply calibration to sensor readings."""
    def process(self, element):
        # Apply calibration formula (example)
        # In a real scenario, this would be based on calibration factors
        element['pm25_calibrated'] = element['pm25'] * 1.1
        element['pm10_calibrated'] = element['pm10'] * 0.9
        return [element]

class PredictAQIDoFn(beam.DoFn):
    """Predict AQI using the TensorFlow model."""
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = None
    
    def setup(self):
        # Load the model at the start of the worker
        local_model_path = "/tmp/aqi_model"
        if self.model_path.startswith("gs://"):
            # Download model from GCS
            bucket_name = self.model_path.split("/")[2]
            prefix = "/".join(self.model_path.split("/")[3:])
            
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            
            # Create local directory if it doesn't exist
            os.makedirs(local_model_path, exist_ok=True)
            
            for blob in blobs:
                local_file_path = os.path.join(local_model_path, os.path.basename(blob.name))
                blob.download_to_filename(local_file_path)
            
            self.model = tf.keras.models.load_model(local_model_path)
        else:
            self.model = tf.keras.models.load_model(self.model_path)
    
    def process(self, element):
        try:
            # Create feature vector for model input
            features = [
                element['pm25_calibrated'],
                element['pm10_calibrated'],
                element['temperature'],
                element['humidity']
            ]
            
            # Convert to numpy array and reshape for model
            import numpy as np # type: ignore
            input_data = np.array([features])
            
            # Make prediction
            prediction = self.model.predict(input_data)
            
            # Extract AQI value from prediction
            aqi = float(prediction[0][0])
            
            # Add prediction to element
            element['aqi_prediction'] = aqi
            
            # Add AQI category based on prediction
            if aqi <= 50:
                element['aqi_category'] = 'Good'
            elif aqi <= 100:
                element['aqi_category'] = 'Moderate'
            elif aqi <= 150:
                element['aqi_category'] = 'Unhealthy for Sensitive Groups'
            elif aqi <= 200:
                element['aqi_category'] = 'Unhealthy'
            elif aqi <= 300:
                element['aqi_category'] = 'Very Unhealthy'
            else:
                element['aqi_category'] = 'Hazardous'
            
            # Add timestamp for when the prediction was made
            element['prediction_timestamp'] = datetime.datetime.now().isoformat()
            
            return [element]
        
        except Exception as e:
            print(f"Error in prediction: {e}")
            return []

class WriteToFirestoreDoFn(beam.DoFn):
    """Write latest predictions to Firestore."""
    def __init__(self, project_id):
        self.project_id = project_id
        self.db = None
    
    def setup(self):
        self.db = firestore.Client(project=self.project_id)
    
    def process(self, element):
        try:
            # Create a document reference with location_id as the document ID
            doc_ref = self.db.collection('current_aqi').document(element['location_id'])
            
            # Update or create the document
            doc_ref.set({
                'location_name': element['location_name'],
                'latitude': element['latitude'],
                'longitude': element['longitude'],
                'aqi': element['aqi_prediction'],
                'aqi_category': element['aqi_category'],
                'pm25': element['pm25_calibrated'],
                'pm10': element['pm10_calibrated'],
                'temperature': element['temperature'],
                'humidity': element['humidity'],
                'timestamp': element['timestamp'],
                'updated_at': firestore.SERVER_TIMESTAMP
            })
            
            return [element]
        
        except Exception as e:
            print(f"Error writing to Firestore: {e}")
            return [element]  # Continue pipeline even if Firestore write fails

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', required=True, help='Google Cloud project ID')
    parser.add_argument('--subscription', required=True, help='Pub/Sub subscription')
    parser.add_argument('--model_path', required=True, help='Path to TensorFlow model in GCS')
    parser.add_argument('--dataset', required=True, help='BigQuery dataset')
    parser.add_argument('--table', required=True, help='BigQuery table')
    parser.add_argument('--temp_location', required=True, help='GCS location for temp files')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Set up pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    
    # Define BigQuery schema
    bq_schema = {
        'fields': [
            {'name': 'location_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'location_name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'latitude', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'longitude', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'pm25', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'pm10', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'temperature', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'humidity', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'pm25_calibrated', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'pm10_calibrated', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'aqi_prediction', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'aqi_category', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'prediction_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
        ]
    }
    
    # Create and run the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # Read from PubSub
        sensor_data = (
            p | 'Read from PubSub' >> ReadFromPubSub(subscription=f'projects/{known_args.project_id}/subscriptions/{known_args.subscription}')
              | 'Parse JSON' >> beam.ParDo(ParseJsonDoFn())
        )
        
        # Process sensor data
        processed_data = (
            sensor_data
            | 'Calibrate Readings' >> beam.ParDo(CalibrateReadingsDoFn())
            | 'Predict AQI' >> beam.ParDo(PredictAQIDoFn(known_args.model_path))
        )
        
        # Write to BigQuery for historical data
        processed_data | 'Write to BigQuery' >> WriteToBigQuery(
            table=f'{known_args.project_id}:{known_args.dataset}.{known_args.table}',
            schema=bq_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
        
        # Write to Firestore for current data
        processed_data | 'Write to Firestore' >> beam.ParDo(WriteToFirestoreDoFn(known_args.project_id))

if __name__ == '__main__':
    run()