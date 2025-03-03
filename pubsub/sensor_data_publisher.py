#!/usr/bin/env python3
import argparse
import datetime
import json
import time
import uuid
import requests
from google.cloud import pubsub_v1

class SensorDataPublisher:
    def __init__(self, project_id, topic_name):
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_name)
        self.location_ids = {}  # Dictionary to store location IDs

    def add_location(self, location_name, latitude, longitude):
        """Add a new location with a unique ID."""
        location_id = str(uuid.uuid4())
        self.location_ids[location_name] = {
            'id': location_id,
            'latitude': latitude,
            'longitude': longitude
        }
        return location_id

    def publish_sensor_data(self, location_name, pm25, pm10, temperature, humidity):
        """Publish sensor data to Pub/Sub topic."""
        if location_name not in self.location_ids:
            raise ValueError(f"Location '{location_name}' not registered. Add it first.")
        
        location_info = self.location_ids[location_name]
        
        # Create a message with sensor data
        message = {
            'location_id': location_info['id'],
            'location_name': location_name,
            'latitude': location_info['latitude'],
            'longitude': location_info['longitude'],
            'timestamp': datetime.datetime.now().isoformat(),
            'pm25': pm25,
            'pm10': pm10,
            'temperature': temperature,
            'humidity': humidity
        }
        
        # Convert message to JSON string and encode to bytes
        message_data = json.dumps(message).encode('utf-8')
        
        # Publish the message
        future = self.publisher.publish(self.topic_path, message_data)
        message_id = future.result()
        
        print(f"Published message for {location_name} with ID: {message_id}")
        return message_id

    def simulate_sensor_data(self, location_name, interval_seconds=60, num_samples=10):
        """Simulate sensor data for testing purposes."""
        import random
        
        for i in range(num_samples):
            # Generate random sensor values
            pm25 = round(random.uniform(5, 50), 2)
            pm10 = round(random.uniform(10, 100), 2)
            temperature = round(random.uniform(20, 35), 2)
            humidity = round(random.uniform(40, 90), 2)
            
            self.publish_sensor_data(location_name, pm25, pm10, temperature, humidity)
            
            if i < num_samples - 1:  # Don't sleep after the last sample
                time.sleep(interval_seconds)

def main():
    parser = argparse.ArgumentParser(description='Publish sensor data to Pub/Sub')
    parser.add_argument('--project_id', required=True, help='Google Cloud project ID')
    parser.add_argument('--topic_name', required=True, help='Pub/Sub topic name')
    parser.add_argument('--mode', choices=['simulate', 'manual'], default='simulate',
                        help='Mode: simulate random data or manual input')
    args = parser.parse_args()
    
    publisher = SensorDataPublisher(args.project_id, args.topic_name)
    
    # Add some example locations
    publisher.add_location('Jakarta', -6.2088, 106.8456)
    publisher.add_location('Bandung', -6.9175, 107.6191)
    publisher.add_location('Surabaya', -7.2575, 112.7521)
    
    if args.mode == 'simulate':
        print("Simulating sensor data for Jakarta...")
        publisher.simulate_sensor_data('Jakarta')
    else:
        # Manual mode for testing
        location = input("Enter location name (Jakarta, Bandung, Surabaya): ")
        pm25 = float(input("Enter PM2.5 value: "))
        pm10 = float(input("Enter PM10 value: "))
        temperature = float(input("Enter temperature (°C): "))
        humidity = float(input("Enter humidity (%): "))
        
        publisher.publish_sensor_data(location, pm25, pm10, temperature, humidity)

if __name__ == "__main__":
    main()