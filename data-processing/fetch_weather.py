#!/usr/bin/env python3
import argparse
import csv
import datetime
import json
import os
import time
import requests
from google.cloud import storage

class WeatherDataFetcher:
    def __init__(self, api_key, gcs_bucket, gcs_prefix):
        self.api_key = api_key
        self.gcs_bucket = gcs_bucket
        self.gcs_prefix = gcs_prefix
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(gcs_bucket)
    
    def fetch_weather_data(self, locations):
        """Fetch current and historical weather data for multiple locations."""
        results = []
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for location in locations:
            try:
                location_name = location['name']
                lat = location['latitude']
                lon = location['longitude']
                
                # Fetch current weather data
                current_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={self.api_key}&units=metric"
                current_response = requests.get(current_url)
                current_response.raise_for_status()
                current_data = current_response.json()
                
                weather_data = {
                    'location_name': location_name,
                    'latitude': lat,
                    'longitude': lon,
                    'timestamp': datetime.datetime.now().isoformat(),
                    'temperature': current_data['main']['temp'],
                    'humidity': current_data['main']['humidity'],
                    'wind_speed': current_data['wind']['speed'],
                    'wind_direction': current_data.get('wind', {}).get('deg', 0),
                    'precipitation': current_data.get('rain', {}).get('1h', 0),
                    'pressure': current_data['main']['pressure'],
                    'weather_condition': current_data['weather'][0]['main'],
                    'weather_description': current_data['weather'][0]['description'],
                    'type': 'current'
                }
                results.append(weather_data)
                
                # Fetch past 7 days of historical data
                for days_ago in range(1, 8):
                    past_timestamp = int((datetime.datetime.now() - datetime.timedelta(days=days_ago)).timestamp())
                    history_url = f"https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={lat}&lon={lon}&dt={past_timestamp}&appid={self.api_key}&units=metric"
                    history_response = requests.get(history_url)
                    history_response.raise_for_status()
                    history_data = history_response.json()
                    
                    for hourly_data in history_data.get('hourly', []):
                        past_weather = {
                            'location_name': location_name,
                            'latitude': lat,
                            'longitude': lon,
                            'timestamp': datetime.datetime.utcfromtimestamp(hourly_data['dt']).isoformat(),
                            'temperature': hourly_data['temp'],
                            'humidity': hourly_data['humidity'],
                            'wind_speed': hourly_data['wind_speed'],
                            'wind_direction': hourly_data.get('wind_deg', 0),
                            'precipitation': hourly_data.get('rain', {}).get('1h', 0),
                            'pressure': hourly_data['pressure'],
                            'weather_condition': hourly_data['weather'][0]['main'],
                            'weather_description': hourly_data['weather'][0]['description'],
                            'type': 'historical'
                        }
                        results.append(past_weather)
                
                print(f"Fetched weather data for {location_name}")
                
                # Sleep briefly to avoid hitting API rate limits
                time.sleep(1)
                
            except Exception as e:
                print(f"Error fetching weather data for {location_name}: {e}")
        
        # Save results to CSV in GCS
        if results:
            csv_filename = f"weather_data_{timestamp}.csv"
            self._save_to_csv(results, csv_filename)
            self._upload_to_gcs(csv_filename, f"{self.gcs_prefix}/{csv_filename}")
            
            # Clean up local file
            os.remove(csv_filename)
        
        return results
    
    def _save_to_csv(self, data, filename):
        """Save data to a CSV file."""
        if not data:
            return
        
        # Get all possible keys from all dictionaries
        fieldnames = set()
        for item in data:
            fieldnames.update(item.keys())
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames))
            writer.writeheader()
            writer.writerows(data)
    
    def _upload_to_gcs(self, local_path, gcs_path):
        """Upload a file to Google Cloud Storage."""
        blob = self.bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        print(f"Uploaded {local_path} to gs://{self.gcs_bucket}/{gcs_path}")

def main():
    parser = argparse.ArgumentParser(description='Fetch weather data and store in GCS')
    parser.add_argument('--api_key', required=True, help='Weather API key')
    parser.add_argument('--gcs_bucket', required=True, help='GCS bucket name')
    parser.add_argument('--gcs_prefix', default='weather_data', help='GCS prefix/folder')
    parser.add_argument('--locations', required=True, help='JSON file with locations or JSON string')
    args = parser.parse_args()
    
    # Parse locations from file or command line
    if os.path.isfile(args.locations):
        with open(args.locations, 'r') as f:
            locations = json.load(f)
    else:
        try:
            locations = json.loads(args.locations)
        except json.JSONDecodeError:
            raise ValueError("Locations must be a valid JSON file or JSON string")
    
    fetcher = WeatherDataFetcher(args.api_key, args.gcs_bucket, args.gcs_prefix)
    fetcher.fetch_weather_data(locations)

if __name__ == "__main__":
    main()
