#!/usr/bin/env python3
import argparse
import csv
import datetime
import json
import os
import time
import requests
from google.cloud import storage

class Sentinel5PDataFetcher:
    def __init__(self, api_key, gcs_bucket, gcs_prefix):
        self.api_key = api_key
        self.gcs_bucket = gcs_bucket
        self.gcs_prefix = gcs_prefix
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(gcs_bucket)
    
    def fetch_satellite_data(self, locations, pollutants=['NO2', 'CO', 'O3', 'SO2', 'PM25', 'PM10']):
        """Fetch Sentinel-5P data for multiple locations and pollutants."""
        results = []
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for location in locations:
            location_name = location['name']
            lat = location['latitude']
            lon = location['longitude']
            
            location_data = {
                'location_name': location_name,
                'latitude': lat,
                'longitude': lon,
                'timestamp': datetime.datetime.now().isoformat()
            }
            
            for pollutant in pollutants:
                try:
                    # This is a placeholder - you would need to replace with the actual Sentinel-5P API endpoint
                    # Example using Copernicus Atmosphere Monitoring Service (CAMS) API
                    url = f"https://api.sentinel-hub.com/api/v1/process"
                    
                    # Payload would depend on the specific API you're using
                    payload = {
                        "input": {
                            "bounds": {
                                "properties": {
                                    "crs": "http://www.opengis.net/def/crs/EPSG/0/4326"
                                },
                                "bbox": [lon-0.05, lat-0.05, lon+0.05, lat+0.05]
                            },
                            "data": [
                                {
                                    "type": "sentinel-5p-l2",
                                    "dataFilter": {
                                        "timeRange": {
                                            "from": (datetime.datetime.now() - datetime.timedelta(days=7)).isoformat() + "Z",
                                            "to": datetime.datetime.now().isoformat() + "Z"
                                        }
                                    }
                                }
                            ]
                        },
                        "evalscript": """
                            //VERSION=3
                            function setup() {
                                return {
                                    input: [{
                                        bands: ["" + POLLUTANT + ""]
                                    }],
                                    output: {
                                        bands: 1
                                    }
                                };
                            }
                            
                            function evaluatePixel(sample) {
                                return [sample["" + POLLUTANT + ""]];
                            }
                        """.replace("POLLUTANT", pollutant.lower())
                    }
                    
                    headers = {
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {self.api_key}"
                    }
                    
                    # NOTE: This is commented out because it's a placeholder
                    # In a real implementation, you would use the actual API
                    # response = requests.post(url, json=payload, headers=headers)
                    # response.raise_for_status()
                    # data = response.json()
                    # pollutant_value = self._extract_pollutant_value(data)
                    
                    # For this example, generate some placeholder data
                    import random
                    pollutant_value = random.uniform(0, 100)
                    
                    # Add to location data
                    location_data[pollutant.lower()] = pollutant_value
                    
                    print(f"Fetched {pollutant} data for {location_name}")
                    
                    # Sleep briefly to avoid hitting API rate limits
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"Error fetching {pollutant} data for {location_name}: {e}")
                    location_data[pollutant.lower()] = None
            
            results.append(location_data)
        
        # Save results to CSV in GCS
        if results:
            csv_filename = f"satellite_data_{timestamp}.csv"
            self._save_to_csv(results, csv_filename)
            self._upload_to_gcs(csv_filename, f"{self.gcs_prefix}/{csv_filename}")
            
            # Clean up local file
            os.remove(csv_filename)
        
        return results
    
    def _extract_pollutant_value(self, data):
        """Extract pollutant value from API response."""
        # This would depend on the specific API and response format
        # This is a placeholder implementation
        try:
            # Example: Averaging values from the response
            values = data.get('data', [])
            if values:
                return sum(values) / len(values)
            return None
        except Exception:
            return None
    
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
    parser = argparse.ArgumentParser(description='Fetch Sentinel-5P data and store in GCS')
    parser.add_argument('--api_key', required=True, help='Sentinel-5P API key')
    parser.add_argument('--gcs_bucket', required=True, help='GCS bucket name')
    parser.add_argument('--gcs_prefix', default='satellite_data', help='GCS prefix/folder')
    parser.add_argument('--locations', required=True, help='JSON file with locations or JSON string')
    parser.add_argument('--pollutants', default='NO2,CO,O3,SO2,PM25,PM10', help='Comma-separated list of pollutants')
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
    
    # Parse pollutants list
    pollutants = args.pollutants.split(',')
    
    fetcher = Sentinel5PDataFetcher(args.api_key, args.gcs_bucket, args.gcs_prefix)
    fetcher.fetch_satellite_data(locations, pollutants)

if __name__ == "__main__":
    main()