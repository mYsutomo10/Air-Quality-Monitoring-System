#!/usr/bin/env python3
import argparse
import csv
import datetime
import json
import os
import pandas as pd
from google.cloud import storage

class CloudStorageManager:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
    
    def store_dataframe(self, df, folder, filename=None):
        """Store a pandas DataFrame to GCS as CSV."""
        if filename is None:
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"data_{timestamp}.csv"
        
        # Ensure the folder path ends with a slash
        if not folder.endswith('/'):
            folder += '/'
        
        # Create the full path
        full_path = folder + filename
        
        # Convert DataFrame to CSV
        csv_data = df.to_csv(index=False)
        
        # Upload to GCS
        blob = self.bucket.blob(full_path)
        blob.upload_from_string(csv_data, content_type='text/csv')
        
        print(f"Uploaded DataFrame to gs://{self.bucket_name}/{full_path}")
        return f"gs://{self.bucket_name}/{full_path}"
    
    def store_json(self, data, folder, filename=None):
        """Store JSON data to GCS."""
        if filename is None:
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"data_{timestamp}.json"
        
        # Ensure the folder path ends with a slash
        if not folder.endswith('/'):
            folder += '/'
        
        # Create the full path
        full_path = folder + filename
        
        # Convert to JSON string
        json_data = json.dumps(data, indent=2)
        
        # Upload to GCS
        blob = self.bucket.blob(full_path)
        blob.upload_from_string(json_data, content_type='application/json')
        
        print(f"Uploaded JSON to gs://{self.bucket_name}/{full_path}")
        return f"gs://{self.bucket_name}/{full_path}"
    
    def store_file(self, local_path, gcs_path):
        """Upload a local file to GCS."""
        blob = self.bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        
        print(f"Uploaded {local_path} to gs://{self.bucket_name}/{gcs_path}")
        return f"gs://{self.bucket_name}/{gcs_path}"
    
    def list_files(self, prefix=None):
        """List files in the GCS bucket with an optional prefix."""
        blobs = self.storage_client.list_blobs(self.bucket_name, prefix=prefix)
        return [blob.name for blob in blobs]
    
    def download_file(self, gcs_path, local_path):
        """Download a file from GCS to local storage."""
        blob = self.bucket.blob(gcs_path)
        blob.download_to_filename(local_path)
        
        print(f"Downloaded gs://{self.bucket_name}/{gcs_path} to {local_path}")
        return local_path
    
    def merge_csv_files(self, folder, output_filename):
        """Merge multiple CSV files in a folder into one CSV file."""
        # List all files in the folder
        files = self.list_files(folder)
        csv_files = [f for f in files if f.endswith('.csv')]
        
        if not csv_files:
            print(f"No CSV files found in gs://{self.bucket_name}/{folder}")
            return None
        
        # Download and read all CSV files
        dfs = []
        for csv_file in csv_files:
            temp_file = f"/tmp/{os.path.basename(csv_file)}"
            self.download_file(csv_file, temp_file)
            
            try:
                df = pd.read_csv(temp_file)
                dfs.append(df)
                os.remove(temp_file)  # Clean up
            except Exception as e:
                print(f"Error reading {csv_file}: {e}")
        
        if not dfs:
            print("No valid CSV files to merge")
            return None
        
        # Concatenate all DataFrames
        merged_df = pd.concat(dfs, ignore_index=True)
        
        # Upload merged DataFrame
        return self.store_dataframe(merged_df, folder, output_filename)
    
    def delete_file(self, gcs_path):
        """Delete a file from GCS."""
        blob = self.bucket.blob(gcs_path)
        blob.delete()
        print(f"Deleted gs://{self.bucket_name}/{gcs_path}")

def main():
    parser = argparse.ArgumentParser(description='Store and manage data in Google Cloud Storage')
    parser.add_argument('--bucket', required=True, help='GCS bucket name')
    parser.add_argument('--action', required=True, choices=['upload', 'download', 'list', 'merge', 'delete'],
                        help='Action to perform')
    parser.add_argument('--local_path', help='Local file path for upload/download')
    parser.add_argument('--gcs_path', help='GCS path for upload/download/delete')
    parser.add_argument('--folder', help='GCS folder for listing or merging files')
    parser.add_argument('--output', help='Output filename for merged files')
    args = parser.parse_args()
    
    manager = CloudStorageManager(args.bucket)
    
    if args.action == 'upload':
        if not args.local_path or not args.gcs_path:
            print("Error: --local_path and --gcs_path are required for upload")
            return
        manager.store_file(args.local_path, args.gcs_path)
    
    elif args.action == 'download':
        if not args.local_path or not args.gcs_path:
            print("Error: --local_path and --gcs_path are required for download")
            return
        manager.download_file(args.gcs_path, args.local_path)
    
    elif args.action == 'list':
        files = manager.list_files(args.folder)
        print("Files in GCS bucket:")
        for file in files:
            print(f"  gs://{args.bucket}/{file}")
    
    elif args.action == 'merge':
        if not args.folder or not args.output:
            print("Error: --folder and --output are required for merge")
            return
        manager.merge_csv_files(args.folder, args.output)
    
    elif args.action == 'delete':
        if not args.gcs_path:
            print("Error: --gcs_path is required for delete")
            return
        manager.delete_file(args.gcs_path)

if __name__ == "__main__":
    main()