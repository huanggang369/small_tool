

import boto3
import os

def download_folder(s3_client, bucket_name, folder_prefix, local_dir):
    """
    Download all objects in a folder from S3 to local directory
    """
    # Create local directory if it doesn't exist
    os.makedirs(local_dir, exist_ok=True)
    
    try:
        # List all objects in the folder
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)
        
        downloaded_count = 0
        total_size = 0
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    
                    # Skip folder objects (keys ending with '/')
                    if key.endswith('/'):
                        print(f"Skipping folder object: {key}")
                        continue
                    
                    # Create local file path
                    local_path = os.path.join(local_dir, key)
                    
                    # Create subdirectories if needed
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    
                    # Download the file
                    print(f"Downloading: {key} -> {local_path}")
                    s3_client.download_file(bucket_name, key, local_path)
                    
                    downloaded_count += 1
                    total_size += obj['Size']
        
        print(f"\nDownload completed!")
        print(f"Downloaded {downloaded_count} files")
        print(f"Total size: {total_size / (1024*1024):.2f} MB")
        print(f"Files saved to: {os.path.abspath(local_dir)}")
        
    except Exception as e:
        print(f"Error downloading folder: {e}")
