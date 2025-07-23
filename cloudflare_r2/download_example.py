

import boto3

# Create S3 client (not resource) for list_objects operation
s3_client = boto3.client('s3',
  endpoint_url = 'https://6d18700b4bd3fff2b330035c35b0bbeb.r2.cloudflarestorage.com',
  aws_access_key_id = '6e17dce5e6699ec405f7ec07deecf321',
  aws_secret_access_key = '4916026eb627351979f06b898b4f8f67137caba46c39bab83934eb29a97f92ad'
)

# List objects in the bucket
try:
    response = s3_client.list_objects_v2(Bucket='tmp-transfer2', Prefix='noval/fanqie')
    
    # Check if the request was successful
    if 'Contents' in response:
        print(f"Found {len(response['Contents'])} objects in bucket:")
        for obj in response['Contents']:
            print(f"  - {obj['Key']} (Size: {obj['Size']} bytes, Last Modified: {obj['LastModified']})")
    else:
        print("No objects found in bucket or bucket is empty")
        
except Exception as e:
    print(f"Error listing objects: {e}")



# from download_folder import download_folder

# download_folder(s3_client, 
#                 'tmp-transfer2', 
#                 'noval', 
#                 '/mnt/llm-process/network_novel/raw')

