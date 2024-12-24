import os
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed

def upload_file(s3_client, bucket_name, file_path, key_name):
    """
    Uploads a single file to S3 and prints the result.

    :param s3_client: A boto3 S3 client instance.
    :param bucket_name: Name of the S3 bucket.
    :param file_path: Absolute path to the local file.
    :param key_name: Key name (path) under which the file will be stored in S3.
    """
    try:
        # Using upload_file for automatic multipart handling
        #print('uploading file...')
        s3_client.upload_file(file_path, bucket_name, key_name.replace("\\","/"))
        print(f"Uploaded: {key_name}")
    except Exception as e:
        print(f"Failed to upload {file_path}: {e}")

def upload_directory_to_s3(directory_path, bucket_name, region='us-east-1', max_threads=10):
    """
    Recursively uploads all files from a local directory to the specified S3 bucket.

    :param directory_path: Absolute path of the directory to upload.
    :param bucket_name: The S3 bucket name.
    :param region: AWS region name for the S3 bucket (default: us-east-1).
    :param max_threads: Number of threads to use for concurrent uploads.
    """
    # Initialize S3 client
    s3_client = boto3.client('s3', region_name=region)

    # Validate directory
    if not os.path.isdir(directory_path):
        raise ValueError(f"Invalid directory path: {directory_path}")

    # Collect all files to upload
    files_to_upload = []
    for root, _, files in os.walk(directory_path):
        for filename in files:
            file_path = os.path.join(root, filename)
            # The key name mirrors the directory structure under the base directory
            key_name = os.path.relpath(file_path, directory_path)
            files_to_upload.append((file_path, key_name))

    # Multi-threaded upload
    print(f"Starting upload of {len(files_to_upload)} files from {directory_path} to bucket {bucket_name}...")
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Submit all upload tasks
        futures = {
            executor.submit(upload_file, s3_client, bucket_name, file_path, key_name): (file_path, key_name)
            for file_path, key_name in files_to_upload
        }

        # Wait for all to complete
        index = 0
        for future in as_completed(futures):
            # This ensures that any raised exceptions in threads are also surfaced here
            index += 1
            print(f"Uploaded file {index} files out of {len(files_to_upload)}")
            
            future.result()

    print("All files attempted. Check logs for individual results.")

if __name__ == "__main__":

    dir = input("Absolute path of the directory to upload: ")
    bucket = "sharders-rawcrawl" #input("S3 bucket name: ")

    upload_directory_to_s3(
        directory_path=dir,
        bucket_name=bucket,
        region='us-east-1',
        max_threads=50
    )
