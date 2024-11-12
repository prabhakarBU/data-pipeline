from b2sdk.v1 import InMemoryAccountInfo, B2Api
import shutil

def get_authenticated_b2_api():
    try:
        info = InMemoryAccountInfo()
        b2_api = B2Api(info)
        
        # Replace with your Backblaze B2 application keys
        application_key_id = ""
        application_key = ""

        
        # Authenticate with Backblaze B2
        b2_api.authorize_account("production", application_key_id, application_key)
                
    except Exception as e:
        print("error occurred:" + e)
    
    return b2_api

def create_bucket(b2_api, bucket_name):
    try:
        bucket = b2_api.get_bucket_by_name(bucket_name)
        print(bucket)
    except Exception as e:
        print(e)
        print(str(e).lower())
        if str(e).lower() == 'no such bucket':
            print("no such bucket found")
        else:
            print("creating the bucket now...")
            bucket = b2_api.create_bucket(bucket_name,"allPrivate")
            print(bucket)

    return bucket

def upload_data(b2_api, compressed_file_path, delta_path):
    try:
        shutil.make_archive(compressed_file_path.replace('.tar.gz', ''), 'gztar', delta_path)
        # Define bucket and file details
        bucket_name = "datasnake-demo-3"
        bucket = b2_api.get_bucket_by_name(bucket_name)
        object_name = "delta-lake-archive.tar.gz"

        # Upload file
        with open(compressed_file_path, "rb") as file:
            bucket.upload_bytes(file.read(), object_name)
    except Exception as e:
        print("Exception while uploading:")    
        print(e)
        
def list_files(b2_api):
    bucket_name = "datasnake-demo-3"
    bucket = b2_api.get_bucket_by_name(bucket_name)
    # List objects in the bucket
    for file_version, folder_name in bucket.ls(recursive=True):  # `ls()` lists files recursively
        print(f"File name: {file_version.file_name}, Size: {file_version.size}")

b2 = get_authenticated_b2_api()
# create_bucket(b2, "datasnake-demo-3")
delta_path = "delta-lake/yfinance-trade-data"
compressed_file_path = "delta-lake-compressed/yfinance-trade-data-compressed/delta-lake-archive.tar.gz"
# upload_data(b2,compressed_file_path, delta_path)
list_files(b2)