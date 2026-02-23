import io
import os
import requests
import pandas as pd
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "de_zoomcamp_03_nyc_taxi_csv")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 10 * 1024 * 1024  # 10 MB
    storage.blob._DEFAULT_CHUNKSIZE = 10 * 1024 * 1024  # 10 MB

    client = storage.Client(project = 'de-zoomcamp-terraform-demo')
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def nyc_data_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.parquet"

        # download it using requests
        request_url = f"{init_url}{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        # print(f"Local: {file_name}")

        # get row count
        df = pd.read_parquet(file_name)
        # print(f"Row count: {len(df)}")

        # transform to csv
        csv_file_name = file_name.replace('.parquet', '.csv')
        df.to_csv(csv_file_name, index=False)
        print(f"CSV: {csv_file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{csv_file_name}", csv_file_name)
        print(f"GCS: {service}/{csv_file_name}")    
        os.remove(file_name)
        os.remove(csv_file_name)


nyc_data_to_gcs('2019', 'green')
nyc_data_to_gcs('2020', 'green')
# nyc_data_to_gcs('2019', 'yellow')
# nyc_data_to_gcs('2020', 'yellow')
# nyc_data_to_gcs('2019', 'fhv')
# nyc_data_to_gcs('2020', 'fhv')


