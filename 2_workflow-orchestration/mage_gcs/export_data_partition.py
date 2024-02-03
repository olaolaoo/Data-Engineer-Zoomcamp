import os
import pyarrow as pa
import pyarrow.parquet as pq

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# tell pyarrow where your credential lives
os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/home/src/coherent-ascent-379901-f8984bc6c655.json"

# tell pyarrow the bucket_name, project_id, table_name in GCS
bucket_name = 'mage-zoomcamp-lili'
project_id = 'coherent-ascent-379901'
table_name = 'nyc_taxi_data'

root_path = f'{bucket_name}/{table_name}'


@data_exporter
def export_data(data, *args, **kwargs):   
    data['tpep_pickup_date'] = data['tpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['tpep_pickup_date'],
        filesystem=gcs
    )