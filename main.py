import json
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud import storage
import gcsfs
import pandas as pd


# gcs_client = storage.Client()
# # Retrieve an existing bucket
# # https://console.cloud.google.com/storage/browser/[bucket-id]/
# bucket = gcs_client.get_bucket('gs://poc-bucket-data-transfer')
# # Then do other things...
# blob = bucket.blob('file1.json')
# data = json.loads(blob.download_as_string(client=None))
# print(data)

def handle_bq():
    # Construct a BigQuery client object.
    bq_client = bigquery.Client()

    query = """
            SELECT name, role, age FROM 
            `accenture-poc-335313.sample.Employee`;
    """
    query_job = bq_client.query(query)  

    print("The query data:")
    for row in query_job:
        # Row values can be accessed by field name or index.
        print("panId={}, name={}".format(row[0], row["role"]))


def create_or_update_table_data(project_id=None, dataset_id=None, table_name=None, 
                                schema_fields=None, data=None):
    '''
        Params:
            Project ID 
            Dataset ID
            Table Name
            Schema Fields
            Data
    '''

    client = bigquery.Client()
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    # Checking table exist or Not, 
    # if not then creating new table in exception section
    try:
        client.get_table(table_id)  
        print(f"Table {table_id} already exists.")
    except NotFound:
        print(f"Table {table_id} is not found.")
        schema = []
        for field in schema_fields:
            schema.append(bigquery.SchemaField(field[0], field[1], mode=field[2]))

        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    # If data is null then returning back to avoid errors
    if len(data) == 0:
        return 0

    # inserting data in chunks of 10000
    chunk = 10000
    end = 10000
    size = len(data) - 1
    if len(data) < 10000:
        errors = client.insert_rows_json(
            table_id, data,
        )  
        if not errors:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
    else:
        for start in range(0, size, chunk)[:-1]:
            # print(start, end - 1)
            data_to_insert = data[start: (end - 1)]
            errors = client.insert_rows_json(
                table_id, data_to_insert,
            )  
            if not errors:
                print("New rows have been added.")
            else:
                print("Encountered errors while inserting rows: {}".format(errors))
            end += chunk
        else:
            data_to_insert = data[(start + chunk): size]
            errors = client.insert_rows_json(
                table_id, data_to_insert,
            )  
            if not errors:
                print("New rows have been added.")
            else:
                print("Encountered errors while inserting rows: {}".format(errors))


def read_from_gcs(file_name=""):

    gcs_file_system = gcsfs.GCSFileSystem(project="accenture-poc-335313")

    gcs_json_path = "gs://poc-bucket-accenture/"

    if file_name.endswith(".csv"):
        df = pd.read_csv(f"gs://poc-bucket-accenture/{file_name}", encoding='utf-8')
        return df.to_dict('r')
    
    with gcs_file_system.open(str(gcs_json_path + file_name)) as f:
        json_dict = json.load(f)
        
        return json_dict


if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "accenture-poc-service.json"

    file_data = read_from_gcs("file1.json")
    
    schema_fields = [
            ("name", "STRING", "NULLABLE"),
            ("role", "STRING", "NULLABLE"),
            ("age", "INT64", "NULLABLE")
        ]

    create_or_update_table_data("accenture-poc-335313", "sample", 'Employee', schema_fields, file_data)
