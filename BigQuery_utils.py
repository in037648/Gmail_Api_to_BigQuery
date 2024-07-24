from google.cloud import bigquery
import pandas as pd

def table_exists(client, dataset_id, table_id):
    try:
        client.get_table(f"{dataset_id}.{table_id}")
        return True
    except Exception:
        return False

def upload_to_bigquery(dataset_id, table_id, dataframe):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    
    if not table_exists(client, dataset_id, table_id):
        schema = [
            bigquery.SchemaField(name, bigquery.enums.SqlTypeNames.STRING) 
            for name in dataframe.columns
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Created table {table_id} in dataset {dataset_id}")

    # Fetch the schema of the existing table
    table = client.get_table(table_ref)
    table_schema = {field.name: field.field_type for field in table.schema}
    
    # Convert DataFrame columns to match the schema
    for column, field_type in table_schema.items():
        if field_type == 'STRING':
            dataframe[column] = dataframe[column].astype(str)
        elif field_type == 'INTEGER':
            dataframe[column] = pd.to_numeric(dataframe[column].str.replace(',', '').str.replace('$', ''), errors='coerce').fillna(0).astype(int)
        elif field_type == 'FLOAT':
            dataframe[column] = pd.to_numeric(dataframe[column].str.replace(',', '').str.replace('$', ''), errors='coerce').astype(float)
        elif field_type == 'BOOLEAN':
            dataframe[column] = dataframe[column].astype(bool)
        # No changes to date or datetime columns

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
    
    try:
        job.result()
        print(f"Loaded {dataframe.shape[0]} rows into {dataset_id}:{table_id}.")
    except Exception as e:
        print(f"An error occurred while loading data to BigQuery: {e}")
