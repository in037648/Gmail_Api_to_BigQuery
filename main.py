from __future__ import print_function
import base64
import google.auth
from google.cloud import storage
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import requests
from bs4 import BeautifulSoup
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import io

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def get_email_body(payload):
    if 'data' in payload['body']:
        return base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8')
    if 'parts' in payload:
        for part in payload['parts']:
            if part['mimeType'] == 'text/html' and 'data' in part['body']:
                return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
            elif part['mimeType'] == 'text/plain' and 'data' in part['body']:
                return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
            elif part['mimeType'].startswith('multipart'):
                body = get_email_body(part)
                if body:
                    return body
    return None

def sanitize_subject(subject):
    keepcharacters = (' ', '.', '_')
    return "".join(c for c in subject if c.isalnum() or c in keepcharacters).rstrip().replace(" ", "_")

def format_column_names(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    df.columns = df.columns.str.replace(r'\W', '_', regex=True)
    return df

def table_exists(client, dataset_id, table_id):
    try:
        client.get_table(f"{dataset_id}.{table_id}")
        return True
    except Exception:
        return False

def clean_dataframe(df):
    # Drop duplicate rows
    df = df.drop_duplicates()
    return df

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
            dataframe[column] = pd.to_numeric(dataframe[column], errors='coerce').fillna(0).astype(int)
        elif field_type == 'FLOAT':
            dataframe[column] = pd.to_numeric(dataframe[column], errors='coerce').astype(float)
        elif field_type == 'BOOLEAN':
            dataframe[column] = dataframe[column].astype(bool)
        # Add more type conversions as necessary

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
    
    try:
        job.result()
        print(f"Loaded {dataframe.shape[0]} rows into {dataset_id}:{table_id}.")
    except Exception as e:
        print(f"An error occurred while loading data to BigQuery: {e}")

def main(request):
    creds = Credentials.from_authorized_user_info({
        "token": "token",
        "refresh_token": "refresh_toekn",
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": "client_id",
        "client_secret": "secret",
        "scopes": ["https://www.googleapis.com/auth/gmail.readonly"],
        "universe_domain": "googleapis.com",
        "account": "",
        "expiry": "2024-06-05T10:11:52.158995Z"
    })

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("gmail", "v1", credentials=creds)
        
        current_date = datetime.utcnow()
        last_1_days = current_date - timedelta(days=0)
        formatted_date = last_1_days.strftime('%Y/%m/%d')
        
        query = f"after:{formatted_date} Your report is ready"
        
        results = service.users().messages().list(userId="me", q=query).execute()
        messages = results.get("messages", [])

        if not messages:
            print("No messages found.")
            return "No messages found."

        print("Messages with report links:")
        file_contents_by_subject = {}
        for message in messages:
            msg = service.users().messages().get(userId="me", id=message['id']).execute()
            msg_payload = msg['payload']
            headers = msg_payload.get('headers', [])
            subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
            sender = next((h['value'] for h in headers if h['name'] == 'From'), 'No Sender')
            print(f"Processing email from {sender} with subject '{subject}'")
            msg_str = get_email_body(msg_payload)
            
            if msg_str:
                soup = BeautifulSoup(msg_str, 'html.parser')
                links = [a['href'] for a in soup.find_all('a', href=True) if 'csv' in a['href'].lower() or 'download' in a.get_text().lower()]
                
                if links:
                    for csv_link in links:
                        print(f'Downloading CSV from: {csv_link}')
                        response = requests.get(csv_link)
                        content = response.content.decode("utf-8")
                        
                        if subject in file_contents_by_subject:
                            file_contents_by_subject[subject] += "\n" + content
                        else:
                            file_contents_by_subject[subject] = content
                        
        dataset_id = 'ometria'
        for subject, content in file_contents_by_subject.items():
            sanitized_subject = sanitize_subject(subject)
            df = pd.read_csv(io.StringIO(content))
            df = format_column_names(df)
            
            # Clean and deduplicate the DataFrame
            df = clean_dataframe(df)

            table_id = sanitized_subject
            upload_to_bigquery(dataset_id, table_id, df)
            
            print(f'Merged and formatted CSV for subject "{subject}" uploaded to BigQuery.')
                        
    except HttpError as error:
        print(f"An error occurred: {error}")
        return f"An error occurred: {error}"

    return "Success"

# The Cloud Function will call the main function when triggered
if __name__ == "__main__":
    from flask import Flask, request
    import os
    
    app = Flask(__name__)

    @app.route("/", methods=["POST"])
    def run():
        return main(request)

    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
            
