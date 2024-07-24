from flask import Flask, request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from email_utils import fetch_csv_links_from_emails
from bigquery_utils import upload_to_bigquery
from data_processing_utils import sanitize_subject, format_column_names, clean_dataframe

from datetime import datetime, timedelta
import io
import pandas as pd

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def main(request):
    creds = Credentials.from_authorized_user_info({
        "token": "access_token",
        "refresh_token": "refresh_token",
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": "client_id",
        "client_secret": "client_secret",
        "scopes": SCOPES,
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
        last_1_days = current_date - timedelta(days=1)
        formatted_date = last_1_days.strftime('%Y/%m/%d')
        
        query = f"after:{formatted_date} Your report is ready"
        
        file_contents_by_subject = fetch_csv_links_from_emails(service, query)

        if not file_contents_by_subject:
            return "No messages found."

        dataset_id = 'your_dataset_id'
        for subject, content in file_contents_by_subject.items():
            sanitized_subject = sanitize_subject(subject)
            df = pd.read_csv(io.StringIO(content))
            original_columns = df.columns.tolist()
            df = format_column_names(df)
            
            # Clean and deduplicate the DataFrame
            df = clean_dataframe(df, original_columns)

            table_id = sanitized_subject
            upload_to_bigquery(dataset_id, table_id, df)
            
            print(f'Merged and formatted CSV for subject "{subject}" uploaded to BigQuery.')
                        
    except HttpError as error:
        print(f"An error occurred: {error}")
        return f"An error occurred: {error}"

    return "Success"

app = Flask(__name__)

@app.route("/", methods=["POST"])
def run():
    return main(request)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
