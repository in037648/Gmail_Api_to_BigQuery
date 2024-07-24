import base64
import requests
from bs4 import BeautifulSoup

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

def fetch_csv_links_from_emails(service, query):
    results = service.users().messages().list(userId="me", q=query).execute()
    messages = results.get("messages", [])

    if not messages:
        print("No messages found.")
        return {}

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

    return file_contents_by_subject

