# Email CSV Processor

This project is designed to automatically fetch CSV files from Gmail, process them, and upload them to BigQuery. It is built using Python and integrates with Google Cloud services.

## Project Structure

- `main.py`: The entry point for the Cloud Function. Handles OAuth2 authentication, fetches emails, processes CSV files, and uploads data to BigQuery.
- `email_utils.py`: Contains functions related to email handling, including parsing and fetching CSV links.
- `bigquery_utils.py`: Contains functions for interacting with BigQuery, including uploading data and checking table existence.
- `data_processing_utils.py`: Contains functions for processing data, such as sanitizing column names and cleaning dataframes.

## Dependencies

This project requires the following Python packages. These can be installed using `pip` and are listed in the `requirements.txt` file:

- `functions-framework==3.*`
- `google-cloud-storage==2.10.0`
- `google-auth==2.21.0`
- `google-auth-oauthlib==1.0.0`
- `google-auth-httplib2==0.1.0`
- `google-api-python-client==2.94.0`
- `requests==2.31.0`
- `beautifulsoup4==4.12.2`
- `pandas==2.0.3`
- `google-cloud-bigquery==3.11.0`
- `flask==2.3.2`
- `pyarrow`

## Setup

1. **Clone the repository:**

   ```sh
   git clone https://github.com/yourusername/your-repo-name.git
   cd your-repo-name
