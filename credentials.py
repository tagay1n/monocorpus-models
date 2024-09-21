import os

import google.auth.transport.requests
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# The OAuth 2.0 scopes we need.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly']


def get_credentials(credentials_file='credentials.json', token_file='token.json'):
    """Obtain OAuth 2.0 credentials or refresh token if expired."""

    creds = None

    # Load token if it exists
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # Refresh the access token using the refresh token
            creds.refresh(google.auth.transport.requests.Request())
        else:
            # If no valid credentials available, initiate OAuth2 login
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            creds = flow.run_local_server(access_type='offline', prompt='consent')

        # Save the credentials for future use
        with open(token_file, 'w') as token:
            token.write(creds.to_json())

    if not creds.refresh_token:
        raise ValueError('No refresh token found in credentials')
    return creds
