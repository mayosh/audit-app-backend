import google.oauth2.credentials
import google_auth_oauthlib.flow
import os
import sys
import pickle

SHEET_SECRETS_FILE = 'creds/sheet_creds.json'


def build_sheet():
    SCOPES = ['https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets']
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        print ('no creds or not creds.valid')
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            print('no creds')
            flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
                SHEET_SECRETS_FILE, SCOPES)
            creds = flow.run_console() #run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)


if __name__ == "__main__":
    build_sheet()
