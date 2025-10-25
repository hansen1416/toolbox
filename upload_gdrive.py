#!/usr/bin/python

import os
import io
from datetime import date
from queue import Queue
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive"]

# Google Drive parent folder ID (optional: set to None to upload to "My Drive" root)
# To get folder ID: open in browser, ID is in the URL
DRIVE_PARENT_ID = None  # e.g., '1A2b3C4d5E6f...' or None


# Token and credentials files
def authenticate_google_drive():
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.

    for more detail of this method:
        https://developers.google.com/workspace/drive/api/quickstart/python?utm_source=chatgpt.com
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return build("drive", "v3", credentials=creds)


def get_or_create_folder(service, folder_name, parent_id=None):
    """Search for folder by name under parent, create if not exists."""
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"

    response = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id, name)")
        .execute()
    )
    folders = response.get("files", [])

    if folders:
        return folders[0]["id"]
    else:
        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
        }
        if parent_id:
            file_metadata["parents"] = [parent_id]

        folder = service.files().create(body=file_metadata, fields="id").execute()
        print(f"Created folder: {folder_name} (ID: {folder.get('id')})")
        return folder.get("id")


def upload_file(service, file_path, drive_parent_id):
    """Upload a single file to Google Drive."""
    file_name = os.path.basename(file_path)

    # Check if file already exists
    query = f"name='{file_name}' and '{drive_parent_id}' in parents and trashed=false"
    response = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id, name, size)")
        .execute()
    )
    existing_files = response.get("files", [])

    local_size = os.path.getsize(file_path)

    # Optional: skip if same size (simple deduplication)
    for existing in existing_files:
        if int(existing.get("size", 0)) == local_size:
            print(f"Skipped (already exists): {file_path}")
            return existing["id"]

    # Upload
    file_metadata = {"name": file_name, "parents": [drive_parent_id]}
    media = MediaIoBaseUpload(
        io.BytesIO(open(file_path, "rb").read()),
        mimetype="application/octet-stream",
        resumable=True,
    )

    request = service.files().create(body=file_metadata, media_body=media, fields="id")
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            print(f"Uploading {file_name}: {int(status.progress() * 100)}%")

    print(f"Uploaded: {file_path} (ID: {response.get('id')})")
    return response.get("id")


def upload_directory(service, local_path, drive_parent_id):
    """Recursively upload directory maintaining structure."""
    folder_name = os.path.basename(local_path)
    current_drive_id = get_or_create_folder(service, folder_name, drive_parent_id)

    # Use queue for BFS traversal (preserves structure)
    q = Queue()
    q.put((local_path, current_drive_id))

    while not q.empty():
        current_local, current_drive = q.get()

        for item in os.listdir(current_local):
            item_local_path = os.path.join(current_local, item)

            if os.path.isdir(item_local_path):
                # Create subfolder and enqueue
                subfolder_drive_id = get_or_create_folder(service, item, current_drive)
                q.put((item_local_path, subfolder_drive_id))
            else:
                # Upload file
                upload_file(service, item_local_path, current_drive)


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--local",
        type=str,
        required=True,
        help="the target folder to upload",
    )

    args = parser.parse_args()

    assert args.local, "Please provide a local folder to upload"

    # get the abs path of the local folder
    LOCAL_ROOT = os.path.abspath(args.local)

    print(f"Starting upload from: {LOCAL_ROOT}")
    if not os.path.exists(LOCAL_ROOT):
        print(f"Error: Local directory not found: {LOCAL_ROOT}")
        exit(0)

    service = authenticate_google_drive()

    root_folder_name = os.path.basename(os.path.normpath(LOCAL_ROOT))
    root_drive_id = DRIVE_PARENT_ID

    if DRIVE_PARENT_ID is None:
        print("Uploading to My Drive root...")
    else:
        print(f"Uploading inside parent folder ID: {DRIVE_PARENT_ID}")

    upload_directory(service, LOCAL_ROOT, root_drive_id or "root")
    print("Upload completed successfully!")
