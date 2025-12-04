#!/usr/bin/python
"""
Multithreaded Google Drive uploader for local folders.
Uses the same authentication model as upload_gdrive.py but uploads files
concurrently while preserving the directory structure in Drive.
"""

import io
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict

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


def authenticate_google_drive():
    """Authenticate and return an authorized Drive service client."""
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
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

    query = f"name='{file_name}' and '{drive_parent_id}' in parents and trashed=false"
    response = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id, name, size)")
        .execute()
    )
    existing_files = response.get("files", [])

    local_size = os.path.getsize(file_path)

    for existing in existing_files:
        if int(existing.get("size", 0)) == local_size:
            print(f"Skipped (already exists): {file_path}")
            return existing["id"]

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


def upload_directory_multithread(service, local_path, drive_parent_id, max_workers=4):
    """Recursively upload directory while uploading files concurrently."""
    folder_name = os.path.basename(local_path.rstrip(os.sep))
    root_drive_id = get_or_create_folder(service, folder_name, drive_parent_id)

    # Map absolute local directory paths to their Drive folder IDs to avoid
    # mixing local names with Drive IDs when traversing.
    folder_ids: Dict[str, str] = {local_path: root_drive_id}
    upload_futures = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for current_local, dirs, files in os.walk(local_path):
            parent_drive_id = folder_ids[current_local]

            for d in dirs:
                drive_id = get_or_create_folder(service, d, parent_drive_id)
                folder_ids[os.path.join(current_local, d)] = drive_id

            for f in files:
                local_file_path = os.path.join(current_local, f)
                future = executor.submit(
                    upload_file, service, local_file_path, parent_drive_id
                )
                upload_futures.append(future)

        for future in as_completed(upload_futures):
            future.result()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local",
        type=str,
        required=True,
        help="the target folder to upload",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="number of concurrent uploads",
    )

    args = parser.parse_args()

    assert args.local, "Please provide a local folder to upload"

    local_root = os.path.abspath(args.local)
    print(f"Starting upload from: {local_root}")
    if not os.path.exists(local_root):
        print(f"Error: Local directory not found: {local_root}")
        raise SystemExit(1)

    service = authenticate_google_drive()

    if DRIVE_PARENT_ID is None:
        print("Uploading to My Drive root...")
    else:
        print(f"Uploading inside parent folder ID: {DRIVE_PARENT_ID}")

    upload_directory_multithread(
        service, local_root, DRIVE_PARENT_ID or "root", args.max_workers
    )
    print("Upload completed successfully!")
