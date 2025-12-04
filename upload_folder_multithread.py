import os
import io
from pathlib import Path
import time
import mimetypes
from dataclasses import dataclass
from collections import deque

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaFileUpload
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive"]

# Google Drive parent folder ID (optional: set to None to upload to "My Drive" root)
# To get folder ID: open in browser, ID is in the URL
DRIVE_PARENT_ID = None  # e.g., '1A2b3C4d5E6f...' or None


@dataclass
class UploadTask:
    local_path: Path
    rel_path: Path
    parent_drive_id: str
    retries: int = 0


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


def list_files_with_cache(
    folder: str,
) -> int:
    """
    Iterate over all files in `folder` and cache the list in `cache_file`.

    Behaviour:
    - If `cache_file` does not exist: build the list, write it, and return the file count.
    - If `cache_file` exists:
        * Count how many lines (files) are in it.
        * Count how many files are currently in `folder`.
        * If the two counts are equal, return the cached count.
        * Otherwise, rebuild the cache and return the new count.
    """

    folder_path = Path(folder)

    cache_folder = Path(".", "tmp", folder_path.name)
    cache_folder.mkdir(parents=True, exist_ok=True)

    cache_file = cache_folder / "file_list.tmp"

    def count_real_files() -> int:
        total = 0
        for _, _, files in os.walk(folder_path):
            total += len(files)
        return total

    def rebuild_cache() -> int:

        # rebuild cache for `cache_file`
        print(f"rebuild cache for {str(cache_file)}")

        with cache_file.open("w", encoding="utf-8") as f:
            count = 0
            for root, _, files in os.walk(folder_path):
                for name in files:
                    full = Path(root) / name
                    # store paths relative to the folder for portability
                    rel = full.relative_to(folder_path)
                    f.write(str(rel) + "\n")
                    count += 1
        return cache_file, count

    if cache_file.exists():
        # number of cached files = number of lines in cache
        with cache_file.open("r", encoding="utf-8") as f:
            cached_count = sum(1 for _ in f)

        real_count = count_real_files()

        if cached_count == real_count:
            # folder size matches cache; assume cache is valid
            return cache_file, cached_count
        else:
            # mismatch: rebuild cache
            return rebuild_cache()
    else:
        # no cache: build from scratch
        return rebuild_cache()


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


def ensure_folder_for_relative_path(
    service, root_drive_id: str, rel_path, folder_cache: dict
) -> str:
    """
    Given a relative folder path like 'video/animation', ensure the
    corresponding folder structure exists under `root_drive_id` and
    return its Drive folder ID.
    """
    if rel_path in ("", ".", None):
        return root_drive_id

    if isinstance(rel_path, Path):
        rel_str = rel_path.as_posix()
    else:
        rel_str = str(rel_path)

    if rel_str in ("", "."):
        return root_drive_id

    if rel_str in folder_cache:
        return folder_cache[rel_str]

    parent_id = root_drive_id
    parts = rel_str.split("/")
    current_rel = []
    for part in parts:
        if not part:
            continue
        current_rel.append(part)
        current_rel_str = "/".join(current_rel)
        if current_rel_str in folder_cache:
            parent_id = folder_cache[current_rel_str]
            continue
        folder_id = get_or_create_folder(service, part, parent_id)
        folder_cache[current_rel_str] = folder_id
        parent_id = folder_id
    return parent_id


def build_tasks_from_cache(local_root: str, root_drive_id: str):
    """
    Read file_list.tmp for `local_root` and build upload tasks that
    preserve the relative folder structure under `root_drive_id`.
    """
    local_root_path = Path(local_root).resolve()
    cache_file = Path(".", "tmp", local_root_path.name) / "file_list.tmp"
    if not cache_file.exists():
        raise FileNotFoundError(f"Cache file not found: {cache_file}")

    tasks = deque()
    folder_cache = {}  # rel_folder_str -> drive_folder_id
    folder_cache[""] = root_drive_id

    service = authenticate_google_drive()

    with cache_file.open("r", encoding="utf-8") as f:
        for line in f:
            rel_str = line.strip()
            if not rel_str:
                continue
            rel_path = Path(rel_str)
            local_path = local_root_path / rel_path
            if not local_path.is_file():
                print(f"Skip missing file: {local_path}")
                continue

            rel_folder = rel_path.parent  # e.g. 'video/animation'
            drive_parent_id = ensure_folder_for_relative_path(
                service, root_drive_id, rel_folder, folder_cache
            )

            tasks.append(
                UploadTask(
                    local_path=local_path,
                    rel_path=rel_path,
                    parent_drive_id=drive_parent_id,
                )
            )
    return service, tasks


def should_retry(error: HttpError) -> bool:
    """Retry on typical transient Drive errors."""
    if not isinstance(error, HttpError):
        return False
    status = int(error.resp.status)
    return status in (403, 500, 502, 503, 504)


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


def process_queue(service, tasks: deque, max_retries: int = 5, base_delay: float = 1.0):
    """
    Process the upload task queue with exponential backoff retries.
    """
    while tasks:
        task = tasks.popleft()
        try:
            upload_file(service, task)
        except HttpError as e:
            task.retries += 1
            if task.retries <= max_retries and should_retry(e):
                delay = base_delay * (2 ** (task.retries - 1))
                print(
                    f"Error uploading {task.rel_path} (attempt {task.retries}): {e}. "
                    f"Retrying in {delay:.1f}s"
                )
                time.sleep(delay)
                tasks.append(task)
            else:
                print(
                    f"Failed to upload {task.rel_path} after {task.retries} attempts: {e}"
                )


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
        "--driver_id",
        type=str,
        required=True,
        help="ID of the root folder on Google Drive where the tree will be mirrored",
    )

    args = parser.parse_args()

    assert args.local, "Please provide a local folder to upload"

    cache_path, count = list_files_with_cache(args.local)
    print(f"Total number of files: {count}")
    print(f"cache {str(cache_path)}")

    service, tasks = build_tasks_from_cache(args.local, args.driver_id)
    print(f"Built {len(tasks)} upload tasks; starting upload...")

    print(service)
    print(len(tasks))

    # process_queue(service, tasks)
