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

SCOPES = ["https://www.googleapis.com/auth/drive"]

# Optional: set to None to upload to My Drive root; otherwise a folder ID
DRIVE_PARENT_ID = None


@dataclass
class UploadTask:
    local_path: Path
    rel_path: Path
    parent_drive_id: str
    retries: int = 0


def authenticate_google_drive():
    """Authenticate and return a Drive v3 service client."""
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


def list_files_with_cache(folder: str):
    """
    Scan `folder` and maintain two cache files:

    - files_todo.tmp : files that still need to be uploaded.
    - files_done.tmp : files that have been uploaded successfully.

    Behaviour:
    - Read `files_done.tmp` (if any) to know what has already been uploaded.
    - Walk the folder and compute the current set of files.
    - `todo` := current_files \ done_files
    - Overwrite `files_todo.tmp` with the todo list.

    Returns:
        todo_cache_path, done_cache_path, total_count, todo_count
    """
    folder_path = Path(folder).resolve()
    cache_folder = Path(".", "tmp", folder_path.name)
    cache_folder.mkdir(parents=True, exist_ok=True)

    todo_cache = cache_folder / "files_todo.tmp"
    done_cache = cache_folder / "files_done.tmp"

    # Load already uploaded files (relative POSIX paths)
    done_set = set()
    if done_cache.exists():
        with done_cache.open("r", encoding="utf-8") as f:
            for line in f:
                rel = line.strip()
                if rel:
                    done_set.add(rel)

    # Scan current filesystem
    current_files = []
    total_count = 0
    for root, _, files in os.walk(folder_path):
        for name in files:
            full = Path(root) / name
            rel = full.relative_to(folder_path).as_posix()
            current_files.append(rel)
            total_count += 1

    # Compute todo = current_files - done_files
    todo_files = [rel for rel in current_files if rel not in done_set]

    # Overwrite todo cache
    with todo_cache.open("w", encoding="utf-8") as f:
        for rel in todo_files:
            f.write(rel + "\n")

    return todo_cache, done_cache, total_count, len(todo_files)


def get_or_create_folder(service, folder_name, parent_id=None):
    """Search for folder by name under parent, create if it does not exist."""
    query = (
        f"name='{folder_name}' and "
        "mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
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


def build_tasks_from_cache(local_root: str, root_drive_id: str, todo_cache: Path):
    """
    Read `todo_cache` and build upload tasks that preserve the relative
    folder structure under `root_drive_id`.
    """
    local_root_path = Path(local_root).resolve()
    if not todo_cache.exists():
        raise FileNotFoundError(f"Todo cache file not found: {todo_cache}")

    service = authenticate_google_drive()

    tasks = deque()
    folder_cache = {}  # rel_folder_str -> drive_folder_id
    folder_cache[""] = root_drive_id

    with todo_cache.open("r", encoding="utf-8") as f:
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


def upload_file(service, task: UploadTask):
    """Upload a single file to Google Drive, given an UploadTask."""
    file_path = task.local_path
    drive_parent_id = task.parent_drive_id
    file_name = file_path.name

    # Check if file already exists in that folder
    query = f"name='{file_name}' and '{drive_parent_id}' in parents and trashed=false"
    response = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id, name, size)")
        .execute()
    )
    existing_files = response.get("files", [])

    local_size = file_path.stat().st_size

    # Optional: skip if same size (simple deduplication)
    for existing in existing_files:
        if int(existing.get("size", 0)) == local_size:
            print(f"Skipped (already exists): {task.rel_path}")
            return existing["id"]

    # Upload (resumable)
    file_metadata = {"name": file_name, "parents": [drive_parent_id]}
    media = MediaFileUpload(
        str(file_path),
        mimetype=mimetypes.guess_type(file_name)[0] or "application/octet-stream",
        resumable=True,
    )

    request = service.files().create(body=file_metadata, media_body=media, fields="id")
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            print(f"Uploading {task.rel_path}: {int(status.progress() * 100)}%")

    print(f"Uploaded: {task.rel_path} (ID: {response.get('id')})")
    return response.get("id")


def process_queue(
    service,
    tasks: deque,
    done_cache: Path,
    max_retries: int = 5,
    base_delay: float = 1.0,
):
    """
    Process the upload task queue with exponential backoff retries.
    On successful upload, append the file to `done_cache`.
    """
    while tasks:
        task = tasks.popleft()
        try:
            upload_file(service, task)
            # Mark as done (store relative path in POSIX form)
            with done_cache.open("a", encoding="utf-8") as f:
                f.write(task.rel_path.as_posix() + "\n")
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

    todo_cache, done_cache, total_count, todo_count = list_files_with_cache(args.local)
    print(f"Total number of files on disk: {total_count}")
    print(f"Files remaining to upload: {todo_count}")
    print(f"Todo cache: {todo_cache}")
    print(f"Done cache: {done_cache}")

    service, tasks = build_tasks_from_cache(args.local, args.driver_id, todo_cache)
    print(f"Built {len(tasks)} upload tasks; starting upload...")

    process_queue(service, tasks, done_cache)
