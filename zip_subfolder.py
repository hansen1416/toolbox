#!/usr/bin/env python3
import os
import shutil


def zip_subfolders(local_folder):

    # parent = "."  # or set to an absolute path

    for name in os.listdir(local_folder):
        path = os.path.join(local_folder, name)
        if os.path.isdir(path):

            zip_base = os.path.join(local_folder, name)  # full path *without* .zip

            print(f"zipping {path} to {zip_base}.zip")

            shutil.make_archive(zip_base, "zip", root_dir=local_folder, base_dir=name)


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--folder",
        type=str,
        required=True,
        help="the target folder to upload",
    )

    args = parser.parse_args()

    assert args.folder, "Please provide a local folder to compress"

    zip_subfolders(args.folder)
