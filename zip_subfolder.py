#!/usr/bin/env python3
import os
import subprocess


def zip_subfolders(local_folder, level=0):

    # parent = "."  # or set to an absolute path

    local_folder = os.path.abspath(local_folder)

    for name in os.listdir(local_folder):
        path = os.path.join(local_folder, name)
        if os.path.isdir(path):

            zip_path = os.path.join(local_folder, f"{name}.zip")

            print(f"zipping {path} to {zip_path}")

            # Build zip command
            # -r : recurse into directories
            # -q : quiet (drop if you want more output)
            # -<level> : compression level
            cmd = [
                "zip",
                f"-{level}",
                "-r",
                zip_path,
                name,
            ]

            # Run in local_folder so 'name' is a relative path
            subprocess.run(cmd, cwd=local_folder, check=True)


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
