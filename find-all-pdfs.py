import os
import shutil
import argparse


def copy_pdfs(source_folder, target_folder):
    """
    Recursively finds all PDF files in the source folder and copies them to the target folder.

    :param source_folder: The directory to search for PDF files.
    :param target_folder: The directory where PDF files will be copied.
    """
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    for root, _, files in os.walk(source_folder):
        for file in files:
            if file.lower().endswith(".pdf"):
                source_path = os.path.join(root, file)
                target_path = os.path.join(target_folder, file)

                # Ensure no filename conflict by renaming if necessary
                counter = 1
                while os.path.exists(target_path):
                    name, ext = os.path.splitext(file)
                    target_path = os.path.join(target_folder, f"{name}_{counter}{ext}")
                    counter += 1

                shutil.copy2(source_path, target_path)
                print(f"Copied: {source_path} -> {target_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Recursively copy all PDF files from source to target folder."
    )
    parser.add_argument("source_folder", type=str, help="Path to the source folder.")
    parser.add_argument("target_folder", type=str, help="Path to the target folder.")

    args = parser.parse_args()
    copy_pdfs(args.source_folder, args.target_folder)
