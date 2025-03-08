import os
import shutil
import argparse
import pdfplumber


def extract_plumber(file_path) -> list:
    """
    Extract text from a PDF file using the pdfplumber library.
    """

    text = ""
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text() + "\n" if page.extract_text() else ""
    return text

def extract_pdfs(source_folder, target_folder):
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
                target_filename = os.path.splitext(file)[0] + ".txt"
                target_path = os.path.join(target_folder, target_filename)

                text = extract_plumber(source_path)

                with open(target_path, "w", encoding="utf-8") as txt_file:
                    txt_file.write(text)

                print(f"Extracted and saved: {target_path} ({len(text)} characters)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Recursively copy all PDF files from source to target folder."
    )
    parser.add_argument("source_folder", type=str, help="Path to the source folder.")
    parser.add_argument("target_folder", type=str, help="Path to the target folder.")

    args = parser.parse_args()
    extract_pdfs(args.source_folder, args.target_folder)
