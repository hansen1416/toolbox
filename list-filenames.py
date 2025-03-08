import os
import argparse


def concatenate_pdf_filenames(folder_path):
    # List all PDF files in the folder
    pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".pdf")]

    # Concatenate filenames with commas
    result_string = "; ".join(pdf_files)

    return result_string


def main():
    parser = argparse.ArgumentParser(
        description="Find all PDF files in a folder and concatenate their names."
    )
    parser.add_argument("folder_path", type=str, help="Path to the target folder")
    args = parser.parse_args()

    result = concatenate_pdf_filenames(args.folder_path)
    print(result)


if __name__ == "__main__":
    main()
