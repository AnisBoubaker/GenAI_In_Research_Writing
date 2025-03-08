import os
import re
from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError

def clean_text(text):
    """
    Cleans extracted text by removing non-ASCII characters and other unwanted characters.

    Args:
        text (str): Extracted text from the PDF.

    Returns:
        str: Cleaned text.
    """
    # Remove non-ASCII characters
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)

    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text)

    # Strip leading and trailing whitespace
    text = text.strip()

    return text

def extract_text_from_pdf(pdf_path, log_file):
    """
    Extracts text from a PDF file.
    Args:
        pdf_path (str): The path to the PDF file.
        log_file (file object): The log file to write errors to.
    Returns:
        str: Extracted text.
    """
    text = ""
    try:
        reader = PdfReader(pdf_path)
        for page in reader.pages:
            text += page.extract_text()
        text = clean_text(text)
    except PdfReadError as e:
        log_file.write(f"Error extracting text from {pdf_path}: {e}\n")
    except Exception as e:
        log_file.write(f"Unexpected error extracting text from {pdf_path}: {e}\n")
    return text

def process_pdfs(input_folder, output_folder, log_file):
    """
    Extracts text from all PDF files in the input folder and saves them as .txt files in the output folder.
    Args:
        input_folder (str): Path to the folder containing PDF files.
        output_folder (str): Path to the folder where .txt files will be saved.
        log_file (file object): The log file to write errors to.
    """
    # Ensure the output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Process each PDF in the input folder
    for file_name in os.listdir(input_folder):
        if file_name.lower().endswith('.pdf') and not file_name.startswith('._'):
            pdf_path = os.path.join(input_folder, file_name)
            text = extract_text_from_pdf(pdf_path, log_file)

            # Save the extracted text to a .txt file
            txt_file_name = os.path.splitext(file_name)[0] + ".txt"
            txt_file_path = os.path.join(output_folder, txt_file_name)
            try:
                with open(txt_file_path, 'w', encoding='utf-8') as txt_file:
                    txt_file.write(text)
                print(f"Extracted text saved to {txt_file_path}")
            except Exception as e:
                log_file.write(f"Error writing to file {txt_file_path}: {e}\n")

if __name__ == "__main__":
    # Specify the input and output folders
    input_folder = "/Volumes/AnisBoubake/ArxivPapers"
    output_folder = "/Volumes/AnisBoubake/ArxivPapersTxt"
    log_file_path = "txt_extraction_errors.txt"

    # Open the log file in append mode
    with open(log_file_path, 'a') as log_file:
        # Process the PDFs
        process_pdfs(input_folder, output_folder, log_file)