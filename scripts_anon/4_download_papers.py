import os
import time
import requests
import mysql.connector
from mysql.connector import Error

def download_papers(download_folder: str):
    """
    1. Connect to DB.
    2. Find all papers where to_download=1 AND downloaded=0.
    3. For each paper:
       - Download PDF from pdf_url.
       - Save it to the specified download_folder using a simple naming convention.
       - Update the 'downloaded' column to 1 in the DB.
       - Sleep for 3 seconds before processing the next paper.
    """
    
    # Ensure the download folder exists
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    # Update these connection parameters to match your own database
    config = {
        'user': 'root',
        'password': '',
        'host': 'localhost',
        'database': 'research_genai',   # adjust to your DB name
    }

    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor(dictionary=True)

        # 1. Fetch all papers where to_download=1 AND downloaded=0
        sql_select = """
            SELECT paper_id, pdf_url
            FROM paper
            WHERE to_download=1 AND downloaded=0
        """
        cursor.execute(sql_select)
        papers_to_download = cursor.fetchall()

        if not papers_to_download:
            print("No papers to download.")
            return

        print(f"Found {len(papers_to_download)} papers to download...")

        for paper in papers_to_download:
            paper_id = paper["paper_id"]
            pdf_url = paper["pdf_url"]

            # Create a local filename for saving
            # e.g. paper_1234.pdf
            local_filename = f"paper_{paper_id}.pdf"
            local_filepath = os.path.join(download_folder, local_filename)

            # Download the file using requests
            try:
                print(f"Downloading paper_id={paper_id} from {pdf_url}")
                
                # Stream download to avoid loading the entire file in memory at once
                response = requests.get(pdf_url, stream=True)
                response.raise_for_status()  # Raise if we encounter HTTP errors

                with open(local_filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                # Mark the paper as downloaded in the database
                sql_update = """
                    UPDATE paper
                    SET downloaded=1
                    WHERE paper_id=%s
                """
                cursor.execute(sql_update, (paper_id,))
                connection.commit()

                print(f"Downloaded and marked paper_id={paper_id} as downloaded. Saved to: {local_filepath}")

            except Exception as e:
                print(f"Failed to download paper_id={paper_id} from {pdf_url}. Error: {e}")
                time.sleep(60)

            # Sleep for 3 seconds before next download
            time.sleep(3)

        print("All eligible papers processed.")

    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

def main():
    # Specify the folder where PDFs should be saved
    download_folder = "/Volumes/AnisBoubake/ArxivPapers"
    download_papers(download_folder)

if __name__ == "__main__":
    main()
