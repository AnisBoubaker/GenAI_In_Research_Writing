##################
# Installation: 
# pip install PyPDF2 textstat spacy
# python -m spacy download en_core_web_sm
##################
import os
import pymysql
import spacy
from PyPDF2 import PdfReader
from textstat import textstat

##########################
# Database Configuration
##########################
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWORD = ""
DB_NAME = "research_genai"

##########################
# Other Configuration
##########################
PDF_FOLDER = "/Volumes/AnisBoubake/ArxivPapers"
nlp = spacy.load("en_core_web_sm")  # spaCy model
# nlp.max_length = 3_000_000

def sanitize_text(text):
    """
    Removes invalid or non-UTF-8 characters from the text.
    """
    return text.encode('utf-8', 'ignore').decode('utf-8', 'ignore')

def load_text_from_pdf(file_path):
    """
    Extracts text from each page of a PDF file using PyPDF2.
    Returns a single string containing all text, or empty on error.
    """
    all_text = []
    try:
        with open(file_path, 'rb') as f:
            pdf = PdfReader(f)
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    sanitized_text = sanitize_text(page_text)
                    all_text.append(sanitized_text)
        return "\n".join(all_text)
    except Exception as e:
        print(f"Error reading PDF '{file_path}': {e}")
        return ""

def compute_stylometric_features(text):
    """
    Computes various stylometric and readability metrics.
    Returns a dictionary with the results.
    """
    doc = nlp(text)
    
    tokens = [token.text for token in doc if not token.is_space and not token.is_punct]
    word_count = len(tokens)
    
    sentences = list(doc.sents)
    sentence_count = len(sentences)
    avg_sentence_length = word_count / sentence_count if sentence_count > 0 else 0.0
    
    unique_tokens = set(tokens)
    type_token_ratio = (len(unique_tokens) / word_count) if word_count > 0 else 0.0
    
    flesch_reading_ease = textstat.flesch_reading_ease(text)
    flesch_kincaid_grade = textstat.flesch_kincaid_grade(text)
    gunning_fog = textstat.gunning_fog(text)
    
    return {
        "word_count": word_count,
        "sentence_count": sentence_count,
        "avg_sentence_length": avg_sentence_length,
        "type_token_ratio": type_token_ratio,
        "flesch_reading_ease": flesch_reading_ease,
        "flesch_kincaid_grade": flesch_kincaid_grade,
        "gunning_fog": gunning_fog
    }

def main():
    # 1. Connect to MySQL
    connection = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        with connection.cursor() as cursor:
            # 2. Select papers where downloaded=1
            sql_select = """
                SELECT paper_id 
                FROM paper
                WHERE downloaded = 1
                AND metrics_done = 0
                AND canceled_error = 0
            """
            cursor.execute(sql_select)
            rows = cursor.fetchall()
            
            if not rows:
                print("No papers found with downloaded=1.")
                return
            
            # 3. For each paper, build PDF path, compute metrics, and insert/update DB
            for row in rows:
                paper_id = row["paper_id"]
                pdf_path = os.path.join(PDF_FOLDER, f"paper_{paper_id}.pdf")

                print(f"Processing file {pdf_path}")
                
                if not os.path.exists(pdf_path):
                    print(f"PDF file not found for paper_id={paper_id}. Expected path: {pdf_path}")
                    continue
                
                # 4. Extract text from PDF
                text = load_text_from_pdf(pdf_path)
                if not text.strip():
                    print(f"No text extracted from {pdf_path}. Skipping metrics.")
                    continue
                
                

                # 5. Compute stylometric metrics
                metrics = compute_stylometric_features(text)
                
                # 6. Insert/Update the metrics in 'paper_metrics' table
                sql_insert = """
                INSERT INTO paper_metrics (
                    paper_id,
                    word_count,
                    sentence_count,
                    avg_sentence_length,
                    type_token_ratio,
                    flesch_reading_ease,
                    flesch_kincaid_grade,
                    gunning_fog
                )
                VALUES (
                    %(paper_id)s,
                    %(word_count)s,
                    %(sentence_count)s,
                    %(avg_sentence_length)s,
                    %(type_token_ratio)s,
                    %(flesch_reading_ease)s,
                    %(flesch_kincaid_grade)s,
                    %(gunning_fog)s
                )
                ON DUPLICATE KEY UPDATE
                    word_count = VALUES(word_count),
                    sentence_count = VALUES(sentence_count),
                    avg_sentence_length = VALUES(avg_sentence_length),
                    type_token_ratio = VALUES(type_token_ratio),
                    flesch_reading_ease = VALUES(flesch_reading_ease),
                    flesch_kincaid_grade = VALUES(flesch_kincaid_grade),
                    gunning_fog = VALUES(gunning_fog)
                """
                
                data = {
                    "paper_id": paper_id,
                    "word_count": metrics["word_count"],
                    "sentence_count": metrics["sentence_count"],
                    "avg_sentence_length": metrics["avg_sentence_length"],
                    "type_token_ratio": metrics["type_token_ratio"],
                    "flesch_reading_ease": metrics["flesch_reading_ease"],
                    "flesch_kincaid_grade": metrics["flesch_kincaid_grade"],
                    "gunning_fog": metrics["gunning_fog"]
                }
                
                cursor.execute(sql_insert, data)
                
                # 7. Mark metrics_done = 1 in paper table
                sql_update_metrics_done = """
                UPDATE paper
                SET metrics_done = 1
                WHERE paper_id = %s
                """
                cursor.execute(sql_update_metrics_done, (paper_id,))
                connection.commit()
                
                print(f"Metrics updated and metrics_done set for paper_id={paper_id}.")
    
    except Exception as ex:
        print(f"Error during database operations: {ex}")
    
    finally:
        connection.close()

if __name__ == "__main__":
    main()
