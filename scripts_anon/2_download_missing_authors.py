import mysql.connector
import requests
import json

# Database configuration
DB_CONFIG = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'research_genai',
}

# OpenAlex API base URL
API_BASE_URL = "https://api.openalex.org/authors/"

def fetch_missing_authors():
    """Fetch authors from paper_oa_author that do not exist in oa_authors."""
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    
    query = (
        "SELECT distinct author_id FROM paper_oa_author "
        "WHERE author_id NOT IN (SELECT id FROM oa_authors);"
    )

    cursor.execute(query)
    missing_authors = [row[0] for row in cursor.fetchall()]

    cursor.close()
    connection.close()
    return missing_authors

def fetch_author_from_api(author_id):
    """Fetch author details from OpenAlex API."""
    try:
        response = requests.get(f"{API_BASE_URL}{author_id}")
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data for author {author_id}: {e}")
        return None

def insert_author_into_db(author):
    """Insert a new author into the oa_authors table."""
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()

    insert_query = (
        "INSERT INTO oa_authors (id, orcid, display_name, display_name_alternative, last_known_institution) "
        "VALUES (%s, %s, %s, %s, %s);"
    )

    # Prepare data for insertion
    author_id = author.get("id")
    orcid = author.get("orcid")
    display_name = author.get("display_name")
    display_name_alternatives = json.dumps(author.get("display_name_alternatives"))
    last_known_institution = json.dumps(author.get("last_known_institutions"))

    try:
        cursor.execute(insert_query, (author_id, orcid, display_name, display_name_alternatives, last_known_institution))
        connection.commit()
        print(f"Inserted author {author_id} into oa_authors.")
    except mysql.connector.Error as e:
        print(f"Error inserting author {author_id}: {e}")
    finally:
        cursor.close()
        connection.close()

def main():
    missing_authors = fetch_missing_authors()
    print(f"Found {len(missing_authors)} missing authors.")

    for author_id in missing_authors:
        author_data = fetch_author_from_api(author_id)
        if author_data:
            insert_author_into_db(author_data)

if __name__ == "__main__":
    main()
