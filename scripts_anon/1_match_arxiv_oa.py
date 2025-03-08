import mysql.connector
import psycopg2
import json

# MySQL connection details
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'research_genai'
}

# PostgreSQL connection details
postgres_config = {
    'host': 'localhost',
    'port': 5432,
    'user': 'anon',
    'password': 'anon',
    'database': 'anon',
    'sslmode': 'disable'  # Disable SSL
}

def update_oa_work_id():
    try:
        # Connect to MySQL
        mysql_conn = mysql.connector.connect(**mysql_config)
        mysql_cursor = mysql_conn.cursor(dictionary=True)

        # Connect to PostgreSQL
        postgres_conn = psycopg2.connect(
            host=postgres_config['host'],
            port=postgres_config['port'],
            user=postgres_config['user'],
            password=postgres_config['password'],
            database=postgres_config['database']
        )
        postgres_cursor = postgres_conn.cursor()

        # Fetch DOIs from MySQL paper table
        mysql_cursor.execute("SELECT paper_id, doi FROM paper WHERE doi IS NOT NULL AND oa_work_id IS NULL")
        papers = mysql_cursor.fetchall()

        # Iterate through MySQL DOIs and find matches in PostgreSQL works table
        for paper in papers:
            paper_id = paper['paper_id']
            paper_doi = paper['doi']

            # Prepare the DOI to match the format in the works table
            formatted_doi = f"https://doi.org/{paper_doi}"

            # Query PostgreSQL to find matching DOI
            postgres_cursor.execute(
                "SELECT id FROM openalex.works WHERE doi = %s", (formatted_doi,)
            )
            result = postgres_cursor.fetchone()

            if result:
                openalex_id = result[0]

                # Update the oa_work_id column in MySQL
                mysql_cursor.execute(
                    """
                    UPDATE paper
                    SET oa_work_id = %s
                    WHERE paper_id = %s
                    """,
                    (openalex_id, paper_id)
                )
                print(f"Updated paper_id {paper_id} with oa_work_id {openalex_id}")

        # Commit changes to MySQL
        mysql_conn.commit()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close connections
        if mysql_cursor:
            mysql_cursor.close()
        if mysql_conn:
            mysql_conn.close()
        if postgres_cursor:
            postgres_cursor.close()
        if postgres_conn:
            postgres_conn.close()

def populate_paper_oa_author():
    try:
        # Connect to MySQL
        mysql_conn = mysql.connector.connect(**mysql_config)
        mysql_cursor = mysql_conn.cursor(dictionary=True)

        # Connect to PostgreSQL
        postgres_conn = psycopg2.connect(
            host=postgres_config['host'],
            port=postgres_config['port'],
            user=postgres_config['user'],
            password=postgres_config['password'],
            database=postgres_config['database']
        )
        postgres_cursor = postgres_conn.cursor()

        # Fetch matching work_ids from MySQL paper table
        mysql_cursor.execute("SELECT paper_id, oa_work_id FROM paper WHERE oa_work_id IS NOT NULL")
        papers = mysql_cursor.fetchall()

        count = 0
        for paper in papers:
            paper_id = paper['paper_id']
            oa_work_id = paper['oa_work_id']

            # Fetch authorship data from PostgreSQL
            postgres_cursor.execute(
                """
                SELECT work_id, author_position, author_id
                FROM openalex.works_authorships
                WHERE work_id = %s
                """,
                (oa_work_id,)
            )
            authorships = postgres_cursor.fetchall()

            # Insert data into MySQL paper_oa_author table
            for authorship in authorships:
                work_id, author_position, author_id = authorship
                mysql_cursor.execute(
                    """
                    INSERT IGNORE INTO paper_oa_author (paper_id, work_id, author_position, author_id)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (paper_id, work_id, author_position, author_id)
                )
                print(f"Inserted authorship for paper_id {paper_id}, work_id {work_id}, author_id {author_id}")

                count += 1
                if count == 100:
                    mysql_conn.commit()
                    count = 0

        # Commit changes to MySQL
        mysql_conn.commit()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close connections
        if mysql_cursor:
            mysql_cursor.close()
        if mysql_conn:
            mysql_conn.close()
        if postgres_cursor:
            postgres_cursor.close()
        if postgres_conn:
            postgres_conn.close()

def populate_oa_authors():
    try:
        # Connect to MySQL
        mysql_conn = mysql.connector.connect(**mysql_config)
        mysql_cursor = mysql_conn.cursor(dictionary=True)

        # Connect to PostgreSQL
        postgres_conn = psycopg2.connect(
            host=postgres_config['host'],
            port=postgres_config['port'],
            user=postgres_config['user'],
            password=postgres_config['password'],
            database=postgres_config['database']
        )
        postgres_cursor = postgres_conn.cursor()

        # Fetch unique author_ids from MySQL paper_oa_author table
        mysql_cursor.execute("SELECT DISTINCT author_id FROM paper_oa_author")
        author_ids = mysql_cursor.fetchall()

        count = 0
        for row in author_ids:
            author_id = row['author_id']

            # Fetch author data from PostgreSQL authors table
            postgres_cursor.execute(
                """
                SELECT id, orcid, display_name, display_name_alternatives, last_known_institution
                FROM openalex.authors
                WHERE id = %s
                """,
                (author_id,)
            )
            author_data = postgres_cursor.fetchone()

            if author_data:
                id, orcid, display_name, display_name_alternatives, last_known_institution = author_data

                # Convert display_name_alternatives to a JSON string if it's a list
                if isinstance(display_name_alternatives, list):
                    display_name_alternatives = json.dumps(display_name_alternatives)

                # Insert author data into MySQL oa_authors table
                mysql_cursor.execute(
                    """
                    INSERT IGNORE INTO oa_authors (id, orcid, display_name, display_name_alternative, last_known_institution)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (id, orcid, display_name, display_name_alternatives, last_known_institution)
                )
                print(f"Inserted author_id {author_id} into oa_authors")
                count += 1
                if count == 100:
                    mysql_conn.commit()
                    count = 0

        # Commit changes to MySQL
        mysql_conn.commit()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close connections
        if mysql_cursor:
            mysql_cursor.close()
        if mysql_conn:
            mysql_conn.close()
        if postgres_cursor:
            postgres_cursor.close()
        if postgres_conn:
            postgres_conn.close()

if __name__ == "__main__":
    #update_oa_work_id()
    #populate_paper_oa_author()
    populate_oa_authors()
