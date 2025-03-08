#!/usr/bin/env python3
import mysql.connector
import psycopg2

# MySQL connection details
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'research_genai'
}

# PostgreSQL connection details (using your working configuration)
postgres_config = {
    'host': 'anon',
    'port': 5432,
    'user': 'anon',
    'password': 'anon',
    'database': 'anon',       # Note: the database name is 'anis', not 'openalex'
    'sslmode': 'disable'      # Disable SSL to avoid GSSAPI issues
}

def update_author_country_code():
    try:
        # Connect to MySQL
        mysql_conn = mysql.connector.connect(**mysql_config)
        mysql_cursor = mysql_conn.cursor()
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return

    try:
        # Connect to PostgreSQL using the working connection parameters
        postgres_conn = psycopg2.connect(
            host=postgres_config['host'],
            port=postgres_config['port'],
            user=postgres_config['user'],
            password=postgres_config['password'],
            database=postgres_config['database'],
            sslmode=postgres_config['sslmode']
        )
        postgres_cursor = postgres_conn.cursor()
    except psycopg2.Error as err:
        print(f"Error connecting to PostgreSQL: {err}")
        mysql_conn.close()
        return

    try:
        # Fetch all author ids from the MySQL table
        mysql_cursor.execute("SELECT id FROM oa_authors")
        authors = mysql_cursor.fetchall()
    except mysql.connector.Error as err:
        print(f"Error querying MySQL: {err}")
        mysql_conn.close()
        postgres_conn.close()
        return

    for (author_id,) in authors:
        # Query PostgreSQL for the author's country_code.
        # We use the 'openalex' schema because your tables (authors, institutions) are there.
        # The join assumes that the author's 'last_known_institution' matches the institution's 'id'.
        pg_query = """
            SELECT i.country_code
            FROM openalex.authors AS a
            LEFT JOIN openalex.institutions AS i
              ON a.last_known_institution = i.id
            WHERE a.id = %s;
        """
        try:
            postgres_cursor.execute(pg_query, (author_id,))
            result = postgres_cursor.fetchone()
        except psycopg2.Error as err:
            print(f"Error querying PostgreSQL for author {author_id}: {err}")
            continue

        if result is not None:
            (country_code,) = result
            try:
                update_query = "UPDATE oa_authors SET country_code = %s WHERE id = %s"
                mysql_cursor.execute(update_query, (country_code, author_id))
                print(f"Updated author {author_id} with country_code: {country_code}")
            except mysql.connector.Error as err:
                print(f"Error updating MySQL for author {author_id}: {err}")
                continue
        else:
            print(f"No PostgreSQL record found for author {author_id}")

    # Commit changes to MySQL
    try:
        mysql_conn.commit()
    except mysql.connector.Error as err:
        print(f"Error committing MySQL transaction: {err}")

    # Close all connections
    postgres_cursor.close()
    postgres_conn.close()
    mysql_cursor.close()
    mysql_conn.close()

if __name__ == '__main__':
    update_author_country_code()
