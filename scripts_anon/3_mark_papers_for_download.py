import mysql.connector
from mysql.connector import Error

def main():
    """
    This script will:
      1. Find all first authors with count_first_before_gpt > 0 and count_first_after_gpt > 0.
      2. For those authors, mark all papers published after 2022-11-30 (i.e. >= 2022-12-01) as to_download = 1.
      3. For each author, count how many papers were just marked. Mark the same number of the most recent
         papers published before 2022-11-11 (i.e. <= 2022-11-10) as to_download = 1.
    """

    # Update these connection parameters to match your own database
    config = {
        'user': 'root',
        'password': '',
        'host': 'localhost',
        'database': 'research_genai',
    }

    try:
        # 1. Connect to the database
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor(dictionary=True)

        # 2. Identify all authors (in oa_authors) with count_first_before_gpt > 0 AND count_first_after_gpt > 0
        sql_authors = """
            SELECT id
            FROM oa_authors
            WHERE count_first_before_gpt > 0
              AND count_first_after_gpt > 0
        """
        cursor.execute(sql_authors)
        authors = cursor.fetchall()

        # For each qualifying author, do the marking
        for author_record in authors:
            author_id = author_record["id"]

            # --- STEP A: Mark all papers published after 2022-11-30 for this author (as first author)
            #             as to_download = 1
            sql_update_after = """
                UPDATE paper p
                JOIN paper_oa_author pa ON p.paper_id = pa.paper_id
                SET p.to_download = 1
                WHERE pa.author_id = %s
                  AND pa.author_position = 'first'
                  AND p.date_published > '2022-11-30'
            """
            cursor.execute(sql_update_after, (author_id, ))
            connection.commit()

            # --- STEP B: Count how many papers we just marked for this author
            sql_count_after = """
                SELECT COUNT(*) AS cnt
                FROM paper p
                JOIN paper_oa_author pa ON p.paper_id = pa.paper_id
                WHERE pa.author_id = %s
                  AND pa.author_position = 'first'
                  AND p.date_published > '2022-11-30'
                  AND p.to_download = 1
            """
            cursor.execute(sql_count_after, (author_id,))
            count_after = cursor.fetchone()["cnt"]
            
            if count_after > 0:
                # --- STEP C: We want to mark the same number (count_after) of the MOST RECENT
                #     papers published before 2022-11-11 as to_download=1.
                #     1) First retrieve the list of paper_ids from the newest to oldest (before 2022-11-11),
                #        limited by count_after
                sql_select_before = """
                    SELECT p.paper_id
                    FROM paper p
                    JOIN paper_oa_author pa ON p.paper_id = pa.paper_id
                    WHERE pa.author_id = %s
                      AND pa.author_position = 'first'
                      AND p.date_published < '2022-11-11'
                    ORDER BY p.date_published DESC
                    LIMIT %s
                """
                cursor.execute(sql_select_before, (author_id, count_after))
                papers_before = cursor.fetchall()

                if papers_before:
                    paper_ids_before = [str(rec["paper_id"]) for rec in papers_before]
                    
                    # 2) Mark those selected paper_ids as to_download = 1
                    sql_update_before = f"""
                        UPDATE paper
                        SET to_download = 1
                        WHERE paper_id IN ({",".join(paper_ids_before)})
                    """
                    cursor.execute(sql_update_before)
                    connection.commit()

        print("Done setting to_download flags according to the specified rules.")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    main()
