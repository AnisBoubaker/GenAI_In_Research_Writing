import mysql.connector
import psycopg2
from collections import Counter, defaultdict

# Configuration
BATCH_SIZE = 1000  # Adjust this based on performance

# MySQL connection
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="research_genai"
)
mysql_cursor = mysql_conn.cursor()

# PostgreSQL connection
pg_conn = psycopg2.connect(
    host="anon",
    user="anon",
    password="anon",
    database="anon"
)
pg_cursor = pg_conn.cursor()

# Create a temporary table to store results
mysql_cursor.execute("""
    CREATE TEMPORARY TABLE temp_paper_fields (
        paper_id INT PRIMARY KEY,
        field1 VARCHAR(100), domain1 VARCHAR(100),
        field2 VARCHAR(100), domain2 VARCHAR(100),
        field3 VARCHAR(100), domain3 VARCHAR(100)
    )
""")

# Retrieve all papers with metrics_done = 1
mysql_cursor.execute("""
    SELECT paper_id, oa_work_id 
    FROM paper 
    WHERE metrics_done = 1 AND oa_work_id IS NOT NULL
""")
papers = mysql_cursor.fetchall()
paper_dict = {p[1]: p[0] for p in papers}  # Map oa_work_id to paper_id

# Process in batches
paper_keys = list(paper_dict.keys())
num_papers = len(paper_keys)
for i in range(0, num_papers, BATCH_SIZE):
    batch_keys = paper_keys[i:i + BATCH_SIZE]
    
    # Fetch topics for the batch
    pg_cursor.execute("""
        SELECT wt.work_id, t.field_display_name, t.domain_display_name
        FROM openalex.works_topics wt
        JOIN openalex.topics t ON wt.topic_id = t.id
        WHERE wt.work_id = ANY(%s)
    """, (batch_keys,))
    
    topics_data = defaultdict(list)
    for work_id, field, domain in pg_cursor.fetchall():
        if field and domain:
            topics_data[work_id].append((field, domain))
    
    # Prepare batch insert data
    insert_data = []
    for oa_work_id, topic_list in topics_data.items():
        paper_id = paper_dict.get(oa_work_id)
        pair_counter = Counter(topic_list)
        top_pairs = [pair for pair, _ in pair_counter.most_common(3)]
        while len(top_pairs) < 3:
            top_pairs.append((None, None))
        insert_data.append((paper_id, *top_pairs[0], *top_pairs[1], *top_pairs[2]))
    
    # Insert batch into temporary table
    if insert_data:
        mysql_cursor.executemany("""
            INSERT INTO temp_paper_fields (paper_id, field1, domain1, field2, domain2, field3, domain3)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, insert_data)
        mysql_conn.commit()
        print("One batch done.")

# Bulk update paper table from temp table
mysql_cursor.execute("""
    UPDATE paper p
    JOIN temp_paper_fields tpf ON p.paper_id = tpf.paper_id
    SET p.field1 = tpf.field1, p.domain1 = tpf.domain1,
        p.field2 = tpf.field2, p.domain2 = tpf.domain2,
        p.field3 = tpf.field3, p.domain3 = tpf.domain3
""")
mysql_conn.commit()

# Cleanup
mysql_cursor.execute("DROP TEMPORARY TABLE temp_paper_fields")
mysql_cursor.close()
mysql_conn.close()
pg_cursor.close()
pg_conn.close()

print("Bulk update complete.")
