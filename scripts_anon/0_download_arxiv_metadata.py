import mysql.connector
import requests
import datetime
import time

def clear_database(connection):
    """Clear all data from the database tables."""
    cursor = connection.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    cursor.execute("TRUNCATE TABLE research_genai.paper_category;")
    cursor.execute("TRUNCATE TABLE research_genai.paper_author;")
    cursor.execute("TRUNCATE TABLE research_genai.paper;")
    cursor.execute("TRUNCATE TABLE research_genai.author;")
    cursor.execute("TRUNCATE TABLE research_genai.category;")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    connection.commit()
    cursor.close()

def fetch_arxiv_data(start_date, end_date, start_index=0, max_results=100):
    """Fetch metadata from Arxiv API."""
    url = "http://export.arxiv.org/api/query"
    params = {
        "search_query": f"submittedDate:[{start_date.strftime('%Y%m%d')} TO {end_date.strftime('%Y%m%d')}]",
        "start": start_index,
        "max_results": max_results
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    #print(f"Fetching data from: {response.url}")
    return response.text

def parse_arxiv_metadata(xml_data):
    """Parse XML metadata from Arxiv."""
    import xml.etree.ElementTree as ET
    updated_xml_data = xml_data.replace(
        '<feed xmlns="http://www.w3.org/2005/Atom">',
        '<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">'
    )
    root = ET.fromstring(updated_xml_data)
    namespace = {'atom': 'http://www.w3.org/2005/Atom', 'arxiv':'http://arxiv.org/schemas/atom'}
    #namespace = {'': 'http://www.w3.org/2005/Atom', 'arxiv': 'http://arxiv.org/schemas/atom'}
    #namespace=None

    papers = []
    for entry in root.findall("atom:entry", namespace):
        skip_paper = False
        title_element = entry.find("atom:title", namespace)
        if title_element is None:
            continue
        
        authors = []
        for author_elem in entry.findall("atom:author", namespace):
            author_name = author_elem.find("atom:name", namespace).text
            if len(author_name)>254:
                skip_paper = True
                break
            affiliation_elem = author_elem.find("atom:affiliation", namespace)
            affiliation = affiliation_elem.text if affiliation_elem is not None else None
            authors.append({"name": author_name, "affiliation": affiliation})

        if skip_paper:
            continue

        primary_category_elem = entry.find("arxiv:primary_category", namespace)
        primary_category = primary_category_elem.attrib.get('term') if primary_category_elem is not None else ""

        doi_elem = entry.find("arxiv:doi", namespace)
        doi =  doi_elem.text if doi_elem is not None else ""

        jref_elem = entry.find("arxiv:journal_ref", namespace)
        jref =  jref_elem.text if jref_elem is not None else ""

        paper = {
            "title": entry.find("atom:title", namespace).text,
            "abstract": entry.find("atom:summary", namespace).text,
            "published": entry.find("atom:published", namespace).text[:-1],
            "updated": entry.find("atom:updated", namespace).text[:-1],
            "authors": authors,
            "pdf_url": entry.find("atom:link[@type='application/pdf']", namespace).attrib.get('href'),
            "categories": set([cat.attrib.get('term').lower() for cat in entry.findall("atom:category", namespace)]),
            "primary_category": primary_category,
            "arxiv_id": entry.find("atom:id", namespace).text,
            "doi": doi,
            "journal_ref": jref
        }
        
        papers.append(paper)
    return papers

def get_category(connection, category):
    """Retrieves the ID of the category from MySQL if it exists, otherwise it creates it. Returns the id (old or new)"""
    #category = category if len(category)<20 else ""
    if len(category)>20: 
        return None

    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT category_id FROM research_genai.category WHERE name=%s
        """,
        (category,)
    )
    result = cursor.fetchone()
    
    if result:
        row_id = result[0]
    else:
        cursor.execute(
            """
            INSERT INTO research_genai.category (name) VALUES (%s)
            """,
            (category,)
        )
        row_id = cursor.lastrowid
    return row_id
    

def populate_database(connection, papers):
    """Insert fetched data into the database."""
    cursor = connection.cursor()
    for paper in papers:
        # Get the category IDs
        paper["primary_category_id"] = get_category(connection, paper["primary_category"])
        paper["categories_id"] = [get_category(connection, cat) for cat in paper["categories"]]
        #print(f"Paper: {paper["title"]}")
        cursor.execute(
            """
            INSERT INTO research_genai.paper (title, pdf_url, last_modification, abstract, date_published, date_updated, arxiv_id, doi, journal_ref,primary_category_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (paper["title"], paper["pdf_url"], paper["updated"], paper["abstract"], paper["published"], paper["updated"], paper["arxiv_id"], paper["doi"], paper["journal_ref"], paper["primary_category_id"])
        )
        paper_id = cursor.lastrowid

        for category_id in paper["categories_id"]: 
            if category_id is not None:
                #print(f"PAPER-CAT: Adding paper {paper["title"]} to {category_id}")
                cursor.execute(
                        """
                        INSERT INTO research_genai.paper_category (paper_id, category_id)
                        VALUES (%s, %s)
                        """,
                        (paper_id, category_id)
                    )

        for order, author in enumerate(paper["authors"], start=1):
            cursor.execute(
                """INSERT INTO research_genai.author (author_name, affiliation_1) VALUES (%s, %s)""",
                (author["name"], author["affiliation"])
            )
            author_id = cursor.lastrowid

            cursor.execute(
                """
                INSERT INTO research_genai.paper_author (paper_id, author_id, `order`) VALUES (%s, %s, %s)
                """,
                (paper_id, author_id, order)
            )
    connection.commit()
    cursor.close()

def main():
    import xml.etree.ElementTree as ET
    db_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="research_genai"
    )
    #clear_database(db_connection)

    # Post: 2023-06-01 to 2024-06-01
    start_date = datetime.date(2023, 6, 1)
    end_date = datetime.date(2023, 12, 22)

    current_date = end_date
    batch_size = 1000

    while current_date >= start_date:
        next_date = current_date - datetime.timedelta(days=1)
        print(f"Fetching results for {next_date} to {current_date}", end="  ")

        start_index = 0
        while True:
            # Fetch data for the current day
            xml_data = fetch_arxiv_data(
                next_date,
                current_date,
                start_index=start_index,
                max_results=batch_size
            )
            
            # Parse the XML data
            root = ET.fromstring(xml_data)
            papers = parse_arxiv_metadata(xml_data)
            
            if not papers:  # Stop if no more papers are found
                print("Nothing to fetch")
                break

            # Populate the database
            db_connection = mysql.connector.connect(
                host="localhost",
                user="root",
                password="",
                database="research_genai"
            )
            populate_database(db_connection, papers)
            db_connection.close()

            print(f"Fetched {len(papers)} papers for index starting at {start_index}")
            start_index += batch_size

            # Respect API rate limits
            time.sleep(1)

        # Move to the next day
        current_date = next_date

    print("All results fetched and stored.")
    db_connection.close()

    

if __name__ == "__main__":
    main()
