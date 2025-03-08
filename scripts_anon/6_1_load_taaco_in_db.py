import csv
import pymysql
import re

# Database connection settings
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWORD = ""
DB_NAME = "research_genai"

# CSV file path
CSV_FILE_PATH = "taaco_metrics.csv"

# SQL INSERT query template
INSERT_QUERY = """
INSERT INTO paper_metrics_taaco (
    paper_id, lemma_ttr, lemma_mattr, lexical_density_tokens, lexical_density_types, content_ttr,
    function_ttr, function_mattr, noun_ttr, verb_ttr, adj_ttr, adv_ttr, prp_ttr, argument_ttr,
    bigram_lemma_ttr, trigram_lemma_ttr, adjacent_overlap_all_sent, adjacent_overlap_all_sent_div_seg,
    adjacent_overlap_binary_all_sent, adjacent_overlap_2_all_sent, adjacent_overlap_2_all_sent_div_seg,
    adjacent_overlap_binary_2_all_sent, adjacent_overlap_cw_sent, adjacent_overlap_cw_sent_div_seg,
    adjacent_overlap_binary_cw_sent, adjacent_overlap_2_cw_sent, adjacent_overlap_2_cw_sent_div_seg,
    adjacent_overlap_binary_2_cw_sent, adjacent_overlap_fw_sent, adjacent_overlap_fw_sent_div_seg,
    adjacent_overlap_binary_fw_sent, adjacent_overlap_2_fw_sent, adjacent_overlap_2_fw_sent_div_seg,
    adjacent_overlap_binary_2_fw_sent, adjacent_overlap_noun_sent, adjacent_overlap_noun_sent_div_seg,
    adjacent_overlap_binary_noun_sent, adjacent_overlap_2_noun_sent, adjacent_overlap_2_noun_sent_div_seg,
    adjacent_overlap_binary_2_noun_sent, adjacent_overlap_verb_sent, adjacent_overlap_verb_sent_div_seg,
    adjacent_overlap_binary_verb_sent, adjacent_overlap_2_verb_sent, adjacent_overlap_2_verb_sent_div_seg,
    adjacent_overlap_binary_2_verb_sent, adjacent_overlap_adj_sent, adjacent_overlap_adj_sent_div_seg,
    adjacent_overlap_binary_adj_sent, adjacent_overlap_2_adj_sent, adjacent_overlap_2_adj_sent_div_seg,
    adjacent_overlap_binary_2_adj_sent, adjacent_overlap_adv_sent, adjacent_overlap_adv_sent_div_seg,
    adjacent_overlap_binary_adv_sent, adjacent_overlap_2_adv_sent, adjacent_overlap_2_adv_sent_div_seg,
    adjacent_overlap_binary_2_adv_sent, adjacent_overlap_pronoun_sent, adjacent_overlap_pronoun_sent_div_seg,
    adjacent_overlap_binary_pronoun_sent, adjacent_overlap_2_pronoun_sent, adjacent_overlap_2_pronoun_sent_div_seg,
    adjacent_overlap_binary_2_pronoun_sent, adjacent_overlap_argument_sent, adjacent_overlap_argument_sent_div_seg,
    adjacent_overlap_binary_argument_sent, adjacent_overlap_2_argument_sent, adjacent_overlap_2_argument_sent_div_seg,
    adjacent_overlap_binary_2_argument_sent, adjacent_overlap_all_para, adjacent_overlap_all_para_div_seg,
    adjacent_overlap_binary_all_para, adjacent_overlap_2_all_para, adjacent_overlap_2_all_para_div_seg,
    adjacent_overlap_binary_2_all_para, adjacent_overlap_cw_para, adjacent_overlap_cw_para_div_seg,
    adjacent_overlap_binary_cw_para, adjacent_overlap_2_cw_para, adjacent_overlap_2_cw_para_div_seg,
    adjacent_overlap_binary_2_cw_para, adjacent_overlap_fw_para, adjacent_overlap_fw_para_div_seg,
    adjacent_overlap_binary_fw_para, adjacent_overlap_2_fw_para, adjacent_overlap_2_fw_para_div_seg,
    adjacent_overlap_binary_2_fw_para, adjacent_overlap_noun_para, adjacent_overlap_noun_para_div_seg,
    adjacent_overlap_binary_noun_para, adjacent_overlap_2_noun_para, adjacent_overlap_2_noun_para_div_seg,
    adjacent_overlap_binary_2_noun_para, adjacent_overlap_verb_para, adjacent_overlap_verb_para_div_seg,
    adjacent_overlap_binary_verb_para, adjacent_overlap_2_verb_para, adjacent_overlap_2_verb_para_div_seg,
    adjacent_overlap_binary_2_verb_para, adjacent_overlap_adj_para, adjacent_overlap_adj_para_div_seg,
    adjacent_overlap_binary_adj_para, adjacent_overlap_2_adj_para, adjacent_overlap_2_adj_para_div_seg,
    adjacent_overlap_binary_2_adj_para, adjacent_overlap_adv_para, adjacent_overlap_adv_para_div_seg,
    adjacent_overlap_binary_adv_para, adjacent_overlap_2_adv_para, adjacent_overlap_2_adv_para_div_seg,
    adjacent_overlap_binary_2_adv_para, adjacent_overlap_pronoun_para, adjacent_overlap_pronoun_para_div_seg,
    adjacent_overlap_binary_pronoun_para, adjacent_overlap_2_pronoun_para, adjacent_overlap_2_pronoun_para_div_seg,
    adjacent_overlap_binary_2_pronoun_para, adjacent_overlap_argument_para, adjacent_overlap_argument_para_div_seg,
    adjacent_overlap_binary_argument_para, adjacent_overlap_2_argument_para, adjacent_overlap_2_argument_para_div_seg,
    adjacent_overlap_binary_2_argument_para, syn_overlap_sent_noun, syn_overlap_sent_verb, syn_overlap_para_noun,
    syn_overlap_para_verb, lsa_1_all_sent, lsa_2_all_sent, lsa_1_all_para, lsa_2_all_para, lda_1_all_sent,
    lda_2_all_sent, lda_1_all_para, lda_2_all_para, word2vec_1_all_sent, word2vec_2_all_sent, word2vec_1_all_para,
    word2vec_2_all_para, basic_connectives, conjunctions, disjunctions, lexical_subordinators, coordinating_conjuncts,
    addition, sentence_linking, the_order, reason_and_purpose, all_causal, positive_causal, opposition, determiners,
    all_demonstratives, attended_demonstratives, unattended_demonstratives, all_additive, all_logical, positive_logical,
    negative_logical, all_temporal, positive_intentional, all_positive, all_negative, all_connective, pronoun_density,
    pronoun_noun_ratio, repeated_content_lemmas, repeated_content_and_pronoun_lemmas
) VALUES ({placeholders});
"""

def extract_paper_id(filename):
    """Extract paper ID from the filename (e.g., paper_123.txt -> 123)."""
    match = re.match(r"paper_(\d+)\.txt", filename)
    if match:
        return int(match.group(1))
    return None

def load_csv_to_mysql():
    try:
        # Establish MySQL connection
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT
        )
        cursor = connection.cursor()

        # Open CSV file and insert data
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                paper_id = extract_paper_id(row['Filename'])  # Extract paper_id from Filename
                if paper_id is None:
                    print(f"Skipping row with invalid filename: {row['Filename']}")
                    continue
                values = (paper_id,) + tuple(row[key] if row[key] else None for key in reader.fieldnames[1:])
                placeholders = ",".join(["%s"] * len(values))
                cursor.execute(INSERT_QUERY.format(placeholders=placeholders), values)

        # Commit changes
        connection.commit()
        print("Data successfully inserted into MySQL table.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    load_csv_to_mysql()
