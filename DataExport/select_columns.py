#!/usr/bin/env python3
import sys
import pandas as pd

def main(input_file, output_file):
    # List of columns to keep
    columns_to_keep = [
        "paper_id",
        "author_id",
        "afterGPT",
        "word_count",
        "sentence_count",
        "avg_sentence_length",
        "flesch_reading_ease",
        "gunning_fog",
        "lemma_ttr",
        "lemma_mattr",
        "lexical_density_tokens",
        "lexical_density_types",
        "content_ttr",
        "function_ttr",
        "function_mattr",
        "noun_ttr",
        "verb_ttr",
        "adj_ttr",
        "prp_ttr",
        "argument_ttr",
        "bigram_lemma_ttr",
        "trigram_lemma_ttr",
        "adjacent_overlap_all_sent",
        "adjacent_overlap_binary_all_sent",
        "adjacent_overlap_2_all_sent",
        "adjacent_overlap_binary_2_all_sent",
        "adjacent_overlap_cw_sent",
        "adjacent_overlap_cw_sent_div_seg",
        "adjacent_overlap_2_cw_sent",
        "adjacent_overlap_2_cw_sent_div_seg",
        "adjacent_overlap_fw_sent",
        "adjacent_overlap_fw_sent_div_seg",
        "adjacent_overlap_binary_fw_sent",
        "adjacent_overlap_2_fw_sent",
        "adjacent_overlap_2_fw_sent_div_seg",
        "adjacent_overlap_binary_2_fw_sent",
        "adjacent_overlap_noun_sent",
        "adjacent_overlap_noun_sent_div_seg",
        "adjacent_overlap_binary_noun_sent",
        "adjacent_overlap_2_noun_sent",
        "adjacent_overlap_2_noun_sent_div_seg",
        "adjacent_overlap_adv_sent_div_seg",
        "adjacent_overlap_binary_adv_sent",
        "adjacent_overlap_2_adv_sent_div_seg",
        "adjacent_overlap_binary_2_adv_sent",
        "adjacent_overlap_pronoun_sent_div_seg",
        "adjacent_overlap_binary_pronoun_sent",
        "adjacent_overlap_2_pronoun_sent_div_seg",
        "adjacent_overlap_binary_2_pronoun_sent",
        "adjacent_overlap_argument_sent",
        "adjacent_overlap_argument_sent_div_seg",
        "adjacent_overlap_2_argument_sent",
        "adjacent_overlap_2_argument_sent_div_seg",
        "syn_overlap_sent_noun",
        "syn_overlap_sent_verb",
        "lsa_1_all_sent",
        "lsa_2_all_sent",
        "lda_1_all_sent",
        "lda_2_all_sent",
        "basic_connectives",
        "conjunctions",
        "lexical_subordinators",
        "coordinating_conjuncts",
        "addition",
        "sentence_linking",
        "the_order",
        "reason_and_purpose",
        "all_causal",
        "positive_causal",
        "opposition",
        "determiners",
        "all_demonstratives",
        "unattended_demonstratives",
        "all_additive",
        "all_logical",
        "positive_logical",
        "negative_logical",
        "all_temporal",
        "positive_intentional",
        "all_positive",
        "all_negative",
        "all_connective",
        "pronoun_density",
        "pronoun_noun_ratio",
        "repeated_content_lemmas",
        "repeated_content_and_pronoun_lemmas"
    ]

    # Read the CSV file
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error reading {input_file}: {e}")
        sys.exit(1)

    # Optional: warn if some columns are missing
    missing_columns = [col for col in columns_to_keep if col not in df.columns]
    if missing_columns:
        print("Warning: The following columns are missing in the input CSV and will be skipped:")
        for col in missing_columns:
            print(f"  - {col}")

    # Select only the columns that exist in the DataFrame
    existing_columns = [col for col in columns_to_keep if col in df.columns]
    df_filtered = df[existing_columns]

    # Write the filtered DataFrame to a new CSV file
    try:
        df_filtered.to_csv(output_file, index=False)
        print(f"Filtered CSV saved to {output_file}")
    except Exception as e:
        print(f"Error writing {output_file}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python select_columns.py input_file.csv output_file.csv")
        sys.exit(1)
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    main(input_file, output_file)
