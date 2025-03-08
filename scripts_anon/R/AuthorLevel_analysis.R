# Load libraries
library(tidyverse)

# Load your data
data <- read.csv("/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/DataExport/paper_metrics.csv")

# Make sure author_id is a factor
data$author_id <- as.factor(data$author_id)

# List of metrics to be analysed
metrics_list <- c("word_count", "sentence_count", "avg_sentence_length", "type_token_ratio", "flesch_reading_ease",
                  "flesch_kincaid_grade", "gunning_fog", "lemma_ttr", "lemma_mattr", "lexical_density_tokens",
                  "lexical_density_types", "content_ttr", "function_ttr", "function_mattr", "noun_ttr",
                  "verb_ttr", "adj_ttr", "adv_ttr", "prp_ttr", "argument_ttr", "bigram_lemma_ttr", "trigram_lemma_ttr",
                  "adjacent_overlap_all_sent", "adjacent_overlap_all_sent_div_seg", "adjacent_overlap_binary_all_sent",
                  "adjacent_overlap_2_all_sent", "adjacent_overlap_2_all_sent_div_seg", "adjacent_overlap_binary_2_all_sent",
                  "adjacent_overlap_cw_sent", "adjacent_overlap_cw_sent_div_seg", "adjacent_overlap_binary_cw_sent",
                  "adjacent_overlap_2_cw_sent", "adjacent_overlap_2_cw_sent_div_seg", "adjacent_overlap_binary_2_cw_sent",
                  "adjacent_overlap_fw_sent", "adjacent_overlap_fw_sent_div_seg", "adjacent_overlap_binary_fw_sent",
                  "adjacent_overlap_2_fw_sent", "adjacent_overlap_2_fw_sent_div_seg", "adjacent_overlap_binary_2_fw_sent",
                  "adjacent_overlap_noun_sent", "adjacent_overlap_noun_sent_div_seg", "adjacent_overlap_binary_noun_sent",
                  "adjacent_overlap_2_noun_sent", "adjacent_overlap_2_noun_sent_div_seg", "adjacent_overlap_binary_2_noun_sent",
                  "adjacent_overlap_verb_sent", "adjacent_overlap_verb_sent_div_seg", "adjacent_overlap_binary_verb_sent",
                  "adjacent_overlap_2_verb_sent", "adjacent_overlap_2_verb_sent_div_seg", "adjacent_overlap_binary_2_verb_sent",
                  "adjacent_overlap_adj_sent", "adjacent_overlap_adj_sent_div_seg", "adjacent_overlap_binary_adj_sent",
                  "adjacent_overlap_2_adj_sent", "adjacent_overlap_2_adj_sent_div_seg", "adjacent_overlap_binary_2_adj_sent",
                  "adjacent_overlap_adv_sent", "adjacent_overlap_adv_sent_div_seg", "adjacent_overlap_binary_adv_sent",
                  "adjacent_overlap_2_adv_sent", "adjacent_overlap_2_adv_sent_div_seg", "adjacent_overlap_binary_2_adv_sent",
                  "adjacent_overlap_pronoun_sent", "adjacent_overlap_pronoun_sent_div_seg", "adjacent_overlap_binary_pronoun_sent",
                  "adjacent_overlap_2_pronoun_sent", "adjacent_overlap_2_pronoun_sent_div_seg", "adjacent_overlap_binary_2_pronoun_sent",
                  "adjacent_overlap_argument_sent", "adjacent_overlap_argument_sent_div_seg", "adjacent_overlap_binary_argument_sent",
                  "adjacent_overlap_2_argument_sent", "adjacent_overlap_2_argument_sent_div_seg", "adjacent_overlap_binary_2_argument_sent",
                  "syn_overlap_sent_noun", "syn_overlap_sent_verb", "lsa_1_all_sent", "lsa_2_all_sent",
                  "lda_1_all_sent", "lda_2_all_sent", "word2vec_1_all_sent", "word2vec_2_all_sent", "basic_connectives",
                  "conjunctions", "disjunctions", "lexical_subordinators", "coordinating_conjuncts", "addition",
                  "sentence_linking", "the_order", "reason_and_purpose", "all_causal", "positive_causal",
                  "opposition", "determiners", "all_demonstratives", "attended_demonstratives", "unattended_demonstratives",
                  "all_additive", "all_logical", "positive_logical", "negative_logical", "all_temporal",
                  "positive_intentional", "all_positive", "all_negative", "all_connective", "pronoun_density",
                  "pronoun_noun_ratio", "repeated_content_lemmas", "repeated_content_and_pronoun_lemmas")

results <- list()
for (metric in metrics_list) {
  # Calculate author-level averages before and after GPT
  author_data <- data %>%
    group_by(author_id, afterGPT) %>%
    # Convert to numeric before taking mean, also handle missing values.
    summarize(mean_value = mean(as.numeric(!!sym(metric)), na.rm = TRUE), .groups = 'drop') %>%
    pivot_wider(names_from = afterGPT, values_from = mean_value, names_prefix = "mean_")
  
  #Calculate author level difference, and remove NA values
  author_data <- author_data %>%
    mutate(diff = mean_0 - mean_1) %>%
    filter(!is.na(diff))
  
  # Use Wilcoxon test
  test_result <- tryCatch(wilcox.test(author_data$diff),
                          error = function(e) {
                            message(paste("Error running wilcoxon test for", metric, ": ", e$message))
                            return(NULL)
                          })
  
  if(!is.null(test_result)){
    # Calculate median of the differences
    median_diff <- median(author_data$diff, na.rm = TRUE)
    
    # Calculate quartiles
    quartiles <- quantile(author_data$diff, probs = c(0.25, 0.75), na.rm = TRUE)
    
    
    results[[metric]] <- data.frame(
      metric = metric,
      test = "Wilcoxon test",
      estimate = NA, #wilcoxon does not return mean difference
      p_value = test_result$p.value,
      median_diff = median_diff,
      Q1 = quartiles[1],
      Q3 = quartiles[2]
      
    )
  }
}

results_df <- bind_rows(results)

# Adjust p-values for multiple testing (optional, but recommended)
results_df$p_adjusted <- p.adjust(results_df$p_value, method = "BH")

# Add direction of effect
results_df <- results_df %>%
  mutate(effect_direction = case_when(
    median_diff > 0 ~ "Increase After GPT",
    median_diff < 0 ~ "Decrease After GPT",
    TRUE ~ "No Effect" #Should be rare but kept just in case the median is 0
  ))

print(results_df)
# Filter by significant results
significant_results <- results_df %>%
  filter(p_adjusted < 0.05)

print(significant_results)
# Save the results to a CSV file
write.csv(results_df, "/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/R/author_aggregate_results.csv", row.names = FALSE)
write.csv(significant_results, "/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/R/significant_author_aggregate_results.csv", row.names = FALSE)
