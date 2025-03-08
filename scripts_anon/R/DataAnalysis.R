# Install required packages if you don't have them already
if(!require(tidyverse)){install.packages("tidyverse")}
if(!require(lme4)){install.packages("lme4")}
if(!require(lmerTest)){install.packages("lmerTest")}
if(!require(broom.mixed)){install.packages("broom.mixed")}
if(!require(MuMIn)){install.packages("MuMIn")}

# Load libraries
library(tidyverse)
library(lme4)
library(lmerTest)
library(broom.mixed)
library(MuMIn)

# Load your data
data <- read.csv("/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/DataExport/paper_metrics.csv") 

# Make sure author_id is a factor
data$author_id <- as.factor(data$author_id)

# List of all the columns of your metric
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

# --- Analysis Loop ---
results <- list() # Store results in a list

for (metric in metrics_list) {
  
  # Create a formula string for the mixed model
  formula_string <- paste(metric, "~ afterGPT + (1 | author_id)")
  
  # Convert the string to formula
  formula <- as.formula(formula_string)
  
  # Fit the mixed effects model (assuming 'afterGPT' is 0/1 indicator)
  model <- tryCatch(lmer(formula, data = data),
                    error = function(e) {
                      message(paste("Error fitting model for", metric, ": ", e$message))
                      return(NULL)
                    })
  
  if(!is.null(model)){
    # Get p-value for 'afterGPT'
    p_value <- tryCatch(summary(model)$coefficients["afterGPT","Pr(>|t|)"],
                        error = function(e) {
                          message(paste("Error getting p-value for", metric, ": ", e$message))
                          return(NA)
                        })
    
    if(!is.na(p_value)){
      #Get confidence intervals
      confidence_intervals <- tryCatch(confint(model, parm = "afterGPT"),
                                       error = function(e) {
                                         message(paste("Error getting CI for", metric, ": ", e$message))
                                         return(NA)})
      
      
      # Extract the estimate of the effect
      estimate <- tryCatch(summary(model)$coefficients["afterGPT", "Estimate"],
                           error = function(e) {
                             message(paste("Error getting estimate for", metric, ": ", e$message))
                             return(NA)
                           })
      
      #Get the marginal R2
      rsq <- tryCatch(r.squaredGLMM(model),
                      error = function(e) {
                        message(paste("Error getting marginal R2 for", metric, ": ", e$message))
                        return(NA)
                      })
      
      # Store results
      results[[metric]] <- data.frame(
        metric = metric,
        estimate = estimate,
        p_value = p_value,
        CI_lower = ifelse(!is.na(confidence_intervals[1]), confidence_intervals[1], NA),
        CI_upper = ifelse(!is.na(confidence_intervals[2]), confidence_intervals[2], NA),
        marginal_rsq = rsq[1]
      )
    }
  }
}

# Convert results to a dataframe
results_df <- bind_rows(results)
print(results_df)
# Adjust p-values for multiple testing (optional, but recommended)
results_df$p_adjusted <- p.adjust(results_df$p_value, method = "BH")  # Use Benjamini-Hochberg method

# Print the results
print(results_df)


# Further examination of significant results

significant_results <- results_df %>%
  filter(p_adjusted < 0.05) # Filter only significant

print("Significant results:")
print(significant_results)

# Examine the direction of significant effects
significant_results <- significant_results %>%
  mutate(effect_direction = case_when(
    estimate > 0 ~ "Increase After GPT",
    estimate < 0 ~ "Decrease After GPT",
    TRUE ~ "No Effect"))

print("Significant results with direction:")
print(significant_results)

# (Optional) Save the results to a CSV file
write.csv(results_df, "/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/R/mixed_model_results.csv", row.names = FALSE)
write.csv(significant_results, "/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/R/significant_mixed_model_results.csv", row.names = FALSE)
