import pandas as pd

def split_csv_by_domain(input_file):
    # Load the CSV file
    df = pd.read_csv(input_file)
    
    # Define domain categories and output filenames
    domains = {
        "Physical Sciences": "/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/DataExport/paper_domain_physical.csv",
        "Health Sciences": "/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/DataExport/paper_domain_health.csv",
        "Life Sciences": "/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/DataExport/paper_domain_life.csv",
        "Social Sciences": "/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/DataExport/paper_domain_social.csv"
    }
    
    # Iterate over each domain and filter rows where it appears in any domain column
    for domain, output_file in domains.items():
        filtered_df = df[(df['domain1'] == domain) | (df['domain2'] == domain) | (df['domain3'] == domain)]
        
        # Save the filtered dataframe to a new CSV file
        filtered_df.to_csv(output_file, index=False)
        print(f"Created {output_file} with {len(filtered_df)} records.")

# Example usage
split_csv_by_domain("/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/DataExport/paper_domains_all.csv")
