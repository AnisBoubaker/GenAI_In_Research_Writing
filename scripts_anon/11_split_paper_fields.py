import pandas as pd

def split_csv_by_domain(input_file):
    # Load the CSV file
    df = pd.read_csv(input_file)
    
    # Define domain categories and output filenames
    domains = {
        "Engineering": "/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/DataExport/paper_field_engineering.csv",
        "Computer Science": "/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/DataExport/paper_field_cs.csv",
        "Physics and Astronomy": "/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/DataExport/paper_field_physics.csv"
    }
    
    # Iterate over each domain and filter rows where it appears in any domain column
    for domain, output_file in domains.items():
        filtered_df = df[(df['field1'] == domain) | (df['field2'] == domain) | (df['field3'] == domain)]
        
        # Save the filtered dataframe to a new CSV file
        filtered_df.to_csv(output_file, index=False)
        print(f"Created {output_file} with {len(filtered_df)} records.")

# Example usage
split_csv_by_domain("/Users/anis/Sync/01_ETS/70_Recherche/IAinResearch/DataExport/paper_domains_all.csv")
