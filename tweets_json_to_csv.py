import json
import pandas as pd
import logging
import sys

# Setup logging for pipeline visibility
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def run_twitter_conversion_pipeline(input_file, output_file):
    """
    Reads a JSON file of tweets, flattens the nested structure, 
    removes duplicates, and exports to CSV.
    """
    try:
        # 1. Load Data
        logging.info(f"Reading data from {input_file}...")
        with open(input_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # 2. Flatten JSON
        # Normalizes nested dicts/lists into a flat table
        logging.info("Flattening JSON data...")
        df = pd.json_normalize(data)
        
        # 3. Data Profiling (Replacing .describe())
        if 'text_content' in df.columns:
            logging.info(f"Processed {len(df)} tweets.")
            unique_texts = df['text_content'].nunique()
            logging.info(f"Unique 'text_content' entries: {unique_texts}")
        else:
            logging.warning("'text_content' column not found in data.")

        # 4. Remove Duplicates
        logging.info("Removing duplicate rows based on tweet_id or text_content...")
        if 'tweet_id' in df.columns:
            df_no_duplicates = df.drop_duplicates(subset=['tweet_id'])
        elif 'text_content' in df.columns:
            df_no_duplicates = df.drop_duplicates(subset=['text_content'])
        else:
            # Fallback if neither are found (less ideal)
            df_no_duplicates = df.drop_duplicates()
        
        # 5. Export to CSV
        logging.info(f"Saving cleaned data to {output_file}...")
        df_no_duplicates.to_csv(output_file, index=False, encoding="utf-8-sig")
        
        logging.info("Pipeline completed successfully.")

    except FileNotFoundError:
        logging.error(f"File not found: {input_file}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Define file paths (these could also be passed via argparse if needed)
    INPUT_JSON = "all_tweets_extracted_WithDateFilter_june.json"
    OUTPUT_CSV = "all_tweets_extracted_WithDateFilter_june.csv"
    
    run_twitter_conversion_pipeline(INPUT_JSON, OUTPUT_CSV)