import streamlit as st
import os
import time
import logging
import threading
from datetime import datetime, timedelta
from main_scrape import scrape_twitter
from parse_html_data import parse_twitter_html
from tweets_json_to_csv import run_twitter_conversion_pipeline

# --- Page Config ---
st.set_page_config(
    page_title="Twitter Scraper Dashboard",
    page_icon="🐦",
    layout="wide"
)

# --- Custom Logging Handler ---
class StreamlitLogHandler(logging.Handler):
    def __init__(self, log_area):
        super().__init__()
        self.log_area = log_area
        self.logs = ""

    def emit(self, record):
        msg = self.format(record)
        self.logs += msg + "\n"
        self.log_area.text_area("Live Logs", value=self.logs, height=300)

# --- App Styling ---
st.markdown("""
<style>
    .main {
        background-color: #0e1117;
        color: #ffffff;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        height: 3em;
        background-color: #1DA1F2;
        color: white;
        font-weight: bold;
    }
    .stButton>button:hover {
        background-color: #1a91da;
        border-color: #1a91da;
    }
</style>
""", unsafe_allow_html=True)

# --- Sidebar / Inputs ---
st.title("🐦 Twitter Scraper Dashboard")
st.write("Extract data from Twitter/X via Nitter instances without an API key.")

with st.sidebar:
    st.header("Search Configuration")
    keyword = st.text_input("Keyword / Hashtag", placeholder="e.g. #Python")
    
    col1, col2 = st.columns(2)
    with col1:
        since_date = st.date_input("Since", datetime.now() - timedelta(days=7))
    with col2:
        until_date = st.date_input("Until", datetime.now())
    
    max_pages = st.number_input("Max Pages", min_value=1, max_value=100, value=5)
    
    st.header("Instance Settings")
    nitter_instance = st.text_input("Nitter Instance", value="https://nitter.net")
    
    st.header("Output Options")
    output_formats = st.multiselect("Select Formats", ["JSON", "CSV"], default=["JSON", "CSV"])
    
    start_button = st.button("🚀 Start Scraping")

# --- Progress & Logs ---
log_col, progress_col = st.columns([2, 1])

with log_col:
    st.subheader("📋 Execution Logs")
    log_placeholder = st.empty()
    logs_text = ""

with progress_col:
    st.subheader("🌐 Visited URLs")
    urls_placeholder = st.empty()

# --- Pipeline Function ---
def run_pipeline():
    global logs_text
    
    # 1. Setup Logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear previous logs in UI
    logs_text = "Starting pipeline...\n"
    log_placeholder.text_area("Live Logs", value=logs_text, height=400)
    
    # Simple log capture
    class UIHandler(logging.Handler):
        def emit(self, record):
            global logs_text
            msg = self.format(record)
            logs_text += msg + "\n"
            log_placeholder.text_area("Live Logs", value=logs_text, height=400)

    handler = UIHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

    try:
        # Define file paths
        output_dir = "extracted_html"
        json_file = "extracted_tweets.json"
        csv_file = "extracted_tweets.csv"

        # Step 1: SCRAPE
        st.info("Step 1: Scraping HTML content...")
        scrape_twitter(
            keyword=keyword,
            since=since_date.strftime("%Y-%m-%d"),
            until=until_date.strftime("%Y-%m-%d"),
            max_pages=max_pages,
            base_url=nitter_instance,
            output_dir=output_dir
        )

        # Step 2: PARSE (Conditional)
        if "JSON" in output_formats or "CSV" in output_formats:
            st.info("Step 2: Parsing HTML to JSON...")
            tweet_count = parse_twitter_html(output_dir, json_file)
            st.success(f"Extracted {tweet_count} unique tweets.")

        # Step 3: CONVERT to CSV (Conditional)
        if "CSV" in output_formats:
            st.info("Step 3: Converting JSON to CSV...")
            run_twitter_conversion_pipeline(json_file, csv_file)
            st.success("CSV conversion complete.")

        # Final Summary
        st.balloons()
        st.success("✅ Workflow finished successfully!")
        
        # Download buttons
        st.divider()
        dl_col1, dl_col2 = st.columns(2)
        if "JSON" in output_formats and os.path.exists(json_file):
            with open(json_file, "rb") as f:
                dl_col1.download_button("Download JSON", f, file_name="tweets.json", mime="application/json")
        if "CSV" in output_formats and os.path.exists(csv_file):
            with open(csv_file, "rb") as f:
                dl_col2.download_button("Download CSV", f, file_name="tweets.csv", mime="text/csv")

    except Exception as e:
        st.error(f"Error occurred: {str(e)}")
        logging.error(f"Pipeline failed: {e}")
    finally:
        logger.removeHandler(handler)

# --- URL Monitoring Loop ---
def monitor_urls():
    while True:
        if os.path.exists('visited_urls.txt'):
            with open('visited_urls.txt', 'r') as f:
                urls = f.readlines()
                urls_placeholder.text("\n".join(urls[-15:])) # Show last 15
        time.sleep(1)

# --- Start Button Handler ---
if start_button:
    if not keyword:
        st.error("Please enter a keyword.")
    else:
        # Start the monitoring in the background is tricky in Streamlit
        # For now, we'll just run the pipeline and update the UI
        run_pipeline()

# Initial display for URLs if they exist
if os.path.exists('visited_urls.txt'):
    with open('visited_urls.txt', 'r') as f:
        urls = f.readlines()
        urls_placeholder.text("\n".join(urls[-15:]))
