import streamlit as st
import asyncio
import os
import logging
from src.services.pipeline import PipelineService
from src.utils.logging import setup_logger

# Setup Logger
logger = setup_logger("TwitterScraperUI")

st.set_page_config(page_title="Twitter Scraper v2", layout="wide")

st.title("🐦 Twitter Scraper Professional")
st.markdown("---")

# Sidebar
with st.sidebar:
    st.header("Search Configuration")
    job_type = st.radio("Job Type", ["Search", "Profile"])
    keyword = st.text_input("Keyword or @Handle", placeholder="e.g. Bitcoin or @elonmusk")
    max_pages = st.number_input("Max Pages", 1, 100, 3)
    base_url = st.text_input("Nitter Instance", "https://nitter.net")
    
    st.subheader("Filters")
    col1, col2 = st.columns(2)
    since = col1.date_input("Since", value=None)
    until = col2.date_input("Until", value=None)
    
    start_btn = st.button("Start Pipeline", use_container_width=True)

# Main Area
log_container = st.container()
log_container.subheader("Pipeline execution logs")
log_area = log_container.empty()

class StreamlitLogHandler(logging.Handler):
    def __init__(self, widget):
        super().__init__()
        self.widget = widget
        self.logs = ""
    def emit(self, record):
        self.logs += self.format(record) + "\n"
        self.widget.text_area("Logs", self.logs, height=300)

if start_btn:
    if not keyword:
        st.error("Please provide a keyword or handle.")
    else:
        # Register log handler for UI
        handler = StreamlitLogHandler(log_area)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(handler)
        
        async def run_app():
            service = PipelineService(base_url)
            
            config = {
                "job_type": job_type,
                "keyword": keyword,
                "max_pages": max_pages,
                "since": since.strftime("%Y-%m-%d") if since else None,
                "until": until.strftime("%Y-%m-%d") if until else None,
            }
            
            st.info(f"Starting {job_type} job for {keyword}...")
            await service.run_scrape_jobs([config])
            
            st.info("Scraping finished. Processing data...")
            csv_path = service.process_all_raw()
            
            if csv_path and os.path.exists(csv_path):
                st.success(f"Pipeline complete! Data saved to {csv_path}")
                with open(csv_path, "rb") as f:
                    st.download_button("Download CSV", f, "results.csv", "text/csv")
            else:
                st.warning("No data extracted. Try a different Nitter instance or search term.")

        asyncio.run(run_app())
